/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.actions.operators;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.streams.StreamResult;
import org.apache.iceberg.flink.actions.streams.TriggerResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Aggregates a result for a single run. */
public class ResultAggregator
    extends KeyedCoProcessFunction<Long, ScheduleRequest, RunResponse, TriggerResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ResultAggregator.class);

  private final TableLoader tableLoader;
  private final Map<Integer, String> streams;

  private transient ListState<ScheduleRequest> scheduleResults;
  private transient ListState<RunResponse> runResponses;
  private transient Counter successfulTriggerCounter;
  private transient Counter failedTriggerCounter;
  private transient Map<Integer, Counter> successfulStreamResultCounterMap;
  private transient Map<Integer, Counter> failedStreamResultCounterMap;
  private transient Map<Integer, AtomicLong> lastRunLength;
  private transient TriggerLock lock;

  public ResultAggregator(TableLoader tableLoader, Map<Integer, String> streams) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");
    Preconditions.checkNotNull(streams, "Streams map should no be null");

    this.tableLoader = tableLoader;
    this.streams = streams;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.successfulTriggerCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.SUCCESSFUL_TRIGGER_COUNTER);
    this.failedTriggerCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.FAILED_TRIGGER_COUNTER);
    this.successfulStreamResultCounterMap = Maps.newHashMapWithExpectedSize(streams.size());
    this.failedStreamResultCounterMap = Maps.newHashMapWithExpectedSize(streams.size());
    this.lastRunLength = Maps.newHashMapWithExpectedSize(streams.size());
    streams.forEach(
        (id, name) -> {
          successfulStreamResultCounterMap.put(
              id,
              getRuntimeContext()
                  .getMetricGroup()
                  .addGroup(MetricConstants.GROUP_KEY, name + " [" + id + "]")
                  .counter(MetricConstants.SUCCESSFUL_STREAM_COUNTER));
          failedStreamResultCounterMap.put(
              id,
              getRuntimeContext()
                  .getMetricGroup()
                  .addGroup(MetricConstants.GROUP_KEY, name + " [" + id + "]")
                  .counter(MetricConstants.FAILED_STREAM_COUNTER));
          AtomicLong length = new AtomicLong(0);
          lastRunLength.put(id, length);
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(MetricConstants.GROUP_KEY, name + " [" + id + "]")
              .gauge(MetricConstants.LAST_RUN_LENGTH, length::get);
        });

    this.scheduleResults =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "resultAggregatorSchedulerResults", TypeInformation.of(ScheduleRequest.class)));
    this.runResponses =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "resultAggregatorRunResults", TypeInformation.of(RunResponse.class)));

    this.lock = new TagBasedLock(tableLoader);
  }

  @Override
  public void processElement1(ScheduleRequest value, Context ctx, Collector<TriggerResult> out)
      throws Exception {
    LOG.debug("ScheduleRequest {} arrived", value);
    scheduleResults.add(value);
    ctx.timerService().registerEventTimeTimer(value.timestamp());
  }

  @Override
  public void processElement2(RunResponse value, Context ctx, Collector<TriggerResult> out)
      throws Exception {
    LOG.debug("RunResponse {} arrived", value);
    runResponses.add(value);
    ctx.timerService().registerEventTimeTimer(value.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TriggerResult> out)
      throws Exception {
    boolean success = true;
    long length = 0;
    int runResultSize = 0;
    int scheduledSize = 0;
    Map<Integer, StreamResult> resultsMap = Maps.newHashMap();

    for (ScheduleRequest streamRequest : scheduleResults.get()) {
      if (streamRequest.triggered()) {
        ++scheduledSize;
      }

      resultsMap.put(
          streamRequest.id(),
          new StreamResult(streamRequest.id(), streamRequest.name(), streamRequest.triggered()));
    }

    for (RunResponse runResponse : runResponses.get()) {
      ++runResultSize;
      StreamResult streamResult = resultsMap.get(runResponse.id());

      Preconditions.checkArgument(
          streamResult != null,
          "Could not found scheduler for run. Current information schedules: %s, runs: %s",
          Lists.newArrayList(scheduleResults.get().iterator()),
          Lists.newArrayList(runResponses.get().iterator()));

      streamResult.result(runResponse.success());
      streamResult.length(runResponse.length());
      streamResult.addExceptions(runResponse.exceptions());

      success &= streamResult.result();
      length += streamResult.length();
    }

    Preconditions.checkArgument(
        scheduledSize == runResultSize,
        "Inconsistent count in scheduler %s and run %s result",
        Lists.newArrayList(scheduleResults.get().iterator()),
        Lists.newArrayList(runResponses.get().iterator()));

    List<StreamResult> resultList = Lists.newArrayList(resultsMap.values().iterator());
    resultList.sort(Comparator.comparingInt(StreamResult::id));
    TriggerResult result = new TriggerResult(timestamp, success, length, resultList);
    out.collect(result);
    LOG.info("Aggregated result is {}", result);
    updateMetrics(result);
    scheduleResults.clear();
    runResponses.clear();
    lock.unlock();
  }

  private void updateMetrics(TriggerResult result) {
    if (result.overall()) {
      successfulTriggerCounter.inc();
    } else {
      failedTriggerCounter.inc();
    }

    for (StreamResult streamResult : result.results()) {
      if (streamResult.scheduled()) {
        if (streams.containsKey(streamResult.id())) {
          if (streamResult.result()) {
            successfulStreamResultCounterMap.get(streamResult.id()).inc();
            lastRunLength.get(streamResult.id()).set(streamResult.length());
          } else {
            failedStreamResultCounterMap.get(streamResult.id()).inc();
          }
        } else {
          LOG.error("Unknown id: {} in {}", streamResult.id(), result);
        }
      }
    }
  }
}
