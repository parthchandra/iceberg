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

import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark} on receiving a watermark
 * when the pre-configured predicates are satisfied.
 *
 * <p>As a side output emits the scheduling result so the {@link ResultAggregator} can calculate the
 * result for the given watermark.
 */
public class StreamScheduler<T>
    extends KeyedCoProcessFunction<Boolean, TableChange, T, ScheduleRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

  private final StreamSchedulerTrigger trigger;
  private final int id;
  private final String name;
  private transient Counter schedulerFired;
  private transient Counter schedulerSkipped;
  private transient ListState<Tuple2<Long, TableChange>> events;
  private transient ValueState<Long> lastTriggerTime;
  private Long cleanUpBarrier = null;

  public StreamScheduler(StreamSchedulerTrigger trigger, int id, String name) {
    Preconditions.checkNotNull(trigger, "Trigger should no be null");
    Preconditions.checkNotNull(name, "Name should no be null");

    this.trigger = trigger;
    this.id = id;
    this.name = name;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.schedulerFired =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.SCHEDULER_FIRED);
    this.schedulerSkipped =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.SCHEDULER_SKIPPED);

    this.events =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "streamSchedulerEvents",
                    TypeInformation.of(new TypeHint<Tuple2<Long, TableChange>>() {})));
    this.lastTriggerTime =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("streamSchedulerLastTriggerTime", Types.LONG));
  }

  @Override
  public void processElement1(TableChange change, Context ctx, Collector<ScheduleRequest> out)
      throws Exception {
    LOG.debug("Scheduler {} change arrived {} at {}", name, change, ctx.timestamp());

    if (change == null) {
      cleanUpBarrier = ctx.timestamp();
      LOG.info("Scheduler {} skipped clean-up trigger at timestamp {}", name, ctx.timestamp());
    } else {
      if (lastTriggerTime.value() == null) {
        lastTriggerTime.update(ctx.timestamp() - 1);
      }

      events.add(Tuple2.of(ctx.timestamp(), change));
    }
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void processElement2(T value, Context ctx, Collector<ScheduleRequest> out)
      throws Exception {
    LOG.debug("Scheduler {} trigger at {}", name, ctx.timestamp());

    // This is here only to handle coordinate watermarks for the 2 flows
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<ScheduleRequest> out)
      throws Exception {
    LOG.debug("Scheduler {} timer run at {}", name, timestamp);
    if (cleanUpBarrier != null) {
      if (cleanUpBarrier.longValue() > timestamp) {
        LOG.info("Waiting for the clean-up barrier in {} at {}", name, timestamp);
      } else {
        // Clean-up trigger
        LOG.info("Scheduler skipped for clean-up in {} at {}", name, timestamp);
        out.collect(new ScheduleRequest(ctx.timestamp(), id, name, false));
        schedulerSkipped.inc();
        cleanUpBarrier = null;
      }

      return;
    }

    List<Tuple2<Long, TableChange>> remaining = Lists.newArrayList();
    TableChange current = new TableChange(0, 0, 0L, 0L, 0);
    events
        .get()
        .forEach(
            t -> {
              if (t.f0 <= timestamp) {
                current.merge(t.f1);
              } else {
                remaining.add(t);
              }
            });

    LOG.debug(
        "Testing scheduler {} change {} at {} with remaining {} with last run {}",
        name,
        current,
        timestamp,
        remaining,
        lastTriggerTime.value());
    if (trigger.check(timestamp, current, lastTriggerTime.value())) {
      out.collect(new ScheduleRequest(timestamp, id, name, true));
      lastTriggerTime.update(timestamp);
      events.update(remaining);
      LOG.info("Scheduler {} triggered by {} at timestamp {}", name, current, timestamp);
      schedulerFired.inc();
    } else {
      LOG.debug(
          "Scheduler {} message arrived, but not triggered in state {} at {}",
          name,
          current,
          timestamp);
      out.collect(new ScheduleRequest(timestamp, id, name, false));
      schedulerSkipped.inc();
    }
  }
}
