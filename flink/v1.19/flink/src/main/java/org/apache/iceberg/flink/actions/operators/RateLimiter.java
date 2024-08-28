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

import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provide a way to limit the request coming to the {@link StreamScheduler} to 1/ms. */
public class RateLimiter
    extends KeyedProcessFunction<Boolean, TableChange, Tuple2<Long, TableChange>>
    implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(RateLimiter.class);

  private final TableLoader tableLoader;
  private final long minFireMs;
  private final long tagCheckDelayMs;
  private final boolean clearLocks;
  private transient Counter rateLimiterTriggeredCounter;
  private transient Counter concurrentRunTriggeredCounter;
  private transient ValueState<Long> nextTriggerTime;
  private transient ValueState<TableChange> accumulatedChange;
  private transient TriggerLock lock;
  private transient boolean fireCleanUpTrigger = false;

  public RateLimiter(
      TableLoader tableLoader, long minFireMs, long tagCheckDelayMs, boolean clearLocks) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");
    Preconditions.checkArgument(minFireMs > 0, "Minimum fire rate should be at least 1.");

    this.tableLoader = tableLoader;
    this.minFireMs = minFireMs;
    this.tagCheckDelayMs = tagCheckDelayMs;
    this.clearLocks = clearLocks;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.rateLimiterTriggeredCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.RATE_LIMITER_TRIGGERED);
    this.concurrentRunTriggeredCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.CONCURRENT_RUN_TRIGGERED);

    this.nextTriggerTime =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("rateLimiterNextTriggerTime", Types.LONG));
    this.accumulatedChange =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(
                    "rateLimiterAccumulatedChange", TypeInformation.of(TableChange.class)));
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // Do nothing
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    LOG.info("Initializing locks restored: {}, clearLocks: {}", context.isRestored(), clearLocks);
    this.lock = new TagBasedLock(tableLoader);
    if (context.isRestored()) {
      // When the job state is restored, there could be ongoing tasks.
      // To prevent collision with the new triggers the following is done:
      //  - add a lock
      //  - fire a clean-up trigger
      // This ensures that the tasks of the previous trigger are executed, and the lock is removed
      // in the end.
      lock.tryLock();
      fireCleanUpTrigger = true;
    } else if (clearLocks) {
      // Remove old lock if we are not restoring the job
      lock.unlock();
    }
  }

  @Override
  public void processElement(
      TableChange change, Context ctx, Collector<Tuple2<Long, TableChange>> out) throws Exception {
    long current = ctx.timerService().currentProcessingTime();
    Long nextTrigger = nextTriggerTime.value();

    if (fireCleanUpTrigger) {
      // Emit a clean-up trigger, in case of state restore
      LOG.info("Rate limiter clean-up trigger fired: {}, next: {}", current, nextTrigger);
      long dummyTime = nextTrigger == null ? current : Math.max(nextTrigger, current);
      out.collect(Tuple2.of(dummyTime, null));
      nextTriggerTime.update(dummyTime + minFireMs);
      nextTrigger = nextTriggerTime.value();
      fireCleanUpTrigger = false;
    }

    accumulatedChange.update(merge(change, accumulatedChange.value()));

    if (nextTrigger == null || nextTrigger < current) {
      fire(current, ctx.timerService(), out);
    } else {
      LOG.info(
          "Rate limiter triggered current: {}, next: {}, change: {}", current, nextTrigger, change);
      rateLimiterTriggeredCounter.inc();
      delayFire(ctx.timerService(), nextTrigger);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, TableChange>> out)
      throws Exception {
    fire(ctx.timerService().currentProcessingTime(), ctx.timerService(), out);
  }

  /**
   * Handles merging changes, even if the second one is null.
   *
   * @param change1 to merge
   * @param change2 to merge (can be null)
   * @return the merged changed
   */
  private static TableChange merge(TableChange change1, TableChange change2) {
    TableChange result = change1.copy();
    if (change2 != null) {
      result.merge(change2);
    }

    return result;
  }

  private void fire(
      long current, TimerService timerService, Collector<Tuple2<Long, TableChange>> out)
      throws IOException {
    if (lock.tryLock()) {
      TableChange change = accumulatedChange.value();
      out.collect(Tuple2.of(current, change));
      LOG.debug("Fired event with time: {}, collected: {}", current, change);
      accumulatedChange.clear();
      nextTriggerTime.update(current + minFireMs);
      timerService.deleteProcessingTimeTimer(current);
    } else {
      // The lock is already held by someone
      LOG.info("Delaying task on failed lock check: {}", current);

      concurrentRunTriggeredCounter.inc();
      delayFire(timerService, current + tagCheckDelayMs);
    }
  }

  private void delayFire(TimerService timerService, long newTime) throws IOException {
    Long previousTriggerTime = nextTriggerTime.value();
    if (previousTriggerTime != null) {
      timerService.deleteProcessingTimeTimer(previousTriggerTime);
      nextTriggerTime.update(Math.max(previousTriggerTime, newTime));
    } else {
      nextTriggerTime.update(newTime);
    }

    timerService.registerProcessingTimeTimer(nextTriggerTime.value());
  }
}
