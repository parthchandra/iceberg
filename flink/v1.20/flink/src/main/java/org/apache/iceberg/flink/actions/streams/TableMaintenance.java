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
package org.apache.iceberg.flink.actions.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.util.Collector;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.RateLimiter;
import org.apache.iceberg.flink.actions.operators.ResultAggregator;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.flink.actions.operators.ScheduleRequest;
import org.apache.iceberg.flink.actions.operators.SerializeCurrentTableMap;
import org.apache.iceberg.flink.actions.operators.TableChange;
import org.apache.iceberg.flink.actions.operators.WindowClosingWatermarkStrategy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Creates the table maintenance graph. */
public class TableMaintenance {
  static final String RATE_LIMITER_TASK_NAME = "Rate limiter";
  static final String SCHEDULER_TASK_NAME = "Scheduler";
  static final String FINAL_RESULT_TASK_NAME = "Final Result";

  private TableMaintenance() {
    // Do not instantiate directly
  }

  /**
   * Use when the monitor stream needs manual configuration for the monitor frequency, or the
   * monitor name.
   *
   * @param changeStream the table changes
   * @param tableLoader used for accessing the table
   * @return builder for the maintenance stream
   */
  public static Builder builder(DataStream<TableChange> changeStream, TableLoader tableLoader) {
    Preconditions.checkNotNull(changeStream, "The change stream should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

    return new Builder(changeStream, tableLoader);
  }

  /**
   * Creates the default monitor source for the table changes and returns a builder for the
   * maintenance stream.
   *
   * @param env used to register the monitor source
   * @param tableLoader used for accessing the table
   * @return builder for the maintenance stream
   */
  public static Builder builder(StreamExecutionEnvironment env, TableLoader tableLoader) {
    Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

    return new Builder(env, tableLoader);
  }

  public static class Builder {
    private final StreamExecutionEnvironment env;
    private final DataStream<TableChange> changeStream;
    private final TableLoader tableLoader;
    private final List<MaintenanceTaskBuilder<?>> taskBuilders;

    private String uidPrefix = "TableMaintenance-" + UUID.randomUUID();
    private String slotSharingGroup = StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;
    private Duration rateLimit = Duration.ofMillis(1);
    private Duration concurrentCheckDelay = Duration.ofSeconds(30);
    private Integer parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
    private boolean clearRunLocks = true;

    private Builder(StreamExecutionEnvironment env, TableLoader tableLoader) {
      this.env = env;
      this.changeStream = null;
      this.tableLoader = tableLoader;
      this.taskBuilders = Lists.newArrayListWithCapacity(4);
    }

    private Builder(DataStream<TableChange> changeStream, TableLoader tableLoader) {
      this.env = null;
      this.changeStream = changeStream;
      this.tableLoader = tableLoader;
      this.taskBuilders = Lists.newArrayListWithCapacity(4);
    }

    /**
     * The prefix used for the generated {@link org.apache.flink.api.dag.Transformation}'s uid.
     *
     * @param newUidPrefix for the transformations
     * @return for chained calls
     */
    public Builder uidPrefix(String newUidPrefix) {
      this.uidPrefix = newUidPrefix;
      return this;
    }

    /**
     * The {@link SingleOutputStreamOperator#slotSharingGroup(String)} for all the operators of the
     * generated stream. Could be used to separate the resources used by this task.
     *
     * @param newSlotSharingGroup to be used for the operators
     * @return for chained calls
     */
    public Builder slotSharingGroup(String newSlotSharingGroup) {
      this.slotSharingGroup = newSlotSharingGroup;
      return this;
    }

    /**
     * Limits the firing frequency for the scheduler.
     *
     * @param newRateLimit firing frequency
     * @return for chained calls
     */
    public Builder rateLimit(Duration newRateLimit) {
      this.rateLimit = newRateLimit;
      return this;
    }

    /**
     * Sets the delay for checking lock availability when a concurrent run detected.
     *
     * @param newConcurrentCheckDelay firing frequency
     * @return for chained calls
     */
    public Builder concurrentCheckDelay(Duration newConcurrentCheckDelay) {
      this.concurrentCheckDelay = newConcurrentCheckDelay;
      return this;
    }

    /**
     * Sets the parallelism of maintenance tasks.
     *
     * @param newParallelism task parallelism
     * @return for chained calls
     */
    public Builder parallelism(int newParallelism) {
      this.parallelism = newParallelism;
      return this;
    }

    /**
     * Clears all run locks when restarting without state.
     *
     * @param newClearRunLocks to clear the locks
     * @return for chained calls
     */
    public Builder clearRunLocks(boolean newClearRunLocks) {
      this.clearRunLocks = newClearRunLocks;
      return this;
    }

    /**
     * Adds a specific task with the given schedule.
     *
     * @param task to add
     * @return for chained calls
     */
    public Builder add(MaintenanceTaskBuilder<?> task) {
      taskBuilders.add(task);
      return this;
    }

    /**
     * Builds the task graph for the maintenance tasks.
     *
     * @return the result of the last scheduled stream
     */
    public DataStream<TriggerResult> build() {
      Preconditions.checkArgument(!taskBuilders.isEmpty(), "Provide at least one task");
      Preconditions.checkNotNull(uidPrefix, "Uid prefix is required");

      long rateLimitMs = changeStream != null ? rateLimit.toMillis() : 1;
      DataStream<Tuple2<Long, TableChange>> tableChangeWithWatermark =
          DataStreamUtils.reinterpretAsKeyedStream(
                  changeStream != null
                      ? changeStream
                      : TableMonitor.builder(env, tableLoader)
                          .monitorFrequency(rateLimit)
                          .uidPrefix(uidPrefix + "-monitor")
                          .slotSharingGroup(slotSharingGroup)
                          .build(),
                  unused -> true)
              .process(
                  new RateLimiter(
                      tableLoader, rateLimitMs, concurrentCheckDelay.toMillis(), clearRunLocks))
              .name(RATE_LIMITER_TASK_NAME)
              .uid(uidPrefix + "-rate-limiter")
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel()
              .assignTimestampsAndWatermarks(new WindowClosingWatermarkStrategy<>())
              .name("Watermark Assigner")
              .uid(uidPrefix + "-watermark-assigner")
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel();

      DataStream<TableChange> watermarked =
          tableChangeWithWatermark
              .map(value -> value.f1)
              .name("Timestamp Remover")
              .uid(uidPrefix + "-timestamp-remover")
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel();

      DataStream<RunResponse> chain =
          tableChangeWithWatermark
              .map(
                  withWatermark ->
                      new RunResponse(withWatermark.f0, -1, 0, true, Collections.emptyList()))
              .name("Trigger [0]")
              .uid(uidPrefix + "-task-trigger-0")
              .slotSharingGroup(slotSharingGroup)
              .forceNonParallel();

      DataStream<ScheduleRequest> scheduled = null;
      DataStream<RunResponse> results = null;

      Map<Integer, String> streams = Maps.newHashMapWithExpectedSize(taskBuilders.size());
      for (int i = 0; i < taskBuilders.size(); i++) {
        MaintenanceTaskBuilder<?> streamBuilder = taskBuilders.get(i);

        String name = nameFor(streamBuilder);
        streams.put(i, name);
        streamBuilder.init(
            i, name + " [" + i + "]", tableLoader, uidPrefix, slotSharingGroup, parallelism);

        SingleOutputStreamOperator<ScheduleRequest> scheduledStream =
            DataStreamUtils.reinterpretAsKeyedStream(watermarked, unused -> true)
                .connect(chain.keyBy(unused -> true))
                .process(streamBuilder.scheduler())
                .name(String.join(" ", SCHEDULER_TASK_NAME, name, "[" + i + "]"))
                .uid(streamBuilder.uidPrefix() + "-scheduler-" + i)
                .slotSharingGroup(slotSharingGroup)
                .forceNonParallel();

        DataStream<SerializableTable> triggerStream =
            scheduledStream
                .flatMap(new TriggerFilter())
                .name("Trigger filter [" + i + "]")
                .uid(streamBuilder.uidPrefix() + "-trigger-filter-" + i)
                .slotSharingGroup(slotSharingGroup)
                .forceNonParallel()
                .map(new SerializeCurrentTableMap<>(tableLoader))
                .name("Table Loader [" + i + "] " + tableLoader.loadTable().name())
                .uid(streamBuilder.uidPrefix() + "-table-loader-" + i)
                .slotSharingGroup(slotSharingGroup)
                .forceNonParallel();

        chain = streamBuilder.build(triggerStream);

        scheduled = scheduled == null ? scheduledStream : scheduled.union(scheduledStream);
        results = results == null ? chain : results.union(chain);
      }

      return scheduled
          .keyBy(ScheduleRequest::timestamp)
          .connect(results.keyBy(RunResponse::timestamp))
          .process(new ResultAggregator(tableLoader, streams))
          .name(FINAL_RESULT_TASK_NAME)
          .uid(uidPrefix + "-final-result")
          .slotSharingGroup(slotSharingGroup)
          .forceNonParallel();
    }
  }

  private static String nameFor(MaintenanceTaskBuilder<?> streamBuilder) {
    return streamBuilder
        .getClass()
        .getName()
        .replace("$Builder", "")
        .replace(streamBuilder.getClass().getPackage().getName() + ".", "");
  }

  private static class TriggerFilter implements FlatMapFunction<ScheduleRequest, ScheduleRequest> {
    @Override
    public void flatMap(ScheduleRequest request, Collector<ScheduleRequest> out) {
      if (request.triggered()) {
        out.collect(request);
      }
    }
  }
}
