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

import static org.apache.iceberg.flink.SimpleDataUtil.createRowData;
import static org.apache.iceberg.flink.actions.ActionTestUtils.closeJobClient;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.CONCURRENT_RUN_TRIGGERED;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.FAILED_STREAM_COUNTER;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.FAILED_TRIGGER_COUNTER;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.GROUP_KEY;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.LAST_RUN_LENGTH;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.RATE_LIMITER_TRIGGERED;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.SCHEDULER_FIRED;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.SCHEDULER_SKIPPED;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.SUCCESSFUL_STREAM_COUNTER;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.SUCCESSFUL_TRIGGER_COUNTER;
import static org.apache.iceberg.flink.actions.streams.TableMaintenance.FINAL_RESULT_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.TableMaintenance.RATE_LIMITER_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.TableMaintenance.SCHEDULER_TASK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.flink.actions.operators.SimpleOperators;
import org.apache.iceberg.flink.actions.operators.TableChange;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestTableMaintenance extends StreamTestBase {
  private static final String NAME =
      TestTableMaintenance.class.getSimpleName()
          + "$"
          + MaintenanceTaskBuilderForTest.class.getSimpleName();
  @TempDir private File checkpointDir;

  private StreamExecutionEnvironment env;

  @BeforeEach
  public void beforeEach() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  @Test
  void testSchedule() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    long oldSnapshot = table.currentSnapshot().snapshotId();

    LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction =
        mapFunction(0, true);
    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
            .rateLimit(Duration.ofSeconds(2))
            .add(
                new MaintenanceTaskBuilderForTest(mapFunction)
                    .scheduleOnCommit(1)
                    .schedulerOnDeleteFileNumber(2)
                    .scheduleOnFileNumber(3)
                    .scheduleOnFileSize(10L)
                    .scheduleOnTime(Duration.ofHours(1)));

    sendEvents(
        schedulerSource,
        streamBuilder,
        ImmutableList.of(new TableChange(1, 1, 1, 1, 1)),
        sink -> assertThat(sink.poll(Duration.ofSeconds(5)).overall()).isTrue());

    assertThat(mapFunction.poll(Duration.ofSeconds(5L)).f1.currentSnapshot().snapshotId())
        .isEqualTo(oldSnapshot);
  }

  @Test
  void testTwoStepTrigger() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    long oldSnapshot = table.currentSnapshot().snapshotId();

    LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction =
        mapFunction(0, true);
    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
            .rateLimit(Duration.ofSeconds(2))
            .add(new MaintenanceTaskBuilderForTest(mapFunction).scheduleOnCommit(2));

    sendEvents(
        schedulerSource,
        streamBuilder,
        ImmutableList.of(new TableChange(1, 1, 1, 1, 1), new TableChange(1, 1, 1, 1, 1)),
        sink -> {
          TriggerResult result1 = sink.poll(Duration.ofSeconds(5));
          assertThat(result1.overall()).isTrue();
          assertThat(result1.results())
              .isEqualTo(ImmutableList.of(new StreamResult(0, task(0), false)));

          TriggerResult result2 = sink.poll(Duration.ofSeconds(5));
          assertThat(result2.overall()).isTrue();
          assertThat(result2.results())
              .isEqualTo(
                  ImmutableList.of(
                      new StreamResult(0, task(0), true, true, 1000L, Collections.emptyList())));

          return true;
        });

    assertThat(mapFunction.poll(Duration.ofSeconds(5L)).f1.currentSnapshot().snapshotId())
        .isEqualTo(oldSnapshot);
  }

  @Test
  void testConcurrentWrite() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar)"
            + "WITH ('flink.max-continuous-empty-commits'='100000')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    long oldSnapshot = table.currentSnapshot().snapshotId();

    env.enableCheckpointing(10);
    env.getCheckpointConfig().setCheckpointStorage("file://" + checkpointDir.getPath());

    // Creating the stream for SchedulerStream
    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction =
        mapFunction(0, true);
    DataStream<TriggerResult> scheduled =
        TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
            .add(new MaintenanceTaskBuilderForTest(mapFunction).scheduleOnCommit(1))
            .build();

    // Sink to collect the results
    CollectingSink<TriggerResult> result = new CollectingSink<>();
    scheduled.sinkTo(result);

    // Creating a stream for inserting data into the table concurrently
    ManualSource<RowData> insertSource =
        new ManualSource<>(env, InternalTypeInfo.of(FlinkSchemaUtil.convert(table.schema())));
    FlinkSink.forRowData(insertSource.getDataStream())
        .tableLoader(tableLoader)
        .uidPrefix(UID_PREFIX + "-iceberg-sink")
        .append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      schedulerSource.sendRecord(new TableChange(1, 1, 1, 1, 1), 0L);
      TriggerResult fromStream = result.poll(Duration.ofSeconds(5L));
      assertThat(fromStream.overall()).isTrue();
      assertThat(mapFunction.poll(Duration.ofSeconds(5L)).f1.currentSnapshot().snapshotId())
          .isEqualTo(oldSnapshot);

      insertSource.sendRecord(createRowData(2, "b"));
      // Wait until the changes are committed
      Awaitility.await()
          .until(
              () -> {
                table.refresh();
                return oldSnapshot != table.currentSnapshot().snapshotId();
              });

      table.refresh();
      long newSnapshot = table.currentSnapshot().snapshotId();
      schedulerSource.sendRecord(new TableChange(1, 1, 1, 1, 1), 1L);
      fromStream = result.poll(Duration.ofSeconds(5L));
      assertThat(fromStream.overall()).isTrue();
      assertThat(mapFunction.poll(Duration.ofSeconds(5L)).f1.currentSnapshot().snapshotId())
          .isEqualTo(newSnapshot);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testMultiSchedule() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction =
        mapFunction(0, true);
    List<LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse>> mapFunctions =
        ImmutableList.of(
            mapFunction,
            new LoggingMapFunction<>(mapFunction),
            new LoggingMapFunction<>(mapFunction),
            new LoggingMapFunction<>(mapFunction));
    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
            .concurrentCheckDelay(Duration.ofMillis(1))
            .add(new MaintenanceTaskBuilderForTest(mapFunctions.get(0)).scheduleOnCommit(1))
            .add(new MaintenanceTaskBuilderForTest(mapFunctions.get(1)).scheduleOnCommit(2))
            .add(new MaintenanceTaskBuilderForTest(mapFunctions.get(2)).scheduleOnCommit(3))
            .add(new MaintenanceTaskBuilderForTest(mapFunctions.get(3)).scheduleOnCommit(4));

    sendEvents(
        schedulerSource,
        streamBuilder,
        ImmutableList.of(
            new TableChange(2, 1, 1, 1, 1),
            new TableChange(3, 1, 1, 1, 1),
            new TableChange(4, 1, 1, 1, 1),
            new TableChange(5, 1, 1, 1, 1)),
        // Wait until all 4 scheduler is called
        unused -> {
          Awaitility.await().until(() -> Sets.newHashSet(mapFunction.callOrder()).size() == 4);
          return null;
        });

    assertCallOrders(mapFunction.remainingOutput(), mapFunction.callOrder(), 4);
  }

  @Test
  void testStateRestore(@TempDir File savepointDir) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<TriggerResult> result = new CollectingSink<>();

    List<LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse>> mapFunctions =
        ImmutableList.of(mapFunction(0, true), mapFunction(1, true));
    List<MaintenanceTaskBuilderForTest> buildersForTest =
        ImmutableList.of(
            new MaintenanceTaskBuilderForTest(mapFunctions.get(0)).scheduleOnCommit(2),
            new MaintenanceTaskBuilderForTest(mapFunctions.get(1)).scheduleOnCommit(2));

    TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
        .uidPrefix(UID_PREFIX)
        .concurrentCheckDelay(Duration.ofMillis(10))
        .add(buildersForTest.get(0))
        .add(buildersForTest.get(1))
        .build()
        .sinkTo(result);

    Configuration conf;
    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      schedulerSource.sendRecord(new TableChange(1, 1, 1, 1, 1));

      // Wait for the execution results
      assertThat(result.poll(Duration.ofSeconds(5)).overall()).isTrue();

      // No runs
      assertThat(mapFunctions.get(0).isEmpty()).isTrue();
      assertThat(mapFunctions.get(1).isEmpty()).isTrue();
    } finally {
      // Stop with savepoint
      conf = closeJobClient(jobClient, savepointDir);
    }

    // Restore from savepoint, create the same topology with a different env
    env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    schedulerSource = new ManualSource<>(env, TypeInformation.of(TableChange.class));
    result = new CollectingSink<>();

    TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
        .uidPrefix(UID_PREFIX)
        .concurrentCheckDelay(Duration.ofMillis(10))
        .add(buildersForTest.get(0))
        .add(buildersForTest.get(1))
        .build()
        .sinkTo(result);

    JobClient clientWithSavepoint = null;
    try {
      clientWithSavepoint = env.executeAsync("Scheduling Stream test with savepoint");

      assertThat(result.isEmpty()).isTrue();
      schedulerSource.sendRecord(new TableChange(1, 1, 1, 1, 1));
      // Wait for the clean-up result
      TriggerResult triggerResult = result.poll(Duration.ofSeconds(5));
      assertThat(triggerResult.overall()).isTrue();
      assertThat(triggerResult.results()).hasSize(2);
      assertThat(triggerResult.results().get(0).scheduled()).isFalse();
      assertThat(triggerResult.results().get(1).scheduled()).isFalse();

      // Wait for the execution results
      triggerResult = result.poll(Duration.ofSeconds(5));
      assertThat(triggerResult.overall()).isTrue();
      assertThat(triggerResult.results()).hasSize(2);
      assertThat(triggerResult.results().get(0).scheduled()).isTrue();
      assertThat(triggerResult.results().get(1).scheduled()).isTrue();
    } finally {
      closeJobClient(clientWithSavepoint);
    }
  }

  @Test
  void testUidAndSlotSharingGroup() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).getDataStream(),
            tableLoader)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(mapFunction(0, true))
                .scheduleOnCommit(1)
                .uidPrefix(UID_PREFIX)
                .slotSharingGroup(SLOT_SHARING_GROUP))
        .build();

    checkUidsAreSet(env, UID_PREFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction =
        mapFunction(0, true);

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).getDataStream(),
            tableLoader)
        .add(new MaintenanceTaskBuilderForTest(mapFunction).scheduleOnCommit(1))
        .build();

    checkUidsAreSet(env, null);
    checkSlotSharingGroupsAreSet(env, null);
  }

  @Test
  void testUidAndSlotSharingGroupInherit() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).getDataStream(),
            tableLoader)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(new MaintenanceTaskBuilderForTest(mapFunction(0, true)).scheduleOnCommit(1))
        .build();

    checkUidsAreSet(env, UID_PREFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupOverWrite() {
    String anotherUid = "Another-UID";
    String anotherSlotSharingGroup = "Another-SlotSharingGroup";
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).getDataStream(),
            tableLoader)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(mapFunction(0, true))
                .scheduleOnCommit(1)
                .uidPrefix(anotherUid)
                .slotSharingGroup(anotherSlotSharingGroup))
        .build();

    // Something from the scheduler
    Transformation<?> schedulerTransformation =
        env.getTransformations().stream()
            .filter(t -> t.getName().equals("Rate limiter"))
            .findFirst()
            .get();
    assertThat(schedulerTransformation.getUid()).contains(UID_PREFIX);
    assertThat(schedulerTransformation.getSlotSharingGroup()).isPresent();
    assertThat(schedulerTransformation.getSlotSharingGroup().get().getName())
        .isEqualTo(SLOT_SHARING_GROUP);

    // Something from the scheduled stream
    Transformation<?> scheduledTransformation =
        env.getTransformations().stream()
            .filter(t -> t.getName().startsWith("Test mapper"))
            .findFirst()
            .get();
    assertThat(scheduledTransformation.getUid()).contains(anotherUid);
    assertThat(scheduledTransformation.getSlotSharingGroup()).isPresent();
    assertThat(scheduledTransformation.getSlotSharingGroup().get().getName())
        .isEqualTo(anotherSlotSharingGroup);
  }

  @Test
  void testUidAndSlotSharingGroupForMonitor() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(env, tableLoader)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(mapFunction(0, true))
                .scheduleOnCommit(1)
                .uidPrefix(UID_PREFIX)
                .slotSharingGroup(SLOT_SHARING_GROUP))
        .build();

    Transformation<?> source = source(env.getTransformations().get(0));
    assertThat(source).isNotNull();
    assertThat(source.getUid()).contains(UID_PREFIX);
    assertThat(source.getSlotSharingGroup()).isPresent();
    assertThat(source.getSlotSharingGroup().get().getName()).isEqualTo(SLOT_SHARING_GROUP);

    checkUidsAreSet(env, UID_PREFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testFailure() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
            .add(new MaintenanceTaskBuilderForTest(mapFunction(0, true)).scheduleOnCommit(1))
            .add(new MaintenanceTaskBuilderForTest(mapFunction(1, false)).scheduleOnCommit(1))
            .add(new MaintenanceTaskBuilderForTest(mapFunction(2, true)).scheduleOnCommit(1));

    sendEvents(
        schedulerSource,
        streamBuilder,
        ImmutableList.of(new TableChange(2, 1, 1, 1, 1)),
        // Wait until all 4 scheduler is called
        sink -> assertThat(sink.poll(Duration.ofSeconds(5)).overall()).isFalse());

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<String, Long>()
            .put(RATE_LIMITER_TASK_NAME + "." + GROUP_KEY + "." + RATE_LIMITER_TRIGGERED, 0L)
            .put(RATE_LIMITER_TASK_NAME + "." + GROUP_KEY + "." + CONCURRENT_RUN_TRIGGERED, 0L)
            .put(SCHEDULER_TASK_NAME + " " + task(0) + "." + task(0) + "." + SCHEDULER_FIRED, 1L)
            .put(SCHEDULER_TASK_NAME + " " + task(0) + "." + task(0) + "." + SCHEDULER_SKIPPED, 0L)
            .put(SCHEDULER_TASK_NAME + " " + task(1) + "." + task(1) + "." + SCHEDULER_FIRED, 1L)
            .put(SCHEDULER_TASK_NAME + " " + task(1) + "." + task(1) + "." + SCHEDULER_SKIPPED, 0L)
            .put(SCHEDULER_TASK_NAME + " " + task(2) + "." + task(2) + "." + SCHEDULER_FIRED, 1L)
            .put(SCHEDULER_TASK_NAME + " " + task(2) + "." + task(2) + "." + SCHEDULER_SKIPPED, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(0) + "." + SUCCESSFUL_STREAM_COUNTER, 1L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(0) + "." + FAILED_STREAM_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(1) + "." + SUCCESSFUL_STREAM_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(1) + "." + FAILED_STREAM_COUNTER, 1L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(2) + "." + SUCCESSFUL_STREAM_COUNTER, 1L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(2) + "." + FAILED_STREAM_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + GROUP_KEY + "." + SUCCESSFUL_TRIGGER_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + GROUP_KEY + "." + FAILED_TRIGGER_COUNTER, 1L)
            .build());
  }

  @Test
  void testMetrics() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.getDataStream(), tableLoader)
            .rateLimit(Duration.ofSeconds(2))
            .add(new MaintenanceTaskBuilderForTest(mapFunction(0, true)).scheduleOnCommit(1))
            .add(new MaintenanceTaskBuilderForTest(mapFunction(1, true)).scheduleOnCommit(2));

    sendEvents(
        schedulerSource,
        streamBuilder,
        ImmutableList.of(new TableChange(1, 1, 1, 1, 1)),
        sink -> assertThat(sink.poll(Duration.ofSeconds(5)).overall()).isTrue());

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<String, Long>()
            .put(RATE_LIMITER_TASK_NAME + "." + GROUP_KEY + "." + RATE_LIMITER_TRIGGERED, 0L)
            .put(RATE_LIMITER_TASK_NAME + "." + GROUP_KEY + "." + CONCURRENT_RUN_TRIGGERED, 0L)
            .put(SCHEDULER_TASK_NAME + " " + task(0) + "." + task(0) + "." + SCHEDULER_FIRED, 1L)
            .put(SCHEDULER_TASK_NAME + " " + task(0) + "." + task(0) + "." + SCHEDULER_SKIPPED, 0L)
            .put(SCHEDULER_TASK_NAME + " " + task(1) + "." + task(1) + "." + SCHEDULER_FIRED, 0L)
            .put(SCHEDULER_TASK_NAME + " " + task(1) + "." + task(1) + "." + SCHEDULER_SKIPPED, 1L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(0) + "." + SUCCESSFUL_STREAM_COUNTER, 1L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(0) + "." + FAILED_STREAM_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(1) + "." + SUCCESSFUL_STREAM_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(1) + "." + FAILED_STREAM_COUNTER, 0L)
            .put(FINAL_RESULT_TASK_NAME + "." + GROUP_KEY + "." + SUCCESSFUL_TRIGGER_COUNTER, 1L)
            .put(FINAL_RESULT_TASK_NAME + "." + GROUP_KEY + "." + FAILED_TRIGGER_COUNTER, 0L)
            .build());

    MetricsReporterFactoryForTests.assertGauges(
        new ImmutableMap.Builder<String, Long>()
            .put(FINAL_RESULT_TASK_NAME + "." + task(0) + "." + LAST_RUN_LENGTH, 1000L)
            .put(FINAL_RESULT_TASK_NAME + "." + task(1) + "." + LAST_RUN_LENGTH, 0L)
            .build());
  }

  private static String task(int id) {
    return NAME + " [" + id + "]";
  }

  private void assertCallOrders(
      List<Tuple2<Long, SerializableTable>> callParameters,
      List<Integer> callOrder,
      int mappersToRun) {
    Map<Long, List<Integer>> mapperIdsByTimestamp = Maps.newHashMap();
    Map<Integer, List<Long>> timestampsByMapper = Maps.newHashMap();
    Map<Long, SerializableTable> tableByTimestamp = Maps.newHashMap();

    assertThat(callParameters).hasSize(callOrder.size());
    for (int i = 0; i < callParameters.size(); ++i) {
      Tuple2<Long, SerializableTable> parameters = callParameters.get(i);
      Integer mapperId = callOrder.get(i);
      // Check the snapshotId of the table
      if (tableByTimestamp.get(parameters.f0) != null) {
        assertThat(parameters.f1.currentSnapshot().snapshotId())
            .isEqualTo(tableByTimestamp.get(parameters.f0).currentSnapshot().snapshotId());
      } else {
        tableByTimestamp.put(parameters.f0, parameters.f1);
      }

      mapperIdsByTimestamp
          .computeIfAbsent(parameters.f0, unused -> Lists.newArrayList())
          .add(mapperId);
      timestampsByMapper
          .computeIfAbsent(mapperId, unused -> Lists.newArrayList())
          .add(parameters.f0);
    }

    assertThat(timestampsByMapper).hasSize(mappersToRun);

    mapperIdsByTimestamp
        .values()
        .forEach(
            mapperIds ->
                assertThat(mapperIds)
                    .isEqualTo(mapperIds.stream().sorted().collect(Collectors.toList())));
    timestampsByMapper
        .values()
        .forEach(
            timestamps ->
                assertThat(timestamps)
                    .isEqualTo(timestamps.stream().sorted().collect(Collectors.toList())));
  }

  private void sendEvents(
      ManualSource<TableChange> schedulerSource,
      TableMaintenance.Builder streamBuilder,
      List<TableChange> events,
      FunctionWithException<CollectingSink<TriggerResult>, Object, Exception> checker)
      throws Exception {
    CollectingSink<TriggerResult> sink = new CollectingSink<>();

    streamBuilder.build().sinkTo(sink).setParallelism(1);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      events.forEach(schedulerSource::sendRecord);

      checker.apply(sink);
    } finally {
      closeJobClient(jobClient);
    }
  }

  private static LoggingMapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction(
      int id, boolean success) {
    return new LoggingMapFunction<>(
        (Function<Tuple2<Long, SerializableTable>, RunResponse> & Serializable)
            trigger ->
                new RunResponse(
                    trigger.f0,
                    id,
                    1000L + id,
                    success,
                    success
                        ? Collections.emptyList()
                        : Lists.newArrayList(new Exception("Testing error"))),
        TypeInformation.of(RunResponse.class));
  }

  private static class MaintenanceTaskBuilderForTest
      extends MaintenanceTaskBuilder<MaintenanceTaskBuilderForTest> {
    private final MapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction;
    private static int counter = 0;

    MaintenanceTaskBuilderForTest(
        MapFunction<Tuple2<Long, SerializableTable>, RunResponse> mapFunction) {
      this.mapFunction = mapFunction;
    }

    @Override
    DataStream<RunResponse> buildInternal(DataStream<SerializableTable> trigger) {
      ++counter;
      return SimpleOperators.extractEventTime(
              trigger, uidPrefix(), "-test-extract-" + counter, slotSharingGroup())
          .map(mapFunction)
          .name("Test mapper " + counter)
          .uid(uidPrefix() + "-test-mapper-" + counter)
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }
  }
}
