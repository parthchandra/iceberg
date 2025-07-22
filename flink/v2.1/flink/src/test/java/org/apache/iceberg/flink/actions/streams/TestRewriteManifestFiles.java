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

import static org.apache.iceberg.flink.actions.ActionTestUtils.closeJobClient;
import static org.apache.iceberg.flink.actions.ActionTestUtils.record;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.ADDED_MANIFEST_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.ADDED_MANIFEST_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.INCOMPATIBLE_SCHEMA_CHANGE;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.INCOMPATIBLE_SPEC_CHANGE;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.MAINTENANCE_ERROR_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_MANIFEST_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_MANIFEST_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_PARTIAL_MANIFEST_NUM;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_PARTIAL_MANIFEST_SIZE;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.COMMIT_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.FIRST_WRITE_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.LAST_WRITE_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.OLD_MANIFESTS_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.READER_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteManifestFiles.SPEC_CHANGE_BLOCKER_TASK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestRewriteManifestFiles extends ScheduledBuilderTestBase {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testManifestFileRewrite(boolean partitioned) throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) %s",
        TABLE_NAME, partitioned ? "PARTITIONED BY (spec)" : "");
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .parallelism(2)
        .planningWorkerPoolSize(10)
        .targetManifestSizeBytes(100_000L)
        .init(
            0, DUMMY_NAME, tableLoader, "OTHER", StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    table.refresh();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testManifestFileRewriteIncompatibleChange(boolean failOnSpecChange) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .failOnSpecChange(failOnSpecChange)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    table.updateSpec().addField("spec").commit();

    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    if (failOnSpecChange) {
      runAndWaitForJobFailure(infra.env(), infra.source(), table);
    } else {
      runAndWaitForFailure(infra.env(), infra.source(), infra.sink(), table);
    }

    table.refresh();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));
  }

  @Test
  void testManifestFileRewriteAfterDataRewrite() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    DataStream<SerializableTable> rewritten =
        RewriteDataFiles.builder()
            .minInputFiles(2)
            .uidPrefix(UID_PREFIX)
            .init(
                0,
                DUMMY_NAME,
                tableLoader,
                "OTHER",
                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
                1)
            .build(infra.triggerStream())
            .map(
                a -> {
                  tableLoader.open();
                  return (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());
                });

    RewriteManifestFiles.builder()
        .parallelism(2)
        .planningWorkerPoolSize(10)
        .targetManifestSizeBytes(100_000L)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(rewritten)
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    table.refresh();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testManifestFileRewriteMultiStage(boolean multiStage) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .parallelism(2)
        .multiStage(multiStage)
        .planningWorkerPoolSize(10)
        .targetManifestSizeBytes(100_000L)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    assertThat(assertTransformationExists(infra.env(), RewriteManifestFiles.FIRST_WRITE_TASK_NAME))
        .isTrue();
    assertThat(assertTransformationExists(infra.env(), RewriteManifestFiles.LAST_WRITE_TASK_NAME))
        .isEqualTo(multiStage);

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    table.refresh();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));
  }

  @Test
  void testStateRestore(@TempDir File savepointDir) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .uidPrefix(UID_PREFIX)
        .init(
            0, DUMMY_NAME, tableLoader, "OTHER", StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    Configuration conf =
        runAndWaitForSavepoint(infra.env(), infra.source(), infra.sink(), savepointDir, table);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));

    // Add some more data
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);

    table.refresh();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    // New env from the savepoint
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    ManualSource<SerializableTable> source = infra.source(env);
    DataStream<SerializableTable> triggerStream = infra.triggerStream(source);
    CollectingSink<RunResponse> sink = infra.newSink();
    RewriteManifestFiles.builder()
        .uidPrefix(UID_PREFIX)
        .init(
            0, DUMMY_NAME, tableLoader, "OTHER", StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, 1)
        .build(triggerStream)
        .sinkTo(sink);

    runAndWaitForSuccess(env, source, sink, table);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2"), record(3, "c", "p3")));
  }

  @Test
  void testConcurrentPartitioningChangeFails(@TempDir File savepointDir) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    Configuration conf =
        runAndWaitForSavepoint(infra.env(), infra.source(), infra.sink(), savepointDir, table);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    // Alter the table schema
    table.updateSpec().addField("spec").commit();

    // New env from the savepoint
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    ManualSource<SerializableTable> source = infra.source(env);
    DataStream<SerializableTable> triggerStream = infra.triggerStream(source);
    CollectingSink<RunResponse> sink = infra.newSink();
    RewriteManifestFiles.builder()
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(triggerStream)
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Do a single manifest rewrite run
      source.sendRecord((SerializableTable) SerializableTable.copyOf(table));

      JobClient finalJobClient = jobClient;
      Awaitility.await().until(() -> finalJobClient.getJobStatus().get().isTerminalState());
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testPartitioningChangeBeforeStart() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    table.updateSpec().addField("spec").commit();
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    table.updateSpec().removeField("spec").addField("data").commit();
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p4')", TABLE_NAME);

    RewriteManifestFiles.builder()
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(3);
    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            record(1, "a", "p1"),
            record(2, "b", "p2"),
            record(3, "c", "p3"),
            record(4, "d", "p4")));
  }

  @Test
  void testFailure() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    runAndWaitForFailure(infra.env(), infra.source(), infra.sink(), table);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(SPEC_CHANGE_BLOCKER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(SPEC_CHANGE_BLOCKER_TASK_NAME + "." + INCOMPATIBLE_SPEC_CHANGE, 0L)
            .put(SPEC_CHANGE_BLOCKER_TASK_NAME + "." + INCOMPATIBLE_SCHEMA_CHANGE, 0L)
            .put(OLD_MANIFESTS_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 1L)
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 1L)
            .put(READER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(FIRST_WRITE_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(FIRST_WRITE_TASK_NAME + "." + REMOVED_PARTIAL_MANIFEST_NUM, 0L)
            .put(FIRST_WRITE_TASK_NAME + "." + REMOVED_PARTIAL_MANIFEST_SIZE, 0L)
            .put(COMMIT_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_MANIFEST_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_MANIFEST_FILE_SIZE_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_MANIFEST_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_MANIFEST_FILE_SIZE_METRIC, 0L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RewriteManifestFiles.builder()
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .uidPrefix(UID_PREFIX)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), UID_PREFIX);
    checkSlotSharingGroupsAreSet(infra.env(), SLOT_SHARING_GROUP);
  }

  @Test
  void testSlotSharingGroupUnset() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RewriteManifestFiles.builder()
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), null);
    checkSlotSharingGroupsAreSet(infra.env(), null);
  }

  @Test
  void testMetrics() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteManifestFiles.builder()
        .multiStage(true)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(SPEC_CHANGE_BLOCKER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(SPEC_CHANGE_BLOCKER_TASK_NAME + "." + INCOMPATIBLE_SPEC_CHANGE, 0L)
            .put(SPEC_CHANGE_BLOCKER_TASK_NAME + "." + INCOMPATIBLE_SCHEMA_CHANGE, 0L)
            .put(OLD_MANIFESTS_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(READER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(FIRST_WRITE_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(FIRST_WRITE_TASK_NAME + "." + REMOVED_PARTIAL_MANIFEST_NUM, 1L)
            .put(FIRST_WRITE_TASK_NAME + "." + REMOVED_PARTIAL_MANIFEST_SIZE, -1L)
            .put(LAST_WRITE_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(LAST_WRITE_TASK_NAME + "." + REMOVED_PARTIAL_MANIFEST_NUM, 0L)
            .put(LAST_WRITE_TASK_NAME + "." + REMOVED_PARTIAL_MANIFEST_SIZE, 0L)
            .put(COMMIT_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_MANIFEST_FILE_NUM_METRIC, 1L)
            .put(COMMIT_TASK_NAME + "." + ADDED_MANIFEST_FILE_SIZE_METRIC, -1L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_MANIFEST_FILE_NUM_METRIC, 2L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_MANIFEST_FILE_SIZE_METRIC, -1L)
            .build());
  }

  private boolean assertTransformationExists(StreamExecutionEnvironment env, String name) {
    return env.getTransformations().stream().anyMatch(t -> t.getName().equals(name));
  }
}
