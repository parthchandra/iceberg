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

import static org.apache.iceberg.flink.actions.ActionTestUtils.record;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.ADDED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.ADDED_DATA_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.MAINTENANCE_ERROR_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_DATA_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_EQUALITY_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_EQUALITY_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_POSITIONAL_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.REMOVED_POSITIONAL_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.actions.streams.RewriteDataFiles.COMMIT_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteDataFiles.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.RewriteDataFiles.REWRITE_TASK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.stream.StreamSupport;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestRewriteDataFiles extends ScheduledBuilderTestBase {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRewriteDataFiles(boolean partitioned) throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) %s",
        TABLE_NAME, partitioned ? "PARTITIONED BY (spec)" : "");
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteDataFiles.builder()
        .parallelism(2)
        .deleteFileThreshold(10)
        .targetFileSizeBytes(1_000_000L)
        .maxFileGroupSizeBytes(10_000_000L)
        .maxFileSizeBytes(2_000_000L)
        .minFileSizeBytes(500_000L)
        .minInputFiles(2)
        .partialProgressEnabled(true)
        .partialProgressMaxCommits(1)
        .rewriteAll(false)
        .maxRewriteBytes(30_000_000L)
        .uidPrefix(UID_PREFIX)
        .init(
            0, DUMMY_NAME, tableLoader, "OTHER", StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    assertFileNum(table, partitioned ? 2 : 1, 0);

    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            record(1, "a", "p1"),
            record(2, "b", "p1"),
            record(3, "c", "p2"),
            record(4, "d", "p2")));
  }

  @Test
  void testStateRestore(@TempDir File savepointDir) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteDataFiles.builder()
        .minInputFiles(2)
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

    Configuration conf =
        runAndWaitForSavepoint(infra.env(), infra.source(), infra.sink(), savepointDir, table);

    assertFileNum(table, 1, 0);
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));

    // Add some more data
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);

    assertFileNum(table, 2, 0);

    // New env from the savepoint
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

    ManualSource<SerializableTable> source = infra.source(env);
    DataStream<SerializableTable> triggerStream = infra.triggerStream(source);
    CollectingSink<RunResponse> sink = infra.newSink();
    RewriteDataFiles.builder()
        .minInputFiles(2)
        .uidPrefix(UID_PREFIX)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(triggerStream)
        .sinkTo(sink);

    runAndWaitForSuccess(env, source, sink, table);

    assertFileNum(table, 1, 0);
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2"), record(3, "c", "p3")));
  }

  @Test
  void testFailure() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteDataFiles.builder()
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

    assertFileNum(table, 2, 0);

    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 1L)
            .put(REWRITE_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_DATA_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_DATA_FILE_SIZE_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_DATA_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_DATA_FILE_SIZE_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_EQUALITY_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_EQUALITY_FILE_SIZE_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_POSITIONAL_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_POSITIONAL_FILE_SIZE_METRIC, 0L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RewriteDataFiles.builder()
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .uidPrefix(UID_PREFIX)
        .init(0, DUMMY_NAME, tableLoader, "OTHER", "OTHER", 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), UID_PREFIX);
    checkSlotSharingGroupsAreSet(infra.env(), SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RewriteDataFiles.builder()
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
  void testV2Table() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar, PRIMARY KEY(`id`) NOT ENFORCED) "
            + "WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (1, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'c', 'p3')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    assertFileNum(table, 2, 3);

    RewriteDataFiles.builder()
        .rewriteAll(true)
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

    // One equality delete remains which could be relevant if there are other files in the table
    assertFileNum(table, 1, 1);

    SimpleDataUtil.assertTableRecords(table, ImmutableList.of(record(1, "c", "p3")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(REWRITE_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_DATA_FILE_NUM_METRIC, 1L)
            .put(COMMIT_TASK_NAME + "." + ADDED_DATA_FILE_SIZE_METRIC, -1L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_DATA_FILE_NUM_METRIC, 2L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_DATA_FILE_SIZE_METRIC, -1L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_EQUALITY_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_EQUALITY_FILE_SIZE_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_POSITIONAL_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_POSITIONAL_FILE_SIZE_METRIC, 0L)
            .build());
  }

  @Test
  void testMetrics() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    RewriteDataFiles.builder()
        .rewriteAll(true)
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
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(REWRITE_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + ADDED_DATA_FILE_NUM_METRIC, 1L)
            .put(COMMIT_TASK_NAME + "." + ADDED_DATA_FILE_SIZE_METRIC, -1L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_DATA_FILE_NUM_METRIC, 2L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_DATA_FILE_SIZE_METRIC, -1L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_EQUALITY_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_EQUALITY_FILE_SIZE_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_POSITIONAL_FILE_NUM_METRIC, 0L)
            .put(COMMIT_TASK_NAME + "." + REMOVED_POSITIONAL_FILE_SIZE_METRIC, 0L)
            .build());
  }

  private static void assertFileNum(
      Table table, int expectedDataFileNum, int expectedDeleteFileNum) {
    table.refresh();
    assertThat(
            table.currentSnapshot().dataManifests(table.io()).stream()
                .flatMap(
                    m ->
                        StreamSupport.stream(
                            ManifestFiles.read(m, table.io(), table.specs()).spliterator(), false))
                .count())
        .isEqualTo(expectedDataFileNum);
    assertThat(
            table.currentSnapshot().deleteManifests(table.io()).stream()
                .flatMap(
                    m ->
                        StreamSupport.stream(
                            ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                                .spliterator(),
                            false))
                .count())
        .isEqualTo(expectedDeleteFileNum);
  }
}
