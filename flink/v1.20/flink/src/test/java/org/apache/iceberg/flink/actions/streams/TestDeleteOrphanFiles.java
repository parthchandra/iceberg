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
import static org.apache.iceberg.flink.actions.ActionTestUtils.row;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.DELETE_FILE_ERROR_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.DELETE_FILE_SUCCESS_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.MAINTENANCE_ERROR_METRIC;
import static org.apache.iceberg.flink.actions.streams.DeleteOrphanFiles.DELETE_FILES_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.DeleteOrphanFiles.FILESYSTEM_FILES_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.DeleteOrphanFiles.METADATA_FILES_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.DeleteOrphanFiles.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.DeleteOrphanFiles.READER_TASK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeleteOrphanFiles extends ScheduledBuilderTestBase {
  @TempDir private File checkpointDir;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testOrphanFileRemoval(boolean partitioned) throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) %s "
            + "WITH ('flink.max-continuous-empty-commits'='100000')",
        TABLE_NAME, partitioned ? "PARTITIONED BY (spec)" : "");
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s /*+ OPTIONS('branch'='b1') */ VALUES (3, 'c', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    infra.env().enableCheckpointing(10);
    infra.env().getCheckpointConfig().setCheckpointStorage("file://" + checkpointDir.getPath());

    DeleteOrphanFiles.builder(infra.env())
        .location(table.location())
        .minAge(Duration.ZERO)
        .parallelism(2)
        .planningWorkerPoolSize(1)
        .deleteAttemptNum(2)
        .deleteWorkerPoolSize(5)
        .uidPrefix(UID_PREFIX)
        .init(
            0, DUMMY_NAME, tableLoader, "OTHER", StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    // Creating a stream for inserting data into the table concurrently
    ManualSource<RowData> insertSource =
        new ManualSource<>(
            infra.env(), InternalTypeInfo.of(FlinkSchemaUtil.convert(table.schema())));
    FlinkSink.forRowData(insertSource.getDataStream()).tableLoader(tableLoader).append();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      Path inRoot = relative(table, "in_root");
      Path inData = relative(table, "data/in_data");
      Path inMetadata = relative(table, "metadata/in_metadata");

      createFiles(inRoot, inData, inMetadata);

      assertThat(inRoot).exists();
      assertThat(inData).exists();
      assertThat(inMetadata).exists();

      // Do a single orphan file removal run
      infra
          .source()
          .sendRecord(
              (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis() + 1);

      // Wait until the extra files are removed
      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isTrue();
      Awaitility.await().until(() -> !Files.exists(inRoot));
      Awaitility.await().until(() -> !Files.exists(inData));
      Awaitility.await().until(() -> !Files.exists(inMetadata));

      // Add new files to the table
      table.refresh();
      long oldSnapshot = table.currentSnapshot().snapshotId();
      insertSource.sendRecord(row(5, "e", "p2"));
      // Wait until the changes are committed
      Awaitility.await()
          .until(
              () -> {
                table.refresh();
                return oldSnapshot != table.currentSnapshot().snapshotId();
              });

      // Add some extra files
      createFiles(inRoot, inData, inMetadata);

      assertThat(inRoot).exists();
      assertThat(inData).exists();
      assertThat(inMetadata).exists();

      // Do a second orphan file removal run
      infra
          .source()
          .sendRecord(
              (SerializableTable) SerializableTable.copyOf(table),
              System.currentTimeMillis() + 1001);

      // Wait until the extra files are removed
      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isTrue();
      Awaitility.await().until(() -> !Files.exists(inRoot));
      Awaitility.await().until(() -> !Files.exists(inData));
      Awaitility.await().until(() -> !Files.exists(inMetadata));

      // Check if we can still read the table, and the data is correct
      SimpleDataUtil.assertTableRecords(
          table,
          ImmutableList.of(
              record(1, "a", "p1"),
              record(1, "b", "p2"),
              record(4, "d", "p1"),
              record(5, "e", "p2")));
      SimpleDataUtil.assertTableRecords(
          table,
          ImmutableList.of(record(1, "a", "p1"), record(1, "b", "p2"), record(3, "c", "p1")),
          "b1");
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testFailure() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    long currentSnapshotId = table.currentSnapshot().snapshotId();

    DeleteOrphanFiles.builder(infra.env())
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

    // Assert that the table data did not change
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(currentSnapshotId);
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(record(1, "a", "p1"), record(2, "b", "p2")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_SUCCESS_METRIC, 0L)
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_ERROR_METRIC, 0L)
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 1L)
            .put(READER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(METADATA_FILES_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 1L)
            .put(FILESYSTEM_FILES_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    DeleteOrphanFiles.builder(infra.env())
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .uidPrefix(UID_PREFIX)
        .init(0, DUMMY_NAME, tableLoader, null, null, 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), UID_PREFIX);
    checkSlotSharingGroupsAreSet(infra.env(), SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    DeleteOrphanFiles.builder(infra.env())
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
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    DeleteOrphanFiles.builder(infra.env())
        .minAge(Duration.ZERO)
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    Path inRoot = relative(table, "in_root");
    createFiles(inRoot);
    assertThat(inRoot).exists();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Do a single orphan file removal run
      infra
          .source()
          .sendRecord(
              (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis() + 1);

      // Wait until the extra files are removed
      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isTrue();
      Awaitility.await().until(() -> !Files.exists(inRoot));
    } finally {
      closeJobClient(jobClient);
    }

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_SUCCESS_METRIC, 1L)
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_ERROR_METRIC, 0L)
            .put(PLANNER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(READER_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(METADATA_FILES_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .put(FILESYSTEM_FILES_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .build());
  }

  private Path relative(Table table, String relativePath) {
    return FileSystems.getDefault().getPath(table.location().substring(5), relativePath);
  }

  private void createFiles(Path... paths) throws IOException {
    for (Path path : paths) {
      Files.write(path, "DUMMY".getBytes(StandardCharsets.UTF_8));
    }
  }
}
