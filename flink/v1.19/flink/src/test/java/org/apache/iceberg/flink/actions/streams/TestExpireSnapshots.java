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
import static org.apache.iceberg.flink.actions.operators.MetricConstants.DELETE_FILE_ERROR_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.DELETE_FILE_SUCCESS_METRIC;
import static org.apache.iceberg.flink.actions.operators.MetricConstants.MAINTENANCE_ERROR_METRIC;
import static org.apache.iceberg.flink.actions.streams.ExpireSnapshots.DELETE_FILES_TASK_NAME;
import static org.apache.iceberg.flink.actions.streams.ExpireSnapshots.EXECUTOR_TASK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestExpireSnapshots extends ScheduledBuilderTestBase {
  @Test
  void testExpireSnapshots() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p4')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    Set<Snapshot> snapshots = Sets.newHashSet(table.snapshots());
    assertThat(snapshots).hasSize(4);

    ExpireSnapshots.builder()
        .parallelism(2)
        .planningWorkerPoolSize(2)
        .deleteAttemptNum(2)
        .deleteWorkerPoolSize(5)
        .minAge(Duration.ZERO)
        .retainLast(1)
        .uidPrefix(UID_PREFIX)
        .init(
            0, DUMMY_NAME, tableLoader, "OTHER", StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, 1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), table);

    // Check that the table data not changed
    table.refresh();
    assertThat(Sets.newHashSet(table.snapshots())).hasSize(1);
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
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    ExpireSnapshots.builder()
        .init(
            0,
            DUMMY_NAME,
            tableLoader,
            UID_PREFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .build(infra.triggerStream())
        .sinkTo(infra.sink());

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Do a single task run
      infra.source().sendRecord(serializableTable, System.currentTimeMillis());

      // First successful run (ensure that the operators are loaded/opened etc.)
      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isTrue();

      // Drop the table, so it will cause an exception
      sql.catalogLoader().loadCatalog().dropTable(TableIdentifier.of(DB_NAME, TABLE_NAME));

      // Failed run
      infra.source().sendRecord(serializableTable, System.currentTimeMillis() + 1);

      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isFalse();
    } finally {
      closeJobClient(jobClient);
    }

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        DUMMY_NAME,
        new ImmutableMap.Builder<String, Long>()
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_SUCCESS_METRIC, 0L)
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_ERROR_METRIC, 0L)
            .put(EXECUTOR_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 1L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    ExpireSnapshots.builder()
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
  void testUidAndSlotSharingGroupUnset() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    ExpireSnapshots.builder()
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

    ExpireSnapshots.builder()
        .minAge(Duration.ZERO)
        .retainLast(1)
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
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_SUCCESS_METRIC, 1L)
            .put(DELETE_FILES_TASK_NAME + "." + DELETE_FILE_ERROR_METRIC, 0L)
            .put(EXECUTOR_TASK_NAME + "." + MAINTENANCE_ERROR_METRIC, 0L)
            .build());
  }
}
