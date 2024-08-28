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

import static org.apache.iceberg.actions.SizeBasedFileRewriter.MIN_INPUT_FILES;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.DUMMY_NAME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.newDataFiles;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.planDataFileRewrite;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.reconstructFileGroups;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestDataFileRewritePlanner extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    Set<DataFile> expected = Sets.newHashSetWithExpectedSize(3);
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    expected.addAll(newDataFiles(table));
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    expected.addAll(newDataFiles(table));
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    expected.addAll(newDataFiles(table));

    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> actual =
        reconstructFileGroups(planDataFileRewrite(table));

    assertThat(actual).hasSize(1);
    assertRewriteFileGroup(actual.get(0), table, expected);
  }

  @Test
  void testSkipNewer() throws Exception {
    Set<DataFile> expected = Sets.newHashSetWithExpectedSize(2);
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    expected.addAll(newDataFiles(table));
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    expected.addAll(newDataFiles(table));

    // Table not refreshed after this insert, so this file should not participate in compaction
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);

    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> actual =
        reconstructFileGroups(planDataFileRewrite(table));

    assertThat(actual).hasSize(1);
    assertRewriteFileGroup(actual.get(0), table, expected);
  }

  @Test
  void testPartitioned() throws Exception {
    Set<DataFile> expectedP1 = Sets.newHashSetWithExpectedSize(2);
    Set<DataFile> expectedP2 = Sets.newHashSetWithExpectedSize(2);
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    expectedP1.addAll(newDataFiles(table));
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    expectedP1.addAll(newDataFiles(table));
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p2')", TABLE_NAME);
    expectedP2.addAll(newDataFiles(table));
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p2')", TABLE_NAME);
    expectedP2.addAll(newDataFiles(table));
    // This should not participate in compaction, as there is no more files in the partition
    sql.exec("INSERT INTO %s VALUES (5, 'e', 'p3')", TABLE_NAME);
    table.refresh();

    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> actual =
        reconstructFileGroups(planDataFileRewrite(table));

    assertThat(actual).hasSize(2);
    if (actual.get(0).f3.info().partition().get(0, String.class).equals("p1")) {
      assertRewriteFileGroup(actual.get(0), table, expectedP1);
      assertRewriteFileGroup(actual.get(1), table, expectedP2);
    } else {
      assertRewriteFileGroup(actual.get(0), table, expectedP2);
      assertRewriteFileGroup(actual.get(1), table, expectedP1);
    }
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    try (OneInputStreamOperatorTestHarness<SerializableTable, DataFileRewriteTask> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new DataFileRewritePlanner(
                DUMMY_NAME, serializableTable, ImmutableMap.of(MIN_INPUT_FILES, "2"), 11, 1))) {
      testHarness.open();

      // Cause an exception
      sql.exec("DROP TABLE IF EXISTS %s", TABLE_NAME);

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(serializableTable, System.currentTimeMillis());
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(ErrorAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("Failed to open input stream for file");
    }
  }

  @Test
  void testMaxRewriteBytes() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p2')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    // First run with high limit
    List<DataFileRewriteTask> planWithNoLimit = planDataFileRewrite(table);
    assertThat(planWithNoLimit).hasSize(4);

    // Second run with limit
    long limit =
        planWithNoLimit.get(0).getTask().sizeBytes()
            + planWithNoLimit.get(1).getTask().sizeBytes()
            + 1;
    try (OneInputStreamOperatorTestHarness<SerializableTable, DataFileRewriteTask> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new DataFileRewritePlanner(
                DUMMY_NAME, serializableTable, ImmutableMap.of(MIN_INPUT_FILES, "2"), 11, limit))) {
      testHarness.open();

      testHarness.processElement(serializableTable, System.currentTimeMillis());

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      // Only two groups are planned
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testV2Table() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, PRIMARY KEY(`id`) NOT ENFORCED) "
            + "WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a'), (1, 'b')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'c')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> actual =
        reconstructFileGroups(planDataFileRewrite(table));

    assertThat(actual).hasSize(1);
    List<FileScanTask> tasks = actual.get(0).f3.fileScans();
    assertThat(tasks).hasSize(2);
    // Find the task with the deletes
    FileScanTask withDelete = tasks.get(0).deletes().isEmpty() ? tasks.get(1) : tasks.get(0);
    assertThat(withDelete.deletes()).hasSize(2);
    // Find the equality delete and the positional delete
    if (withDelete.deletes().get(0).content() == FileContent.EQUALITY_DELETES) {
      assertThat(withDelete.deletes().get(1).content()).isEqualTo(FileContent.POSITION_DELETES);
    } else {
      assertThat(withDelete.deletes().get(0).content()).isEqualTo(FileContent.POSITION_DELETES);
      assertThat(withDelete.deletes().get(1).content()).isEqualTo(FileContent.EQUALITY_DELETES);
    }
  }

  void assertRewriteFileGroup(
      Tuple4<Long, SerializableTable, Integer, RewriteFileGroup> actual,
      Table table,
      Set<DataFile> files) {
    assertThat(actual.f1.currentSnapshot().snapshotId())
        .isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(actual.f2).isEqualTo(1);
    assertThat(actual.f3.fileScans().stream().map(s -> s.file().path()).collect(Collectors.toSet()))
        .isEqualTo(files.stream().map(ContentFile::path).collect(Collectors.toSet()));
  }
}
