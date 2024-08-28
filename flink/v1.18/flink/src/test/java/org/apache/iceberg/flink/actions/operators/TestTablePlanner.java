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

import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.DUMMY_NAME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.jupiter.api.Test;

class TestTablePlanner extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<IcebergSourceSplit> actual = ManifestUtil.planEntriesSplits(tableLoader);

    assertEntriesSchema(actual);
    assertManifestNames(actual, tableLoader.loadTable());
  }

  @Test
  void testPartitioned() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar, PRIMARY KEY(`id`, `spec`) NOT ENFORCED) "
            + "PARTITIONED BY (spec) WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<IcebergSourceSplit> actual = ManifestUtil.planEntriesSplits(tableLoader);

    assertEntriesSchema(actual);
    assertManifestNames(actual, tableLoader.loadTable());
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    SerializableTable table = (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());

    try (OneInputStreamOperatorTestHarness<SerializableTable, IcebergSourceSplit> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new TablePlanner(DUMMY_NAME, table, 10))) {
      testHarness.open();

      // Cause an exception
      sql.exec("DROP TABLE IF EXISTS %s", TABLE_NAME);

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(table, System.currentTimeMillis());
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

  private static void assertManifestNames(List<IcebergSourceSplit> actual, Table table) {
    assertThat(
            actual.stream()
                .flatMap(split -> split.task().files().stream())
                .map(task -> task.file().path().toString())
                .collect(Collectors.toSet()))
        .isEqualTo(
            table.currentSnapshot().allManifests(table.io()).stream()
                .map(ManifestFile::path)
                .collect(Collectors.toSet()));
  }

  private static void assertEntriesSchema(List<IcebergSourceSplit> actual) {
    actual.stream()
        .map(s -> s.task().tasks().iterator().next().schema())
        .forEach(
            schema -> {
              assertThat(schema.findField("status")).isNotNull();
              assertThat(schema.findField("snapshot_id")).isNotNull();
              assertThat(schema.findField("sequence_number")).isNotNull();
              assertThat(schema.findField("file_sequence_number")).isNotNull();
              assertThat(schema.findField("data_file")).isNotNull();
            });
  }
}
