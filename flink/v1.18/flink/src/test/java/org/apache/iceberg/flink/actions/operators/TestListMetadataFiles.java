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
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestListMetadataFiles extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s /*+ OPTIONS('branch'='b1') */ VALUES (2, 'b')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<String> actual = listMetadataFiles(tableLoader);

    // Check that we have found all the metadata files. Use set to remove duplicates.
    assertRegex(
        Sets.newHashSet(actual),
        ImmutableList.of(
            ".*/test_table/metadata/version-hint.text",
            ".*/test_table/metadata/v1.metadata.json",
            ".*/test_table/metadata/v2.metadata.json",
            ".*/test_table/metadata/v3.metadata.json",
            ".*/test_table/metadata/v4.metadata.json",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/snap-.*.avro",
            ".*/test_table/metadata/snap-.*.avro",
            ".*/test_table/metadata/snap-.*.avro"));
  }

  @Test
  void testPartitioned() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (data, spec)",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s /*+ OPTIONS('branch'='b1') */ VALUES (3, 'c', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<String> actual = listMetadataFiles(tableLoader);

    // Check that we have found all the metadata files. Use set to remove duplicates.
    assertRegex(
        Sets.newHashSet(actual),
        ImmutableList.of(
            ".*/test_table/metadata/version-hint.text",
            ".*/test_table/metadata/v1.metadata.json",
            ".*/test_table/metadata/v2.metadata.json",
            ".*/test_table/metadata/v3.metadata.json",
            ".*/test_table/metadata/v4.metadata.json",
            ".*/test_table/metadata/v5.metadata.json",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/.*-m0.avro",
            ".*/test_table/metadata/snap-.*.avro",
            ".*/test_table/metadata/snap-.*.avro",
            ".*/test_table/metadata/snap-.*.avro",
            ".*/test_table/metadata/snap-.*.avro"));
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    SerializableTable table = (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());

    try (OneInputStreamOperatorTestHarness<SerializableTable, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new ListMetadataFiles(DUMMY_NAME))) {
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

  private static List<String> listMetadataFiles(TableLoader tableLoader) throws Exception {
    try (OneInputStreamOperatorTestHarness<SerializableTable, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new ListMetadataFiles(DUMMY_NAME))) {
      testHarness.open();
      testHarness.processElement(
          (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable()),
          System.currentTimeMillis());
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }
}
