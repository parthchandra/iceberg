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

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestListFileSystemFiles extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s /*+ OPTIONS('branch'='b1') */ VALUES (2, 'b')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c')", TABLE_NAME);

    List<String> actual = listFiles(System.currentTimeMillis() + 1, 0);

    // Check that we have found all the data/metadata files, but did not find the crc, and hidden
    // files
    assertRegex(
        actual,
        ImmutableList.of(
            ".*/test_table/data/00000-0-.*.parquet",
            ".*/test_table/data/00000-0-.*.parquet",
            ".*/test_table/data/00000-0-.*.parquet",
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

    List<String> actual = listFiles(System.currentTimeMillis() + 1, 0);

    // Check that we have found all the data/metadata files, but did not find the crc, and hidden
    // files
    assertRegex(
        actual,
        ImmutableList.of(
            ".*/test_table/data/data=a/spec=p1/00000-0-.*.parquet",
            ".*/test_table/data/data=b/spec=p2/00000-0-.*.parquet",
            ".*/test_table/data/data=c/spec=p1/00000-0-.*.parquet",
            ".*/test_table/data/data=d/spec=p1/00000-0-.*.parquet",
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
  void testMinAgeMs() throws Exception {
    long before = System.currentTimeMillis();
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    long after = System.currentTimeMillis() + 1;

    assertThat(listFiles(before, 0)).isEmpty();
    assertThat(listFiles(after, 0)).hasSize(2);
    assertThat(listFiles(after, after - before)).isEmpty();
  }

  @Test
  void testExtraFile() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    // Write an extra files
    Path extra = FileSystems.getDefault().getPath(table.location().substring(5), "extra");
    Files.write(extra, "DUMMY".getBytes(StandardCharsets.UTF_8));

    List<String> actual = listFiles(System.currentTimeMillis() + 1, 0);

    assertThat(actual).contains("file:" + extra);
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    try (OneInputStreamOperatorTestHarness<Long, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListFileSystemFiles(
                DUMMY_NAME,
                (SupportsPrefixOperations) table.io(),
                "invalid",
                ImmutableMap.of(),
                0))) {
      testHarness.open();
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(System.currentTimeMillis(), System.currentTimeMillis());
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(ErrorAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("File invalid does not exist");
    }
  }

  private List<String> listFiles(long eventTime, long minAgeMs) throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    try (OneInputStreamOperatorTestHarness<Long, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListFileSystemFiles(
                DUMMY_NAME,
                (SupportsPrefixOperations) table.io(),
                table.location(),
                table.specs(),
                minAgeMs))) {
      testHarness.open();
      testHarness.processElement(eventTime, eventTime);
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }
}
