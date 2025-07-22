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
import org.junit.jupiter.api.Test;

class TestAllManifests extends OperatorTestBase {
  @Test
  void testFilter() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    table.updateSpec().addField("spec").commit();
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    table.refresh();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    List<ManifestFile> actual;
    try (OneInputStreamOperatorTestHarness<SerializableTable, ManifestFile> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new AllManifests(DUMMY_NAME))) {
      testHarness.open();

      testHarness.processElement(serializableTable, System.currentTimeMillis());
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      actual = testHarness.extractOutputValues();
    }

    // Only the last manifest is listed, the manifests with old spec are omitted
    assertThat(actual)
        .isEqualTo(
            table.currentSnapshot().allManifests(table.io()).stream()
                .filter(m -> m.partitionSpecId() == table.spec().specId())
                .collect(Collectors.toList()));
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    try (OneInputStreamOperatorTestHarness<SerializableTable, ManifestFile> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new AllManifests(DUMMY_NAME))) {
      testHarness.open();

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
}
