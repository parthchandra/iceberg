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

import java.io.IOException;
import java.util.List;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Test;

class TestTableReader extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    List<RowData> actual = ManifestUtil.readSplits(tableLoader, splits);

    assertManifests(actual, tableLoader);
  }

  @Test
  void testPartitioned() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar, PRIMARY KEY(`id`, `spec`) NOT ENFORCED) PARTITIONED BY (spec) WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    List<RowData> actual = ManifestUtil.readSplits(tableLoader, splits);

    assertManifests(actual, tableLoader);
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    SerializableTable table = (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());

    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    assertThat(splits).hasSize(1);

    try (OneInputStreamOperatorTestHarness<IcebergSourceSplit, RowData> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new TableReader(DUMMY_NAME, table))) {
      testHarness.open();

      // Cause an exception
      sql.exec("DROP TABLE IF EXISTS %s", TABLE_NAME);

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(splits.get(0), System.currentTimeMillis());
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

  private static void assertManifests(List<RowData> actual, TableLoader tableLoader)
      throws IOException {
    Table table = tableLoader.loadTable();
    RowType rowType = ManifestUtil.metadataTableRowType(table);
    List<ManifestFile> dataManifests = table.currentSnapshot().dataManifests(table.io());
    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());

    for (RowData row : actual) {
      String filePath = ManifestUtil.extractDataFilePath(row, rowType);
      int content = ManifestUtil.extractDataFileContent(row, rowType);
      assertEntry(
          row, rowType, findManifest(dataManifests, deleteManifests, filePath, content, table));
    }
  }

  private static ManifestFile findManifest(
      List<ManifestFile> dataManifests,
      List<ManifestFile> deleteManifests,
      String filePath,
      int content,
      Table table)
      throws IOException {
    switch (content) {
      case 0:
        // Data
        for (ManifestFile manifest : dataManifests) {
          try (CloseableIterator<DataFile> dataFiles =
              ManifestFiles.read(manifest, table.io(), table.specs()).iterator()) {
            while (dataFiles.hasNext()) {
              if (filePath.equals(dataFiles.next().path().toString())) {
                return manifest;
              }
            }
          }
        }

        break;
      case 1:
      case 2:
        // Delete
        for (ManifestFile manifest : deleteManifests) {
          try (CloseableIterator<DeleteFile> deleteFiles =
              ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs()).iterator()) {
            while (deleteFiles.hasNext()) {
              if (filePath.equals(deleteFiles.next().path().toString())) {
                return manifest;
              }
            }
          }
        }

        break;
    }

    // We could not find the ManifestFile based on the type and the filename
    return null;
  }

  private static void assertEntry(RowData actual, RowType rowType, ManifestFile expected) {
    assertThat(expected).isNotNull();
    // This information is coming from the ManifestFile because of the metadata inheritance
    assertThat(actual.getInt(rowType.getFieldIndex(WriteManifests.STATUS))).isEqualTo(1);
    assertThat(actual.getLong(rowType.getFieldIndex(WriteManifests.SNAPSHOT_ID)))
        .isEqualTo(expected.snapshotId());
    assertThat(actual.getLong(rowType.getFieldIndex(WriteManifests.SEQUENCE_NUMBER)))
        .isEqualTo(expected.sequenceNumber());
    assertThat(actual.getLong(rowType.getFieldIndex(WriteManifests.FILE_SEQUENCE_NUMBER)))
        .isEqualTo(expected.minSequenceNumber());
  }
}
