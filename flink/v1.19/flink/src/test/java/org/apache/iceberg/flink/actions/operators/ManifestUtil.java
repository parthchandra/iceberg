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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

class ManifestUtil {
  private ManifestUtil() {
    // Do not instantiate
  }

  static RowType metadataTableRowType(Table table) {
    return FlinkSchemaUtil.convert(
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ENTRIES).schema());
  }

  static Tuple2<RowData, RowType> extractDataFile(RowData data, RowType type) {
    int fileIndex = type.getFieldIndex(WriteManifests.DATA_FILE);
    RowType fileType = (RowType) type.getTypeAt(fileIndex);
    return Tuple2.of(data.getRow(fileIndex, fileType.getFieldCount()), fileType);
  }

  static String extractDataFilePath(RowData data, RowType type) {
    Tuple2<RowData, RowType> dataFile = extractDataFile(data, type);
    return dataFile.f0.getString(dataFile.f1.getFieldIndex(DataFile.FILE_PATH.name())).toString();
  }

  static int extractDataFileContent(RowData data, RowType type) {
    Tuple2<RowData, RowType> dataFile = extractDataFile(data, type);
    return dataFile.f0.getInt(dataFile.f1.getFieldIndex(DataFile.CONTENT.name()));
  }

  static List<RowData> readSplits(TableLoader tableLoader, List<IcebergSourceSplit> splits)
      throws Exception {
    SerializableTable metaTable =
        (SerializableTable)
            SerializableTable.copyOf(
                MetadataTableUtils.createMetadataTableInstance(
                    tableLoader.loadTable(), MetadataTableType.ENTRIES));
    try (OneInputStreamOperatorTestHarness<IcebergSourceSplit, RowData> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new TableReader(DUMMY_NAME, metaTable))) {
      testHarness.open();

      for (IcebergSourceSplit split : splits) {
        testHarness.processElement(split, System.currentTimeMillis());
      }

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static List<IcebergSourceSplit> planEntriesSplits(TableLoader tableLoader) throws Exception {
    SerializableTable table =
        (SerializableTable)
            SerializableTable.copyOf(
                MetadataTableUtils.createMetadataTableInstance(
                    tableLoader.loadTable(), MetadataTableType.ENTRIES));

    try (OneInputStreamOperatorTestHarness<SerializableTable, IcebergSourceSplit> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(new TablePlanner(DUMMY_NAME, table, 10))) {
      testHarness.open();

      testHarness.processElement(table, System.currentTimeMillis());

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static List<ManifestFile> writeManifests(TableLoader tableLoader, List<RowData> entries)
      throws Exception {
    WriteManifests writeManifests = writeManifests(tableLoader.loadTable(), false);

    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();

      for (RowData entry : entries) {
        testHarness.processElement(entry, EVENT_TIME);
      }

      testHarness.processWatermark(new Watermark(EVENT_TIME));

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static WriteManifests writeManifests(Table table, boolean wrongLocation) {
    SerializableTable serializedTable = (SerializableTable) SerializableTable.copyOf(table);

    return new WriteManifests(
        DUMMY_NAME,
        serializedTable,
        wrongLocation ? "https://invalid" : table.location() + "/metadata",
        ((HasTableOperations) table).operations().current().formatVersion(),
        1L,
        100,
        true);
  }

  static KeyedTwoInputStreamOperatorTestHarness<
          Long,
          Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
          Tuple2<Long, Exception>,
          Tuple2<Long, Boolean>>
      harnessForUpdater(ManifestUpdater manifestUpdater) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(manifestUpdater),
        (KeySelector<Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>, Long>)
            value -> value.f0,
        value -> value.f0,
        Types.LONG,
        1,
        1,
        0);
  }

  static String outputLocation(Table table) {
    return table.location() + "/metadata";
  }
}
