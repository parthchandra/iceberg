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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME_2;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.apache.iceberg.flink.actions.operators.ManifestUtil.outputLocation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.ManifestForTests;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestWriteManifests extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    List<RowData> entries = ManifestUtil.readSplits(tableLoader, splits);

    List<ManifestFile> actual = ManifestUtil.writeManifests(tableLoader, entries);

    assertManifests(tableLoader, actual);
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

    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    List<RowData> entries = ManifestUtil.readSplits(tableLoader, splits);

    List<ManifestFile> actual = ManifestUtil.writeManifests(tableLoader, entries);

    assertManifests(tableLoader, actual);
  }

  @Test
  void testWithoutPartialCommit() throws Exception {
    int manifestNum = 4;
    int rowsPerManifest = 3;

    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p4')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    SerializableTable serializedTable = (SerializableTable) SerializableTable.copyOf(table);

    List<Tuple3<String, RowData, ContentFile>> testData =
        generateTestData(tableLoader, manifestNum);

    // Run the actual test
    WriteManifests writeManifests =
        new WriteManifests(
            DUMMY_NAME,
            serializedTable,
            outputLocation(table),
            ((HasTableOperations) table).operations().current().formatVersion(),
            1L,
            rowsPerManifest,
            false);

    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();
      for (Tuple3<String, RowData, ContentFile> entry : testData) {
        testHarness.processElement(entry.f1, EVENT_TIME);
      }

      // A single manifest file has been generated, but the side output is empty
      assertManifestContent(
          testHarness.extractOutputValues(), 1, testData.subList(0, rowsPerManifest), table);
      assertThat(testHarness.getSideOutput(WriteManifests.REMAINING)).isNull();

      // Check that the timestamps are correct in the result
      testHarness
          .extractOutputStreamRecords()
          .forEach(streamRecord -> assertThat(streamRecord.getTimestamp()).isEqualTo(EVENT_TIME));

      testHarness.processWatermark(EVENT_TIME);

      // No more manifest files are generated
      assertThat(testHarness.extractOutputValues()).hasSize(1);
      assertSideOutput(
          testHarness.getSideOutput(WriteManifests.REMAINING),
          testData.subList(rowsPerManifest, testData.size()));

      // No changes in the input after close
      testHarness.close();
      assertThat(testHarness.extractOutputValues()).hasSize(1);
      assertThat(testHarness.getSideOutput(WriteManifests.REMAINING))
          .hasSize(manifestNum - rowsPerManifest);
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testStateRestore() throws Exception {
    int manifestNum = 5;
    int rowsPerManifest = 2;

    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p4')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (5, 'e', 'p5')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    SerializableTable serializedTable = (SerializableTable) SerializableTable.copyOf(table);
    String location = outputLocation(table);
    int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();

    List<Tuple3<String, RowData, ContentFile>> testData =
        generateTestData(tableLoader, manifestNum);

    // Run the actual test
    WriteManifests writeManifests =
        new WriteManifests(
            DUMMY_NAME, serializedTable, location, formatVersion, 1L, rowsPerManifest, true);

    // Write some data, create a checkpoint, check the data which is already written
    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();

      for (int i = 0; i < 3; ++i) {
        testHarness.processElement(testData.get(i).f1, EVENT_TIME);
      }

      // A single manifest file has been generated, but the side output is empty
      assertManifestContent(
          testHarness.extractOutputValues(), 1, testData.subList(0, rowsPerManifest), table);
      assertThat(testHarness.getSideOutput(WriteManifests.REMAINING)).isNull();
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data which is written
    writeManifests =
        new WriteManifests(
            DUMMY_NAME, serializedTable, location, formatVersion, 1L, rowsPerManifest, true);
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement(testData.get(3).f1, EVENT_TIME);
      testHarness.processElement(testData.get(4).f1, EVENT_TIME);
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      assertManifestContent(
          testHarness.extractOutputValues(),
          1,
          testData.subList(rowsPerManifest, 2 * rowsPerManifest),
          table);

      state = testHarness.snapshot(2, EVENT_TIME);
    }

    // Restore the checkpoint, do not write data, but emit a watermark which flushes the data, check
    // the result
    writeManifests =
        new WriteManifests(
            DUMMY_NAME, serializedTable, location, formatVersion, 1L, rowsPerManifest, true);
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.initializeState(state);
      testHarness.open();
      testHarness.processWatermark(EVENT_TIME);

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      assertManifestContent(
          testHarness.extractOutputValues(),
          1,
          testData.subList(2 * rowsPerManifest, manifestNum),
          table);
    }
  }

  @Test
  void testMixedKeys() throws Exception {
    int rowsPerManifest = 10;

    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar, PRIMARY KEY(`id`, `spec`) NOT ENFORCED) "
            + "WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    SerializableTable serializedTable = (SerializableTable) SerializableTable.copyOf(table);
    String location = outputLocation(table);
    int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();

    List<Tuple3<String, RowData, ContentFile>> testData = generateTestData(tableLoader, 4);

    WriteManifests writeManifests =
        new WriteManifests(
            DUMMY_NAME, serializedTable, location, formatVersion, 1L, rowsPerManifest, true);

    // Write some data, create a checkpoint, check the data which is already written
    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();

      for (int i = 0; i < 3; ++i) {
        testHarness.processElement(testData.get(i).f1, EVENT_TIME);
      }

      // A no new manifest file has been generated, no data is in the side output yet
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(WriteManifests.REMAINING)).isNull();
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Insert some new data, and start inserting those records
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p4')", TABLE_NAME);

    List<Tuple3<String, RowData, ContentFile>> testData2 = generateTestData(tableLoader, 8);

    writeManifests =
        new WriteManifests(
            DUMMY_NAME, serializedTable, location, formatVersion, 1L, rowsPerManifest, true);

    // Restore data from savepoint, insert some data for the new run, and finish the old run
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.initializeState(state);

      for (int i = 0; i < 6; ++i) {
        testHarness.processElement(testData2.get(i).f1, EVENT_TIME_2);
      }

      for (int i = 3; i < testData.size(); ++i) {
        testHarness.processElement(testData.get(i).f1, EVENT_TIME);
      }

      // A no new manifest file has been generated, no data is in the side output yet
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(WriteManifests.REMAINING)).isNull();
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();

      state = testHarness.snapshot(2, EVENT_TIME_2);
    }

    writeManifests =
        new WriteManifests(
            DUMMY_NAME, serializedTable, location, formatVersion, 1L, rowsPerManifest, true);

    // Restore data from savepoint, finish the old run, and then the new run
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.initializeState(state);

      testHarness.processWatermark(EVENT_TIME);

      assertManifestContent(testHarness.extractOutputValues(), 2, testData, table);

      for (int i = 6; i < testData2.size(); ++i) {
        testHarness.processElement(testData2.get(i).f1, EVENT_TIME_2);
      }

      testHarness.processWatermark(EVENT_TIME_2);
      List<ManifestFile> output = testHarness.extractOutputValues();
      assertManifestContent(
          output.subList(2, output.size()), 2, Lists.newArrayList(testData2), table);
      assertThat(testHarness.getSideOutput(WriteManifests.REMAINING)).isNull();
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    List<RowData> entries = ManifestUtil.readSplits(tableLoader, splits);

    WriteManifests writeManifests = ManifestUtil.writeManifests(tableLoader.loadTable(), true);

    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      for (RowData entry : entries) {
        testHarness.processElement(entry, EVENT_TIME);
      }

      testHarness.processWatermark(new Watermark(EVENT_TIME));
      // Error both on processElement and on processWatermark
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).hasSize(2);
      assertThat(
              testHarness
                  .getSideOutput(ErrorAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("Relative path in absolute URI");
      assertThat(
              testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM).poll().getValue().getClass())
          .isEqualTo(NullPointerException.class);
    }
  }

  private static List<Tuple3<String, RowData, ContentFile>> generateTestData(
      TableLoader tableLoader, int expectedSize) throws Exception {
    Table table = tableLoader.loadTable();
    List<IcebergSourceSplit> splits = ManifestUtil.planEntriesSplits(tableLoader);
    List<RowData> entries = ManifestUtil.readSplits(tableLoader, splits);
    List<ContentFile> manifestFiles =
        readDataFiles(table.currentSnapshot().dataManifests(table.io()), table);
    manifestFiles.addAll(
        readDeleteFiles(table.currentSnapshot().deleteManifests(table.io()), table));
    assertThat(entries).hasSize(expectedSize);
    assertThat(manifestFiles).hasSize(expectedSize);

    List<Tuple3<String, RowData, ContentFile>> testData = organize(entries, manifestFiles, table);
    assertThat(testData).hasSize(expectedSize);
    return testData;
  }

  private static void assertManifestContent(
      List<ManifestFile> actual,
      int expectedManifestNum,
      List<Tuple3<String, RowData, ContentFile>> expected,
      Table table)
      throws IOException {
    assertThat(actual).hasSize(expectedManifestNum);
    // Read the new manifest files and check the data
    List<ContentFile> actualFiles =
        readDataFiles(
            actual.stream()
                .filter(manifestFile -> manifestFile.content().equals(ManifestContent.DATA))
                .map(ManifestForTests::new)
                .collect(Collectors.toList()),
            table);
    actualFiles.addAll(
        readDeleteFiles(
            actual.stream()
                .filter(manifestFile -> manifestFile.content().equals(ManifestContent.DELETES))
                .map(ManifestForTests::new)
                .collect(Collectors.toList()),
            table));
    assertThat(actualFiles).hasSize(expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      assertEntry(actualFiles.get(i), expected.get(i).f2);
    }
  }

  private static void assertSideOutput(
      ConcurrentLinkedQueue<StreamRecord<RowData>> actual,
      List<Tuple3<String, RowData, ContentFile>> expected) {
    List<Tuple2<Long, RowData>> values =
        actual.stream()
            .map(streamRecord -> Tuple2.of(streamRecord.getTimestamp(), streamRecord.getValue()))
            .collect(Collectors.toList());
    assertThat(values).hasSize(expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      assertThat(values.get(i).f0).isEqualTo(EVENT_TIME);
      assertThat(values.get(i).f1).isEqualTo(expected.get(i).f1);
    }
  }

  private static void assertManifests(TableLoader tableLoader, List<ManifestFile> actual)
      throws IOException {
    Table table = tableLoader.loadTable();
    Snapshot current = table.currentSnapshot();
    List<ContentFile> expectedDataFiles = readDataFiles(current.dataManifests(table.io()), table);
    List<ContentFile> expectedDeleteFiles =
        readDeleteFiles(current.deleteManifests(table.io()), table);

    List<ContentFile> actualDataFiles =
        Lists.newArrayListWithExpectedSize(expectedDataFiles.size());
    List<ContentFile> actualDeleteFiles =
        Lists.newArrayListWithExpectedSize(expectedDeleteFiles.size());

    for (ManifestFile newManifest : actual) {
      switch (newManifest.content()) {
        case DATA:
          actualDataFiles.addAll(
              readDataFiles(ImmutableList.of(new ManifestForTests(newManifest)), table));
          break;
        case DELETES:
          actualDeleteFiles.addAll(
              readDeleteFiles(ImmutableList.of(new ManifestForTests(newManifest)), table));
          break;
      }
    }

    assertEntries(actualDataFiles, expectedDataFiles);
    assertEntries(actualDeleteFiles, expectedDeleteFiles);
  }

  private static List<ContentFile> readDataFiles(List<ManifestFile> manifestFiles, Table table)
      throws IOException {
    List<ContentFile> result = Lists.newArrayList();
    FileIO fileIO = EncryptingFileIO.combine(table.io(), table.encryption());
    for (ManifestFile manifestFile : manifestFiles) {
      try (CloseableIterator<DataFile> iterator =
          ManifestFiles.read(manifestFile, fileIO, table.specs()).iterator()) {
        result.addAll(Lists.newArrayList(iterator));
      }
    }

    return result;
  }

  private static List<ContentFile> readDeleteFiles(List<ManifestFile> manifestFiles, Table table)
      throws IOException {
    List<ContentFile> result = Lists.newArrayList();
    FileIO fileIO = EncryptingFileIO.combine(table.io(), table.encryption());
    for (ManifestFile manifestFile : manifestFiles) {
      try (CloseableIterator<DeleteFile> iterator =
          ManifestFiles.readDeleteManifest(manifestFile, fileIO, table.specs()).iterator()) {
        result.addAll(Lists.newArrayList(iterator));
      }
    }

    return result;
  }

  private static <F extends ContentFile<F>> void assertEntries(List<F> actual, List<F> expected) {
    assertThat(actual).hasSize(expected.size());
    actual.forEach(
        actualFile -> {
          Optional<F> found =
              expected.stream()
                  .filter(expectedFile -> expectedFile.path().equals(actualFile.path()))
                  .findFirst();
          assertThat(found).isPresent();
          assertEntry(actualFile, found.get());
        });
  }

  private static <F extends ContentFile<F>> void assertEntry(F actual, F expected) {
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.path()).isEqualTo(expected.path());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.partition()).isEqualTo(expected.partition());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.columnSizes()).isEqualTo(expected.columnSizes());
    assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
    assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
    assertThat(actual.dataSequenceNumber()).isEqualTo(expected.dataSequenceNumber());
    assertThat(actual.fileSequenceNumber()).isEqualTo(expected.fileSequenceNumber());
  }

  private static List<Tuple3<String, RowData, ContentFile>> organize(
      List<RowData> rowDatas, List<ContentFile> contentFiles, Table table) {
    RowType rowType = ManifestUtil.metadataTableRowType(table);
    return rowDatas.stream()
        .map(
            rowData -> {
              String fileName = ManifestUtil.extractDataFilePath(rowData, rowType);
              ContentFile dataFile =
                  contentFiles.stream()
                      .filter(file -> file.path().toString().equals(fileName))
                      .findFirst()
                      .get();
              return Tuple3.of(fileName, rowData, dataFile);
            })
        .collect(Collectors.toList());
  }
}
