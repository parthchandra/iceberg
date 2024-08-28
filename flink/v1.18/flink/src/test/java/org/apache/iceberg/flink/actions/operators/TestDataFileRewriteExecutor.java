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

import static org.apache.iceberg.actions.SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES;
import static org.apache.iceberg.actions.SizeBasedFileRewriter.MIN_INPUT_FILES;
import static org.apache.iceberg.actions.SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.DUMMY_NAME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.executeRewrite;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.planDataFileRewrite;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.reconstructFileGroups;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.rewriteHarness;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestDataFileRewriteExecutor extends OperatorTestBase {
  private static final Set<StructLike> UNPARTITIONED =
      ImmutableSet.of(new PartitionData(PartitionSpec.unpartitioned().partitionType()));

  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> reconstructed =
        reconstructFileGroups(planned);
    assertThat(reconstructed).hasSize(1);
    List<Tuple3<Long, Integer, RewriteFileGroup>> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(
        actual.get(0),
        table,
        records(
            table.schema(),
            ImmutableSet.of(
                ImmutableList.of(1, "a", "p1"),
                ImmutableList.of(2, "b", "p2"),
                ImmutableList.of(3, "c", "p3"))),
        1,
        UNPARTITIONED);
  }

  @Test
  void testPartitioned() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    PartitionData partition = new PartitionData(table.spec().partitionType());
    partition.set(0, "p1");

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> reconstructed =
        reconstructFileGroups(planned);
    assertThat(reconstructed).hasSize(1);
    List<Tuple3<Long, Integer, RewriteFileGroup>> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(
        actual.get(0),
        table,
        records(
            table.schema(),
            ImmutableSet.of(
                ImmutableList.of(1, "a", "p1"),
                ImmutableList.of(2, "b", "p1"),
                ImmutableList.of(3, "c", "p1"))),
        1,
        ImmutableSet.of(partition));
  }

  @Test
  void testSplitSize() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    File dataDir = new File(new Path(table.location(), "data").toUri().getPath());
    dataDir.mkdir();
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(table, FileFormat.PARQUET, dataDir.toPath());
    Set<Record> expected = Sets.newHashSetWithExpectedSize(4000);
    for (int i = 0; i < 4; ++i) {
      List<Record> batch = RandomGenericData.generate(table.schema(), 1000, 10 + i);
      dataAppender.appendToTable(batch);
      expected.addAll(batch);
    }

    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    // First run with high limit
    List<DataFileRewriteTask> planWithNoLimit = planDataFileRewrite(table);
    assertThat(planWithNoLimit).hasSize(4);

    // Second run with limit
    long limit =
        planWithNoLimit.get(0).getTask().sizeBytes() + planWithNoLimit.get(1).getTask().sizeBytes();
    List<DataFileRewriteTask> planned;
    try (OneInputStreamOperatorTestHarness<SerializableTable, DataFileRewriteTask> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new DataFileRewritePlanner(
                DUMMY_NAME,
                serializableTable,
                ImmutableMap.of(
                    MIN_INPUT_FILES,
                    "2",
                    TARGET_FILE_SIZE_BYTES,
                    String.valueOf(limit),
                    MIN_FILE_SIZE_BYTES,
                    String.valueOf(limit - 1)),
                11,
                10_000_000))) {
      testHarness.open();

      testHarness.processElement(serializableTable, System.currentTimeMillis());

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      planned = testHarness.extractOutputValues();
      assertThat(planned).hasSize(4);
    }

    List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> reconstructed =
        reconstructFileGroups(planned);
    assertThat(reconstructed).hasSize(1);
    List<Tuple3<Long, Integer, RewriteFileGroup>> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(actual.get(0), table, expected, 2, UNPARTITIONED);
  }

  @Test
  void testPartitionSpecChange() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    PartitionData oldPartition = new PartitionData(table.spec().partitionType());
    oldPartition.set(0, "p1");

    try (KeyedOneInputStreamOperatorTestHarness<
            DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
        testHarness = rewriteHarness()) {
      testHarness.open();

      List<DataFileRewriteTask> planned = planDataFileRewrite(table);
      List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> reconstructed =
          reconstructFileGroups(planned);

      assertThat(planned).hasSize(2);
      assertThat(reconstructed).hasSize(1);

      processGroup(testHarness, reconstructed.get(0));
      List<Tuple3<Long, Integer, RewriteFileGroup>> actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(1);
      assertRewriteFileGroup(
          actual.get(0),
          table,
          records(
              table.schema(),
              ImmutableSet.of(ImmutableList.of(1, "a", "p1"), ImmutableList.of(2, "b", "p1"))),
          1,
          ImmutableSet.of(oldPartition));

      sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);
      table.refresh();

      planned = planDataFileRewrite(table);
      reconstructed = reconstructFileGroups(planned);
      assertThat(planned).hasSize(3);
      assertThat(reconstructed).hasSize(1);

      processGroup(testHarness, reconstructed.get(0));
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(2);
      assertRewriteFileGroup(
          actual.get(1),
          table,
          records(
              table.schema(),
              ImmutableSet.of(
                  ImmutableList.of(1, "a", "p1"),
                  ImmutableList.of(2, "b", "p1"),
                  ImmutableList.of(3, "c", "p1"))),
          1,
          ImmutableSet.of(oldPartition));

      // Alter the table schema
      table.updateSpec().addField("data").commit();
      // Insert some now data
      sql.exec("INSERT INTO %s VALUES (4, 'd', 'p1')", TABLE_NAME);
      sql.exec("INSERT INTO %s VALUES (5, 'd', 'p1')", TABLE_NAME);
      PartitionData newPartition = new PartitionData(table.spec().partitionType());
      newPartition.set(0, "p1");
      newPartition.set(1, "d");
      PartitionData[] transformedPartitions = {
        newPartition.copy(), newPartition.copy(), newPartition.copy()
      };
      transformedPartitions[0].set(1, "a");
      transformedPartitions[1].set(1, "b");
      transformedPartitions[2].set(1, "c");
      table.refresh();

      planned = planDataFileRewrite(table);
      reconstructed = reconstructFileGroups(planned);
      assertThat(planned).hasSize(5);
      assertThat(reconstructed).hasSize(2);

      Tuple4<Long, SerializableTable, Integer, RewriteFileGroup> oldCompact = reconstructed.get(0);
      Tuple4<Long, SerializableTable, Integer, RewriteFileGroup> newCompact = reconstructed.get(1);
      if (oldCompact.f3.fileScans().size() == 2) {
        newCompact = reconstructed.get(0);
        oldCompact = reconstructed.get(1);
      }

      processGroup(testHarness, newCompact);

      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(3);
      assertRewriteFileGroup(
          actual.get(2),
          table,
          records(
              table.schema(),
              ImmutableSet.of(ImmutableList.of(4, "d", "p1"), ImmutableList.of(5, "d", "p1"))),
          1,
          ImmutableSet.of(newPartition));

      processGroup(testHarness, oldCompact);
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(4);
      assertRewriteFileGroup(
          actual.get(3),
          table,
          records(
              table.schema(),
              ImmutableSet.of(
                  ImmutableList.of(1, "a", "p1"),
                  ImmutableList.of(2, "b", "p1"),
                  ImmutableList.of(3, "c", "p1"))),
          transformedPartitions.length,
          Sets.newHashSet(transformedPartitions));
    }
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    try (KeyedOneInputStreamOperatorTestHarness<
            DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
        testHarness = rewriteHarness()) {
      testHarness.open();

      List<DataFileRewriteTask> planned = planDataFileRewrite(table);
      assertThat(planned).hasSize(2);
      // Cause an exception
      sql.exec("DROP TABLE IF EXISTS %s", TABLE_NAME);

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(planned.get(0), System.currentTimeMillis());
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(ErrorAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("File does not exist");
    }
  }

  @Test
  void testV2Table() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar, PRIMARY KEY(`id`) NOT ENFORCED) "
            + "WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (1, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'c', 'p3')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(2);

    List<Tuple3<Long, Integer, RewriteFileGroup>> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(
        actual.get(0),
        table,
        records(table.schema(), ImmutableSet.of(ImmutableList.of(1, "c", "p3"))),
        1,
        UNPARTITIONED);
  }

  @Test
  void testStateRestore() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(2);

    OperatorSubtaskState state = null;
    try (KeyedOneInputStreamOperatorTestHarness<
            DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
        testHarness = rewriteHarness()) {
      testHarness.open();

      testHarness.processElement(planned.get(0), planned.get(0).getKey().getTimestamp());

      assertThat(testHarness.extractOutputValues()).isEmpty();

      state = testHarness.snapshot(1, System.currentTimeMillis());
    } catch (Exception e) {
      // do nothing
    }

    try (KeyedOneInputStreamOperatorTestHarness<
            DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DataFileRewriteExecutor(DUMMY_NAME)),
                DataFileRewriteTask::getKey,
                TypeInformation.of(DataFileRewriteTask.Key.class))) {

      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement(planned.get(1), planned.get(1).getKey().getTimestamp());
      testHarness.processWatermark(planned.get(1).getKey().getTimestamp());

      // The result is empty, as we can not restore the writer state and throw away the partial data
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  private void assertRewriteFileGroup(
      Tuple3<Long, Integer, RewriteFileGroup> actual,
      Table table,
      Set<Record> records,
      int expectedFileNum,
      Set<StructLike> partitions)
      throws IOException {
    assertThat(actual.f0).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(actual.f1).isEqualTo(1);
    assertThat(actual.f2.addedFiles()).hasSize(expectedFileNum);
    Set<Record> writtenRecords = Sets.newHashSetWithExpectedSize(records.size());
    Set<StructLike> writtenPartitions = Sets.newHashSetWithExpectedSize(partitions.size());
    for (DataFile newDataFile : actual.f2.addedFiles()) {
      assertThat(newDataFile.format()).isEqualTo(FileFormat.PARQUET);
      assertThat(newDataFile.content()).isEqualTo(FileContent.DATA);
      assertThat(newDataFile.keyMetadata()).isNull();
      writtenPartitions.add(newDataFile.partition());

      try (CloseableIterable<Record> reader =
          Parquet.read(table.io().newInputFile(newDataFile.path().toString()))
              .project(table.schema())
              .createReaderFunc(
                  fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
              .build()) {
        Set<Record> newRecords = Sets.newHashSet(reader);
        assertThat(newRecords).hasSize((int) newDataFile.recordCount());
        writtenRecords.addAll(Sets.newHashSet(reader));
      }
    }

    assertThat(writtenRecords).isEqualTo(records);
    assertThat(writtenPartitions).isEqualTo(partitions);
  }

  private Set<Record> records(Schema schema, Set<List<Object>> data) {
    GenericRecord record = GenericRecord.create(schema);

    ImmutableSet.Builder<Record> builder = ImmutableSet.builder();
    data.forEach(
        recordData ->
            builder.add(
                record.copy(
                    ImmutableMap.of(
                        "id",
                        recordData.get(0),
                        "data",
                        recordData.get(1),
                        "spec",
                        recordData.get(2)))));

    return builder.build();
  }

  private void processGroup(
      KeyedOneInputStreamOperatorTestHarness<
              DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
          harness,
      Tuple4<Long, SerializableTable, Integer, RewriteFileGroup> group)
      throws Exception {
    boolean first = true;
    for (FileScanTask task : group.f3.fileScans()) {
      harness.processElement(
          new DataFileRewriteTask(
              group.f0, group.f1, group.f2, Long.MAX_VALUE, group.f3.info(), task, first),
          group.f0);
      first = false;
    }

    harness.processWatermark(group.f0);
  }
}
