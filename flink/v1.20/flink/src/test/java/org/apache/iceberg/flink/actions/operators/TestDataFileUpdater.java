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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.apache.iceberg.flink.actions.operators.DataFileUpdater.EventTimeKeySelector;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.executeRewrite;
import static org.apache.iceberg.flink.actions.operators.RewriteUtil.planDataFileRewrite;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDataFileUpdater extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(3);
    List<Tuple3<Long, Integer, RewriteFileGroup>> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(1);

    DataFileUpdater dataFileUpdater = new DataFileUpdater(DUMMY_NAME, tableLoader);

    try (KeyedOneInputStreamOperatorTestHarness<
            Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
        testHarness = harnessForUpdater(dataFileUpdater)) {
      testHarness.open();

      testHarness.processElement(addTimestamp(rewritten.get(0), EVENT_TIME), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    assertDataFiles(table, rewritten.get(0).f2.addedFiles(), rewritten.get(0).f2.rewrittenFiles());
  }

  @Test
  void testPartitioned() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p2')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(4);
    List<Tuple3<Long, Integer, RewriteFileGroup>> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(2);
    assertThat(rewritten.get(0).f1).isEqualTo(1);
    assertThat(rewritten.get(1).f1).isEqualTo(1);

    try (KeyedOneInputStreamOperatorTestHarness<
            Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
        testHarness = harnessForUpdater(new DataFileUpdater(DUMMY_NAME, tableLoader))) {
      testHarness.open();

      testHarness.processElement(addTimestamp(rewritten.get(0), EVENT_TIME), EVENT_TIME);
      // This should be committed synchronously
      assertDataFiles(
          table, rewritten.get(0).f2.addedFiles(), rewritten.get(0).f2.rewrittenFiles());

      testHarness.processElement(addTimestamp(rewritten.get(1), EVENT_TIME), EVENT_TIME);
      // This should be committed synchronously
      assertDataFiles(
          table, rewritten.get(1).f2.addedFiles(), rewritten.get(1).f2.rewrittenFiles());

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testBatchSize() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (5, 'e', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (6, 'f', 'p3')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(6);
    List<Tuple3<Long, Integer, RewriteFileGroup>> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(3);

    try (KeyedOneInputStreamOperatorTestHarness<
            Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
        testHarness = harnessForUpdater(new DataFileUpdater(DUMMY_NAME, tableLoader))) {
      testHarness.open();

      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(0), EVENT_TIME, 2), EVENT_TIME);
      assertNoChange(table);
      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(1), EVENT_TIME, 2), EVENT_TIME);
      // This should be committed synchronously
      Set<DataFile> added = Sets.newHashSet(rewritten.get(0).f2.addedFiles());
      added.addAll(rewritten.get(1).f2.addedFiles());
      Set<DataFile> removed = Sets.newHashSet(rewritten.get(0).f2.rewrittenFiles());
      removed.addAll(rewritten.get(1).f2.rewrittenFiles());
      assertDataFiles(table, added, removed);

      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(2), EVENT_TIME, 2), EVENT_TIME);
      assertNoChange(table);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    // This should be committed on close
    assertDataFiles(table, rewritten.get(2).f2.addedFiles(), rewritten.get(2).f2.rewrittenFiles());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStateRestore(boolean withException) throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, spec varchar) PARTITIONED BY (spec)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (4, 'd', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (5, 'e', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (6, 'f', 'p3')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (7, 'g', 'p4')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (8, 'h', 'p4')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(8);
    List<Tuple3<Long, Integer, RewriteFileGroup>> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(4);

    OperatorSubtaskState state = null;
    try (KeyedOneInputStreamOperatorTestHarness<
            Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
        testHarness = harnessForUpdater(new DataFileUpdater(DUMMY_NAME, tableLoader))) {
      testHarness.open();

      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(0), EVENT_TIME, 2), EVENT_TIME);
      assertNoChange(table);

      state = testHarness.snapshot(1, System.currentTimeMillis());
      if (withException) {
        throw new RuntimeException("Testing exception");
      }
    } catch (Exception e) {
      // do nothing
    }

    if (withException) {
      // Simulate that commit was successful, but not yet updated the state
      RewriteDataFilesCommitManager manager =
          new RewriteDataFilesCommitManager(table, table.currentSnapshot().snapshotId());
      manager.commitFileGroups(ImmutableSet.of(rewritten.get(0).f2));

      assertDataFiles(
          table, rewritten.get(0).f2.addedFiles(), rewritten.get(0).f2.rewrittenFiles());
    }

    try (KeyedOneInputStreamOperatorTestHarness<
            Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
        testHarness = harnessForUpdater(new DataFileUpdater(DUMMY_NAME, tableLoader))) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(1), EVENT_TIME, 2), EVENT_TIME);

      assertDataFiles(
          table, rewritten.get(0).f2.addedFiles(), rewritten.get(0).f2.rewrittenFiles());
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertNoChange(table);

      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(2), EVENT_TIME, 2), EVENT_TIME);

      // This should be committed synchronously
      Set<DataFile> added = Sets.newHashSet(rewritten.get(1).f2.addedFiles());
      added.addAll(rewritten.get(2).f2.addedFiles());
      Set<DataFile> removed = Sets.newHashSet(rewritten.get(1).f2.rewrittenFiles());
      removed.addAll(rewritten.get(2).f2.rewrittenFiles());
      assertDataFiles(table, added, removed);

      testHarness.processElement(
          addTimestampAndUpdateBatchSize(rewritten.get(3), EVENT_TIME, 2), EVENT_TIME);
      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    // This should be committed on close
    assertDataFiles(table, rewritten.get(3).f2.addedFiles(), rewritten.get(3).f2.rewrittenFiles());
  }

  @Test
  void testError() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p3')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<DataFileRewriteTask> planned = planDataFileRewrite(table);
    assertThat(planned).hasSize(3);
    List<Tuple3<Long, Integer, RewriteFileGroup>> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(1);

    DataFileUpdater dataFileUpdater = new DataFileUpdater(DUMMY_NAME, tableLoader);

    try (KeyedOneInputStreamOperatorTestHarness<
            Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
        testHarness = harnessForUpdater(dataFileUpdater)) {
      testHarness.open();

      // Cause an exception
      sql.exec("DROP TABLE IF EXISTS %s", TABLE_NAME);

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(addTimestamp(rewritten.get(0), EVENT_TIME), EVENT_TIME);
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(ErrorAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("Metadata file for version");
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean>
      harnessForUpdater(DataFileUpdater dataFileUpdater) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(dataFileUpdater),
        new EventTimeKeySelector(),
        Types.LONG,
        1,
        1,
        0);
  }

  private static Tuple4<Long, Long, Integer, RewriteFileGroup> addTimestamp(
      Tuple3<Long, Integer, RewriteFileGroup> from, long eventTime) {
    return Tuple4.of(eventTime, from.f0, from.f1, from.f2);
  }

  private static Tuple4<Long, Long, Integer, RewriteFileGroup> addTimestampAndUpdateBatchSize(
      Tuple3<Long, Integer, RewriteFileGroup> from, long eventTime, int batchSize) {
    return Tuple4.of(eventTime, from.f0, batchSize, from.f2);
  }

  private static void assertDataFiles(
      Table actual, Set<DataFile> expectedAdded, Set<DataFile> expectedRemoved) {
    actual.refresh();

    Set<DataFile> actualAdded =
        Sets.newHashSet(actual.currentSnapshot().addedDataFiles(actual.io()));
    Set<DataFile> actualRemoved =
        Sets.newHashSet(actual.currentSnapshot().removedDataFiles(actual.io()));
    assertThat(actualAdded.stream().map(DataFile::path).collect(Collectors.toSet()))
        .isEqualTo(expectedAdded.stream().map(DataFile::path).collect(Collectors.toSet()));
    assertThat(actualRemoved.stream().map(DataFile::path).collect(Collectors.toSet()))
        .isEqualTo(expectedRemoved.stream().map(DataFile::path).collect(Collectors.toSet()));
  }

  private static void assertNoChange(Table table) {
    long original = table.currentSnapshot().snapshotId();
    table.refresh();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(original);
  }
}
