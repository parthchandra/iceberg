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

import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestRateLimiter extends OperatorTestBase {
  @Test
  void testCommitNumber() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RateLimiter rateLimiter = new RateLimiter(tableLoader, 1L, 1L, false);
    List<Tuple2<Long, TableChange>> expected = Lists.newArrayListWithExpectedSize(2);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      testHarness.setProcessingTime(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      expected.add(Tuple2.of(EVENT_TIME, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
      finishTask(tableLoader);

      testHarness.processElement(withCommitNum(2), Long.MIN_VALUE);
      testHarness.processElement(withCommitNum(3), Long.MIN_VALUE);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.setProcessingTime(EVENT_TIME + 1);
      expected.add(Tuple2.of(EVENT_TIME + 1, withCommitNum(5)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStateRestore(boolean delayedFinish) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RateLimiter rateLimiter = new RateLimiter(tableLoader, 1L, 1L, false);
    List<Tuple2<Long, TableChange>> expected = Lists.newArrayListWithExpectedSize(2);
    OperatorSubtaskState state;
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      testHarness.setProcessingTime(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      expected.add(Tuple2.of(EVENT_TIME, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
      if (!delayedFinish) {
        finishTask(tableLoader);
      }

      testHarness.processElement(withCommitNum(2), Long.MIN_VALUE);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data
    rateLimiter = new RateLimiter(tableLoader, 1L, 1L, false);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement(withCommitNum(3), Long.MIN_VALUE);
      finishTask(tableLoader);

      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(Tuple2.of(EVENT_TIME + 1, null)));

      testHarness.setProcessingTime(EVENT_TIME + 2);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(
              ImmutableList.of(
                  Tuple2.of(EVENT_TIME + 1, null), Tuple2.of(EVENT_TIME + 2, withCommitNum(5))));
    }
  }

  @Test
  void testConcurrentDelaySameJob() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RateLimiter rateLimiter = new RateLimiter(tableLoader, 1L, 4L, false);
    List<Tuple2<Long, TableChange>> expected = Lists.newArrayListWithExpectedSize(2);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      testHarness.setProcessingTime(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      expected.add(Tuple2.of(EVENT_TIME, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.processElement(withCommitNum(2), Long.MIN_VALUE);
      testHarness.processElement(withCommitNum(3), Long.MIN_VALUE);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.setProcessingTime(EVENT_TIME + 1);
      // No finish task here as we simulate a concurrent trigger
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      finishTask(tableLoader);
      testHarness.setProcessingTime(EVENT_TIME + 5);
      expected.add(Tuple2.of(EVENT_TIME + 5, withCommitNum(5)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
    }
  }

  @Test
  void testConcurrentDelayDifferentJob() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RateLimiter rateLimiter = new RateLimiter(tableLoader, 1L, 4L, false);
    List<Tuple2<Long, TableChange>> expected = Lists.newArrayListWithExpectedSize(1);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      // The lock shouldn't be there when starting first time
      assertThat(checkLock(tableLoader)).isFalse();

      testHarness.setProcessingTime(EVENT_TIME);
      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      expected.add(Tuple2.of(EVENT_TIME, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
    }

    rateLimiter = new RateLimiter(tableLoader, 1L, 4L, false);
    expected = Lists.newArrayListWithExpectedSize(1);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      // The lock should be there after restore
      assertThat(checkLock(tableLoader)).isTrue();

      testHarness.setProcessingTime(EVENT_TIME + 1);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      testHarness.setProcessingTime(EVENT_TIME + 2);
      // Still no response as the lock is held by another task
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.setProcessingTime(EVENT_TIME + 6);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      finishTask(tableLoader);

      testHarness.setProcessingTime(EVENT_TIME + 10);
      expected.add(Tuple2.of(EVENT_TIME + 10, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
    }
  }

  @Test
  void testCleanupWithLockFromOtherJob() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RateLimiter rateLimiter = new RateLimiter(tableLoader, 1L, 4L, false);
    List<Tuple2<Long, TableChange>> expected = Lists.newArrayListWithExpectedSize(1);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      testHarness.setProcessingTime(EVENT_TIME);
      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      expected.add(Tuple2.of(EVENT_TIME, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
      // Task is not finished
    }

    rateLimiter = new RateLimiter(tableLoader, 1L, 4L, true);
    expected = Lists.newArrayListWithExpectedSize(1);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      testHarness.setProcessingTime(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      expected.add(Tuple2.of(EVENT_TIME, withCommitNum(1)));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
    }
  }

  @Test
  void testEmptyTable() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    RateLimiter rateLimiter = new RateLimiter(tableLoader, 1L, 1L, false);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>>
        testHarness = harness(rateLimiter)) {
      testHarness.open();

      testHarness.setProcessingTime(EVENT_TIME);
      testHarness.processElement(withCommitNum(1), Long.MIN_VALUE);
      assertThat(testHarness.extractOutputValues()).hasSize(1);
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          Boolean, TableChange, Tuple2<Long, TableChange>>
      harness(RateLimiter rateLimiter) throws Exception {
    KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Tuple2<Long, TableChange>> result =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(rateLimiter), unused -> true, Types.BOOLEAN, 1, 1, 0);
    result.setup(
        TypeInformation.of(new TypeHint<Tuple2<Long, TableChange>>() {})
            .createSerializer(new ExecutionConfig()));
    return result;
  }

  private static TableChange withCommitNum(int commitNum) {
    return new TableChange(0, 0, 0L, 0L, commitNum);
  }

  private static void finishTask(TableLoader tableLoader) {
    Table table = tableLoader.loadTable();
    Map<String, SnapshotRef> refs = table.refs();
    ManageSnapshots manage = table.manageSnapshots();
    List<String> keys =
        refs.keySet().stream()
            .filter(key -> key.equals(TagBasedLock.RUNNING_TAG))
            .collect(Collectors.toList());
    assertThat(keys).hasSize(1);
    manage.removeTag(keys.get(0));
    manage.commit();
  }

  private static boolean checkLock(TableLoader tableLoader) {
    return tableLoader.loadTable().refs().keySet().stream()
        .anyMatch(key -> key.equals(TagBasedLock.RUNNING_TAG));
  }
}
