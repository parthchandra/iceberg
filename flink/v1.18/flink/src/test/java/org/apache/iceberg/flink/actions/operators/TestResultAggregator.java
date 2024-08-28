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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK;
import static org.apache.iceberg.flink.actions.operators.TagBasedLock.RUNNING_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.streams.StreamResult;
import org.apache.iceberg.flink.actions.streams.TriggerResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class TestResultAggregator extends OperatorTestBase {
  private static final String STREAM_1 = "stream1";
  private static final String STREAM_2 = "stream2";
  private static final Map<Integer, String> STREAMS = ImmutableMap.of(1, STREAM_1, 2, STREAM_2);

  @Test
  void testSuccess() throws Exception {
    checkResult(
        ImmutableList.of(
            new ScheduleRequest(EVENT_TIME, 1, STREAM_1, true),
            new ScheduleRequest(EVENT_TIME, 2, STREAM_2, true)),
        ImmutableList.of(
            new RunResponse(EVENT_TIME, 1, 10L, true, Collections.emptyList()),
            new RunResponse(EVENT_TIME, 2, 20L, true, Collections.emptyList())),
        new TriggerResult(
            EVENT_TIME,
            true,
            30L,
            ImmutableList.of(
                new StreamResult(1, STREAM_1, true, true, 10L, Collections.emptyList()),
                new StreamResult(2, STREAM_2, true, true, 20L, Collections.emptyList()))));
  }

  @Test
  void testFailure() throws Exception {
    Exception exception = new RuntimeException("Test Exception");
    checkResult(
        ImmutableList.of(
            new ScheduleRequest(EVENT_TIME, 1, STREAM_1, true),
            new ScheduleRequest(EVENT_TIME, 2, STREAM_2, true)),
        ImmutableList.of(
            new RunResponse(EVENT_TIME, 1, 10L, true, Collections.emptyList()),
            new RunResponse(EVENT_TIME, 2, 20L, false, ImmutableList.of(exception))),
        new TriggerResult(
            EVENT_TIME,
            false,
            30L,
            ImmutableList.of(
                new StreamResult(1, STREAM_1, true, true, 10L, Collections.emptyList()),
                new StreamResult(2, STREAM_2, true, false, 20L, ImmutableList.of(exception)))));
  }

  @Test
  void testUnscheduled() throws Exception {
    checkResult(
        ImmutableList.of(
            new ScheduleRequest(EVENT_TIME, 1, STREAM_1, true),
            new ScheduleRequest(EVENT_TIME, 2, STREAM_2, false)),
        ImmutableList.of(new RunResponse(EVENT_TIME, 1, 10, true, Collections.emptyList())),
        new TriggerResult(
            EVENT_TIME,
            true,
            10L,
            ImmutableList.of(
                new StreamResult(1, STREAM_1, true, true, 10L, Collections.emptyList()),
                new StreamResult(2, STREAM_2, false, false, 0L, Collections.emptyList()))));
  }

  @Test
  void testMissingSchedule() {
    assertThatThrownBy(
            () ->
                checkResult(
                    ImmutableList.of(),
                    ImmutableList.of(
                        new RunResponse(EVENT_TIME, 1, 10, true, Collections.emptyList())),
                    new TriggerResult(EVENT_TIME, true, 10L, ImmutableList.of())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Could not found scheduler for run");
  }

  @Test
  void testMissingRun() {
    assertThatThrownBy(
            () ->
                checkResult(
                    ImmutableList.of(new ScheduleRequest(EVENT_TIME, 1, STREAM_1, true)),
                    ImmutableList.of(),
                    new TriggerResult(EVENT_TIME, true, 0L, ImmutableList.of())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Inconsistent count in scheduler");
  }

  @Test
  void testStateRestore() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    OperatorSubtaskState state;
    try (KeyedTwoInputStreamOperatorTestHarness<Long, ScheduleRequest, RunResponse, TriggerResult>
        testHarness = harness(new ResultAggregator(tableLoader, STREAMS))) {
      testHarness.open();

      testHarness.processElement1(new ScheduleRequest(EVENT_TIME, 1, STREAM_1, true), EVENT_TIME);
      testHarness.processElement2(
          new RunResponse(EVENT_TIME, 2, 20L, true, Collections.emptyList()), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      state = testHarness.snapshot(1, EVENT_TIME);

      // Create a lock manually
      createLock(table);
    }

    // Restore the state, write some more data, check the result
    try (KeyedTwoInputStreamOperatorTestHarness<Long, ScheduleRequest, RunResponse, TriggerResult>
        testHarness = harness(new ResultAggregator(tableLoader, STREAMS))) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement1(new ScheduleRequest(EVENT_TIME, 2, STREAM_2, true), EVENT_TIME);
      testHarness.processElement2(
          new RunResponse(EVENT_TIME, 1, 10L, true, Collections.emptyList()), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(
              ImmutableList.of(
                  new TriggerResult(
                      EVENT_TIME,
                      true,
                      30L,
                      ImmutableList.of(
                          new StreamResult(1, STREAM_1, true, true, 10L, Collections.emptyList()),
                          new StreamResult(
                              2, STREAM_2, true, true, 20L, Collections.emptyList())))));

      // Check that the lock is removed
      table.refresh();
      assertThat(table.refs().keySet()).isEqualTo(ImmutableSet.of("main"));
    }
  }

  @Test
  void testLocking() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    try (KeyedTwoInputStreamOperatorTestHarness<Long, ScheduleRequest, RunResponse, TriggerResult>
        testHarness = harness(new ResultAggregator(tableLoader, STREAMS))) {
      testHarness.open();

      // Create a lock manually
      createLock(table);

      testHarness.processElement1(new ScheduleRequest(EVENT_TIME, 1, STREAM_1, true), EVENT_TIME);
      testHarness.processElement2(
          new RunResponse(EVENT_TIME, 1, 10L, true, Collections.emptyList()), EVENT_TIME);
      testHarness.processBothWatermarks(WATERMARK);

      // Check that the lock is removed
      table.refresh();
      assertThat(testHarness.extractOutputValues()).hasSize(1);
      assertThat(table.refs().keySet()).isEqualTo(ImmutableSet.of("main"));
    }
  }

  private void createLock(Table table) {
    table.manageSnapshots().createTag(RUNNING_TAG, table.currentSnapshot().snapshotId()).commit();
    assertThat(table.refs()).hasSize(2);
  }

  private void checkResult(
      List<ScheduleRequest> schedules, List<RunResponse> responses, TriggerResult expected)
      throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    try (KeyedTwoInputStreamOperatorTestHarness<Long, ScheduleRequest, RunResponse, TriggerResult>
        testHarness =
            harness(new ResultAggregator(tableLoader, ImmutableMap.of(1, "name1", 2, "name2")))) {
      testHarness.open();

      for (ScheduleRequest schedule : schedules) {
        testHarness.processElement1(schedule, EVENT_TIME);
      }

      for (RunResponse response : responses) {
        testHarness.processElement2(response, EVENT_TIME);
      }

      testHarness.processBothWatermarks(WATERMARK);

      assertThat(testHarness.extractOutputValues()).isEqualTo(ImmutableList.of(expected));
    }
  }

  static KeyedTwoInputStreamOperatorTestHarness<Long, ScheduleRequest, RunResponse, TriggerResult>
      harness(ResultAggregator resultAggregator) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(resultAggregator),
        ScheduleRequest::timestamp,
        RunResponse::timestamp,
        Types.LONG);
  }
}
