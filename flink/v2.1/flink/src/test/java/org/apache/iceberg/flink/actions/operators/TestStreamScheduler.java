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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK_2;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class TestStreamScheduler {
  private long watermark = 0L;

  @Test
  void testCommitNumber() throws Exception {
    StreamScheduler<Boolean> scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().commitNumber(3).build(), 0, DUMMY_NAME);
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 0);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 2), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 3), 2);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 10), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 3);

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 4);
    }
  }

  @Test
  void testFileNumber() throws Exception {
    StreamScheduler<Boolean> scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().fileNumber(3).build(), 0, DUMMY_NAME);
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(1, 0, 0, 0, 0), 0);

      addEventAndCheckResult(testHarness, new TableChange(1, 1, 0, 0, 0), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 3, 0, 0, 0), 2);
      addEventAndCheckResult(testHarness, new TableChange(5, 7, 0, 0, 0), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(1, 0, 0, 0, 0), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 3);

      addEventAndCheckResult(testHarness, new TableChange(1, 0, 0, 0, 0), 4);
    }
  }

  @Test
  void testFileSize() throws Exception {
    StreamScheduler<Boolean> scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().fileSize(3).build(), 0, DUMMY_NAME);
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 0, 0), 0);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 1, 0), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 3, 0), 2);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 5, 7, 0), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 0, 0), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 1, 0), 3);

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 0, 0), 4);
    }
  }

  @Test
  void testDeleteFileNumber() throws Exception {
    StreamScheduler<Boolean> scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().deleteFileNumber(3).build(), 0, DUMMY_NAME);
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(3, 1, 0, 0, 0), 0);
      addEventAndCheckResult(testHarness, new TableChange(0, 2, 0, 0, 0), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 3, 0, 0, 0), 2);
      addEventAndCheckResult(testHarness, new TableChange(0, 10, 0, 0, 0), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 3);

      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 4);
    }
  }

  @Test
  void testTimeout() throws Exception {
    StreamScheduler<Boolean> scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().timeout(Duration.ofSeconds(1)).build(),
            0,
            DUMMY_NAME);
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.open();

      TableChange event = new TableChange(1, 0, 0, 0, 1);

      // Wait for one trigger
      testHarness.processElement1(event, 0L);
      testHarness.processBothWatermarks(new Watermark(0L));

      assertThat(triggeredCount(testHarness.extractOutputValues())).isZero();

      // Wait for a second trigger
      testHarness.processElement1(event, 1001L);
      testHarness.processBothWatermarks(new Watermark(1001L));

      assertThat(triggeredCount(testHarness.extractOutputValues())).isEqualTo(1);
    }
  }

  @Test
  void testStateRestore() throws Exception {
    StreamScheduler<Boolean> scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().commitNumber(2).build(), 0, DUMMY_NAME);
    OperatorSubtaskState state;
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.open();

      testHarness.processElement1(new TableChange(1, 0, 0, 0, 1), EVENT_TIME);
      testHarness.processBothWatermarks(WATERMARK);

      assertThat(triggeredCount(testHarness.extractOutputValues())).isZero();

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data which is written
    scheduler =
        new StreamScheduler<>(
            new StreamSchedulerTrigger.Builder().commitNumber(2).build(), 0, DUMMY_NAME);
    try (KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
        testHarness = harness(scheduler)) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement1(null, EVENT_TIME);
      // First trigger is an empty one after restore
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(new ScheduleRequest(EVENT_TIME, 0, DUMMY_NAME, false)));

      // Arrives the first real change
      testHarness.processElement1(new TableChange(0, 0, 0, 0, 1), EVENT_TIME_2);
      testHarness.processBothWatermarks(WATERMARK_2);
      assertThat(triggeredCount(testHarness.extractOutputValues())).isEqualTo(1);
    }
  }

  private KeyedTwoInputStreamOperatorTestHarness<Boolean, TableChange, Boolean, ScheduleRequest>
      harness(StreamScheduler<Boolean> scheduler) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(scheduler),
        unused -> true,
        unused -> true,
        Types.BOOLEAN,
        1,
        1,
        0);
  }

  private void addEventAndCheckResult(
      TwoInputStreamOperatorTestHarness<TableChange, Boolean, ScheduleRequest> testHarness,
      TableChange event,
      int expectedSize)
      throws Exception {
    ++watermark;
    List<ScheduleRequest> previous = testHarness.extractOutputValues();

    testHarness.processElement1(event, watermark);
    assertThat(testHarness.extractOutputValues()).isEqualTo(previous);

    testHarness.processBothWatermarks(new Watermark(watermark));
    assertThat(triggeredCount(testHarness.extractOutputValues())).isEqualTo(expectedSize);
  }

  private static long triggeredCount(List<ScheduleRequest> output) {
    return output.stream().filter(ScheduleRequest::triggered).count();
  }
}
