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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_1;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_2;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSkipOnError extends OperatorTestBase {
  @Test
  void testNoFailure() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long, Tuple2<Long, String>, Tuple2<Long, Exception>, String>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_2), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(FILE_NAME_1, FILE_NAME_2));
    }
  }

  @Test
  void testFailure() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long, Tuple2<Long, String>, Tuple2<Long, Exception>, String>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processElement2(Tuple2.of(EVENT_TIME, new Exception("Test error")), EVENT_TIME);
      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_2), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testWrongData() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long, Tuple2<Long, String>, Tuple2<Long, Exception>, String>
        testHarness = harness()) {
      testHarness.open();

      assertThatThrownBy(() -> testHarness.processElement2(Tuple2.of(EVENT_TIME, null), EVENT_TIME))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("Error could not be");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStateRestore(boolean withError) throws Exception {
    OperatorSubtaskState state;
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long, Tuple2<Long, String>, Tuple2<Long, Exception>, String>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      if (withError) {
        testHarness.processElement2(Tuple2.of(EVENT_TIME, new Exception("Test error")), EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      state = testHarness.snapshot(1L, EVENT_TIME);
    }

    try (KeyedTwoInputStreamOperatorTestHarness<
            Long, Tuple2<Long, String>, Tuple2<Long, Exception>, String>
        testHarness = harness()) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_2), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      if (withError) {
        assertThat(testHarness.extractOutputValues()).isEmpty();
      } else {
        assertThat(testHarness.extractOutputValues())
            .isEqualTo(ImmutableList.of(FILE_NAME_1, FILE_NAME_2));
      }
    }
  }

  private KeyedTwoInputStreamOperatorTestHarness<
          Long, Tuple2<Long, String>, Tuple2<Long, Exception>, String>
      harness() throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(new SkipOnError()),
        value -> value.f0,
        value -> value.f0,
        Types.LONG);
  }
}
