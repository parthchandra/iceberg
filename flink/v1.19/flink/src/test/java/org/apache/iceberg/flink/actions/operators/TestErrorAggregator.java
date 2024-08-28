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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestErrorAggregator {
  private static final int ID = 0;
  private static final Exception TEST_EXCEPTION = new Exception("Test error");

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testResult(boolean success) throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long, Tuple2<Long, Long>, Tuple2<Long, Exception>, RunResponse>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, 1L), EVENT_TIME);
      if (!success) {
        testHarness.processElement2(Tuple2.of(EVENT_TIME, TEST_EXCEPTION), EVENT_TIME);
      }

      testHarness.setProcessingTime(10L);
      testHarness.processBothWatermarks(WATERMARK);

      RunResponse expected =
          new RunResponse(
              EVENT_TIME,
              ID,
              9L,
              success,
              success ? ImmutableList.of() : ImmutableList.of(TEST_EXCEPTION));

      assertThat(testHarness.extractOutputValues()).isEqualTo(ImmutableList.of(expected));
    }
  }

  static KeyedTwoInputStreamOperatorTestHarness<
          Long, Tuple2<Long, Long>, Tuple2<Long, Exception>, RunResponse>
      harness() throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new KeyedCoProcessOperator<>(new ErrorAggregator(ID)),
        value -> value.f0,
        value -> value.f0,
        Types.LONG);
  }
}
