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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME_2;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_1;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_2;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.WATERMARK_2;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class TestAntiJoin {
  @Test
  void testFileSystemFirst() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processWatermark1(WATERMARK);
      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processWatermark2(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testTableFirst() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processWatermark1(WATERMARK);
      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processWatermark2(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testOnlyFileSystem() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(Tuple2.of(EVENT_TIME, FILE_NAME_1)));
    }
  }

  @Test
  void testOnlyTable() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement1(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testMixedWindows() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement2(Tuple2.of(EVENT_TIME, FILE_NAME_1), EVENT_TIME);
      // Notice that the event time is different
      testHarness.processElement1(Tuple2.of(EVENT_TIME_2, FILE_NAME_1), EVENT_TIME_2);
      testHarness.processWatermark1(WATERMARK);
      testHarness.processElement2(Tuple2.of(EVENT_TIME_2, FILE_NAME_2), EVENT_TIME_2);
      testHarness.processWatermark2(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(Tuple2.of(EVENT_TIME, FILE_NAME_1)));
      testHarness.processElement2(Tuple2.of(EVENT_TIME_2, FILE_NAME_1), EVENT_TIME_2);
      testHarness.processBothWatermarks(WATERMARK_2);
      // No new item for FILE_NAME_1, but one new item for FILE_NAME_2
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(
              ImmutableList.of(
                  Tuple2.of(EVENT_TIME, FILE_NAME_1), Tuple2.of(EVENT_TIME_2, FILE_NAME_2)));
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<
          Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>>
      testHarness() throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
        new AntiJoin(),
        (KeySelector<Tuple2<Long, String>, Tuple2<Long, String>>) t -> t,
        (KeySelector<Tuple2<Long, String>, Tuple2<Long, String>>) t -> t,
        TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}));
  }
}
