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

import static org.apache.iceberg.flink.actions.ActionTestUtils.SCHEMA;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME_2;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_1;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_2;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.FILE_NAME_3;
import static org.apache.iceberg.flink.actions.operators.ManifestUpdater.ManifestSource.OLD;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.flink.actions.ManifestForTests;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestFilterSplitsByFileName {
  @Test
  void testFilter() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, IcebergSourceSplit>,
            IcebergSourceSplit>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_1)), EVENT_TIME);
      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_2)), EVENT_TIME);
      testHarness.processElement2(
          Tuple2.of(EVENT_TIME, createSplit(FILE_NAME_1, FILE_NAME_3)), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark1(new Watermark(EVENT_TIME));
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark2(new Watermark(EVENT_TIME));
      assertThat(testHarness.extractOutputValues()).hasSize(1);
      assertFileNamesInSplit(testHarness.extractOutputValues().get(0), FILE_NAME_1);
    }
  }

  @Test
  void testFilterNoChange() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, IcebergSourceSplit>,
            IcebergSourceSplit>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement2(Tuple2.of(EVENT_TIME, createSplit(FILE_NAME_1)), EVENT_TIME);
      testHarness.processElement2(
          Tuple2.of(EVENT_TIME, createSplit(FILE_NAME_1, FILE_NAME_2)), EVENT_TIME);
      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_1)), EVENT_TIME);
      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_2)), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark2(new Watermark(EVENT_TIME));
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark1(new Watermark(EVENT_TIME));
      assertThat(testHarness.extractOutputValues()).hasSize(2);
      assertFileNamesInSplit(testHarness.extractOutputValues().get(0), FILE_NAME_1);
      assertFileNamesInSplit(testHarness.extractOutputValues().get(1), FILE_NAME_1, FILE_NAME_2);
    }
  }

  @Test
  void testNoRecord() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, IcebergSourceSplit>,
            IcebergSourceSplit>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_1)), EVENT_TIME);
      testHarness.processElement2(Tuple2.of(EVENT_TIME, createSplit(FILE_NAME_3)), EVENT_TIME);
      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_2)), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark2(new Watermark(EVENT_TIME));
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark1(new Watermark(EVENT_TIME));

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testMixedData() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, IcebergSourceSplit>,
            IcebergSourceSplit>
        testHarness = testHarness()) {
      testHarness.open();

      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_1)), EVENT_TIME);
      testHarness.processElement1(
          Tuple3.of(EVENT_TIME, OLD, new ManifestForTests(FILE_NAME_2)), EVENT_TIME);
      testHarness.processElement1(
          Tuple3.of(EVENT_TIME_2, OLD, new ManifestForTests(FILE_NAME_3)), EVENT_TIME_2);
      testHarness.processElement2(
          Tuple2.of(EVENT_TIME, createSplit(FILE_NAME_2, FILE_NAME_3)), EVENT_TIME);
      testHarness.processElement2(
          Tuple2.of(EVENT_TIME_2, createSplit(FILE_NAME_2, FILE_NAME_3)), EVENT_TIME_2);

      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(new Watermark(EVENT_TIME));

      assertThat(testHarness.extractOutputValues()).hasSize(1);
      assertFileNamesInSplit(testHarness.extractOutputValues().get(0), FILE_NAME_2);

      testHarness.processBothWatermarks(new Watermark(EVENT_TIME_2));
      assertThat(testHarness.extractOutputValues()).hasSize(2);
      assertFileNamesInSplit(testHarness.extractOutputValues().get(1), FILE_NAME_3);
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<
          Long,
          Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
          Tuple2<Long, IcebergSourceSplit>,
          IcebergSourceSplit>
      testHarness() throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
        new FilterSplitsByFileName(),
        (KeySelector<Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>, Long>) t -> t.f0,
        (KeySelector<Tuple2<Long, IcebergSourceSplit>, Long>) t -> t.f0,
        Types.LONG);
  }

  private static void assertFileNamesInSplit(IcebergSourceSplit split, String... fileNames) {
    assertThat(
            split.task().files().stream()
                .map(f -> f.file().path().toString())
                .collect(Collectors.toList()))
        .isEqualTo(Arrays.stream(fileNames).collect(Collectors.toList()));
  }

  private static IcebergSourceSplit createSplit(String... fileNames) {
    CombinedScanTask combinedScanTask =
        new BaseCombinedScanTask(
            Arrays.stream(fileNames)
                .map(TestFilterSplitsByFileName::createTask)
                .collect(Collectors.toList()));
    return IcebergSourceSplit.fromCombinedScanTask(combinedScanTask);
  }

  private static FileScanTask createTask(String fileName) {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(5L)
            .withFileSizeInBytes(10L)
            .withPath(fileName)
            .withFormat(FileFormat.AVRO)
            .withMetrics(
                new Metrics(
                    5L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()))
            .build();

    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(Expressions.alwaysTrue());
    return new BaseFileScanTask(
        dataFile,
        null,
        SchemaParser.toJson(SCHEMA),
        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
        residuals);
  }
}
