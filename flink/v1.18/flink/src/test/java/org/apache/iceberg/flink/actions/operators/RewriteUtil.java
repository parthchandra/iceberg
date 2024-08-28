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

import static org.apache.iceberg.actions.SizeBasedFileRewriter.MIN_INPUT_FILES;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.DUMMY_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

class RewriteUtil {
  private RewriteUtil() {
    // Do not instantiate
  }

  static List<DataFileRewriteTask> planDataFileRewrite(Table table) throws Exception {
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    try (OneInputStreamOperatorTestHarness<SerializableTable, DataFileRewriteTask> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new DataFileRewritePlanner(
                DUMMY_NAME,
                serializableTable,
                ImmutableMap.of(MIN_INPUT_FILES, "2"),
                11,
                10_000_000))) {
      testHarness.open();

      testHarness.processElement(serializableTable, System.currentTimeMillis());

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static KeyedOneInputStreamOperatorTestHarness<
          DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
      rewriteHarness() throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedProcessFunction(
        new DataFileRewriteExecutor(DUMMY_NAME),
        DataFileRewriteTask::getKey,
        TypeInformation.of(DataFileRewriteTask.Key.class));
  }

  static List<Tuple3<Long, Integer, RewriteFileGroup>> executeRewrite(
      List<DataFileRewriteTask> elements) throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<
            DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
        testHarness = rewriteHarness()) {
      testHarness.open();

      for (DataFileRewriteTask element : elements) {
        testHarness.processElement(element, element.getKey().getTimestamp());
      }

      testHarness.processWatermark(elements.get(0).getKey().getTimestamp());

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static Set<DataFile> newDataFiles(Table table) {
    table.refresh();
    return Sets.newHashSet(table.currentSnapshot().addedDataFiles(table.io()));
  }

  static List<Tuple4<Long, SerializableTable, Integer, RewriteFileGroup>> reconstructFileGroups(
      List<DataFileRewriteTask> tasks) {
    Map<DataFileRewriteTask.Key, List<FileScanTask>> taskMap = Maps.newHashMap();
    Map<DataFileRewriteTask.Key, DataFileRewriteTask.Init> initMap = Maps.newHashMap();
    tasks.forEach(
        task -> {
          List<FileScanTask> group =
              taskMap.computeIfAbsent(task.getKey(), unused -> Lists.newArrayList());
          group.add(task.getTask());

          if (initMap.get(task.getKey()) != null) {
            // If not the first, the init is empty
            assertThat(task.getInit()).isNull();
          } else {
            // This is the first
            assertThat(taskMap.get(task.getKey())).hasSize(1);
            initMap.put(task.getKey(), task.getInit());
          }
        });

    return taskMap.entrySet().stream()
        .map(
            entry -> {
              RewriteDataFiles.FileGroupInfo info =
                  ImmutableRewriteDataFiles.FileGroupInfo.builder()
                      .globalIndex(entry.getKey().getGlobalIndex())
                      .partitionIndex(entry.getKey().getPartitionIndex())
                      .partition(entry.getKey().getPartition())
                      .build();

              DataFileRewriteTask.Init init = initMap.get(entry.getKey());
              return Tuple4.of(
                  entry.getKey().getTimestamp(),
                  init.getTable(),
                  init.getGroupsPerCommit(),
                  new RewriteFileGroup(info, Lists.newArrayList(entry.getValue())));
            })
        .collect(Collectors.toList());
  }
}
