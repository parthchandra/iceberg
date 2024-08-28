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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterSplitsByFileName
    extends KeyedCoProcessFunction<
        Long,
        Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
        Tuple2<Long, IcebergSourceSplit>,
        IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(FilterSplitsByFileName.class);

  private transient ListState<IcebergSourceSplit> splits;
  private transient ListState<String> allowedFileNames;

  @Override
  public void open(Configuration config) throws Exception {
    splits =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "FilterWrongSpecIdSplits", TypeInformation.of(IcebergSourceSplit.class)));
    allowedFileNames =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>("FilterWrongSpecIdAllowedFileNames", Types.STRING));
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<IcebergSourceSplit> out)
      throws Exception {
    try {
      Set<String> allowed = Sets.newHashSet(allowedFileNames.get().iterator());
      for (IcebergSourceSplit split : splits.get()) {
        Collection<FileScanTask> tasks = split.task().tasks();
        List<FileScanTask> filteredTasks = Lists.newArrayListWithExpectedSize(tasks.size());
        for (FileScanTask task : tasks) {
          if (allowed.contains(task.file().path().toString())) {
            filteredTasks.add(task);
          } else {
            LOG.debug(
                "Filtered out file {} as it is not in the allowed file list {} at {}",
                task.file().path(),
                allowed,
                ctx.timestamp());
          }
        }

        if (filteredTasks.size() < tasks.size()) {
          if (!filteredTasks.isEmpty()) {
            out.collect(
                IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(filteredTasks)));
          }
        } else {
          out.collect(split);
        }
      }
    } finally {
      splits.clear();
      allowedFileNames.clear();
    }

    super.onTimer(timestamp, ctx, out);
  }

  @Override
  public void processElement1(
      Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile> value,
      Context ctx,
      Collector<IcebergSourceSplit> out)
      throws Exception {
    allowedFileNames.add(value.f2.path());
    ctx.timerService().registerEventTimeTimer(value.f0);
  }

  @Override
  public void processElement2(
      Tuple2<Long, IcebergSourceSplit> value, Context ctx, Collector<IcebergSourceSplit> out)
      throws Exception {
    splits.add(value.f1);
    ctx.timerService().registerEventTimeTimer(value.f0);
  }
}
