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

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a rewrite for a single {@link RewriteFileGroup}. Reads the files with the standard
 * {@link FileScanTaskReader}, so the delete files are considered, and writes using the {@link
 * TaskWriterFactory}. The input is a {@link Tuple3} of:
 *
 * <ul>
 *   <li>Serializable table - the table which is compacted
 *   <li>Number of file groups to include in a single commit
 *   <li>A {@link RewriteFileGroup} where the {@link RewriteFileGroup#fileScans()} are set
 * </ul>
 *
 * The output is a {@link Tuple3} of:
 *
 * <ul>
 *   <li>Snapshot id of the compacted table
 *   <li>Number of file groups to include in a single commit
 *   <li>A {@link RewriteFileGroup} where the {@link RewriteFileGroup#addedFiles()} are already
 *       calculated, and the data files are written
 * </ul>
 *
 * If there is an error then an exception is thrown, so the job could be restarted.
 */
public class DataFileRewriteExecutor
    extends KeyedProcessFunction<
        DataFileRewriteTask.Key, DataFileRewriteTask, Tuple3<Long, Integer, RewriteFileGroup>>
    implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewriteExecutor.class);

  private final String name;

  private transient int subTaskId;
  private transient int attemptId;
  private transient Map<
          DataFileRewriteTask.Key,
          Tuple3<DataFileRewriteTask.Init, TaskWriter<RowData>, Set<FileScanTask>>>
      writers;
  private transient Set<DataFileRewriteTask.Key> aborted;
  private transient Counter errorCounter;
  private transient ValueState<Boolean> writing;

  public DataFileRewriteExecutor(String name) {
    Preconditions.checkNotNull(name, "Name should no be null");

    this.name = name;
  }

  @Override
  public void open(Configuration parameters) {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);

    this.writing =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("dataFileRewriteExecutorWriting", Types.BOOLEAN));

    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();

    this.writers = Maps.newHashMap();
    this.aborted = Sets.newHashSet();
  }

  @Override
  public void processElement(
      DataFileRewriteTask value,
      Context ctx,
      Collector<Tuple3<Long, Integer, RewriteFileGroup>> out)
      throws Exception {
    if (!aborted.contains(ctx.getCurrentKey())) {
      try {
        LOG.debug(
            "Reading {} for rewrite in group {}", value.getTask().file().path(), value.getKey());
        Tuple3<DataFileRewriteTask.Init, TaskWriter<RowData>, Set<FileScanTask>> writer =
            writer(value);
        writer.f2.add(value.getTask());

        RowDataFileScanTaskReader reader =
            new RowDataFileScanTaskReader(
                writer.f0.getTable().schema(),
                writer.f0.getTable().schema(),
                PropertyUtil.propertyAsString(
                    writer.f0.getTable().properties(), DEFAULT_NAME_MAPPING, null),
                false,
                Collections.emptyList());
        try (DataIterator<RowData> iterator =
            new DataIterator<>(
                reader,
                new BaseCombinedScanTask(value.getTask()),
                writer.f0.getTable().io(),
                writer.f0.getTable().encryption())) {
          while (iterator.hasNext()) {
            RowData rowData = iterator.next();
            writer.f1.write(rowData);
          }
        }
      } catch (Exception ex) {
        LOG.info(
            "Exception rewriting datafile group with value: {} at {}", value, ctx.timestamp(), ex);
        ctx.output(ErrorAggregator.ERROR_STREAM, ex);
        errorCounter.inc();
        abort(ctx.getCurrentKey());
      }
    }

    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(
      long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Integer, RewriteFileGroup>> out)
      throws Exception {
    try {
      if (!aborted.contains(ctx.getCurrentKey())) {
        Tuple3<DataFileRewriteTask.Init, TaskWriter<RowData>, Set<FileScanTask>> value =
            writers.get(ctx.getCurrentKey());
        RewriteDataFiles.FileGroupInfo info =
            ImmutableRewriteDataFiles.FileGroupInfo.builder()
                .globalIndex(ctx.getCurrentKey().getGlobalIndex())
                .partitionIndex(ctx.getCurrentKey().getPartitionIndex())
                .partition(value.f2.iterator().next().partition())
                .build();
        RewriteFileGroup rewriteFileGroup =
            new RewriteFileGroup(info, Lists.newArrayList(value.f2));
        rewriteFileGroup.setOutputFiles(Sets.newHashSet(value.f1.dataFiles()));
        out.collect(
            Tuple3.of(
                value.f0.getTable().currentSnapshot().snapshotId(),
                value.f0.getGroupsPerCommit(),
                rewriteFileGroup));
        LOG.debug(
            "Rewritten from {} to {}",
            rewriteFileGroup.rewrittenFiles(),
            rewriteFileGroup.addedFiles());
      } else {
        LOG.debug("Skipping rewrite for {}", ctx.getCurrentKey());
      }
    } catch (Exception e) {
      LOG.warn(
          "Exception writing datafile group {} at {}",
          ctx.getCurrentKey(),
          ctx.getCurrentKey().getTimestamp(),
          e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
      abort(ctx.getCurrentKey());
    }

    writers.remove(ctx.getCurrentKey());
    aborted.remove(ctx.getCurrentKey());
    writing.clear();
  }

  private Tuple3<DataFileRewriteTask.Init, TaskWriter<RowData>, Set<FileScanTask>> writer(
      DataFileRewriteTask value) {
    return writers.computeIfAbsent(
        value.getKey(),
        k -> {
          try {
            if (writing.value() != null) {
              aborted.add(k);
              writing.clear();
              LOG.info("Restoring state, pending writes are missing, so group {} is aborted", k);
            } else {
              writing.update(true);
              LOG.debug("Writer is created for {}", k);
            }
          } catch (Exception e) {
            throw new RuntimeException("Error accessing state", e);
          }

          String formatString =
              PropertyUtil.propertyAsString(
                  value.getInit().getTable().properties(),
                  TableProperties.DEFAULT_FILE_FORMAT,
                  TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
          RowDataTaskWriterFactory taskWriterFactory =
              new RowDataTaskWriterFactory(
                  value.getInit().getTable(),
                  FlinkSchemaUtil.convert(value.getInit().getTable().schema()),
                  value.getInit().getSplitSize(),
                  FileFormat.fromString(formatString),
                  value.getInit().getTable().properties(),
                  null,
                  false);
          taskWriterFactory.initialize(subTaskId, attemptId);

          return Tuple3.of(value.getInit(), taskWriterFactory.create(), Sets.newHashSet());
        });
  }

  private void abort(DataFileRewriteTask.Key key) {
    aborted.add(key);

    // Remove the generated files
    try {
      writers.get(key).f1.abort();
    } catch (Exception ex) {
      LOG.info("Exception cleaning up at {}", key, ex);
    }
  }
}
