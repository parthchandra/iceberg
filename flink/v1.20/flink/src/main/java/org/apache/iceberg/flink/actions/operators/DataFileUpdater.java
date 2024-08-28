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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager.CommitService;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits the compaction changes using {@link RewriteDataFilesCommitManager}. The input is a {@link
 * Tuple4} of:
 *
 * <ul>
 *   <li>Event time
 *   <li>Snapshot id of the compacted table
 *   <li>Number of file groups to include in a single commit
 *   <li>A {@link RewriteFileGroup} where the {@link RewriteFileGroup#addedFiles()} are already
 *       calculated, and the data files are written
 * </ul>
 *
 * Keyed by the event time, and the result is a single <code>true</code> which is emitted after the
 * watermark which signals, that data file compaction round has finished.
 */
public class DataFileUpdater
    extends KeyedProcessFunction<Long, Tuple4<Long, Long, Integer, RewriteFileGroup>, Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileUpdater.class);

  private final String name;
  private final TableLoader tableLoader;

  private transient Table table;
  private transient ListState<RewriteFileGroup> inProgress;
  private transient ValueState<Long> snapshotId;
  private transient Map<Long, CommitService> commitServices;
  private transient Counter errorCounter;
  private transient Counter addedDataFileNumCounter;
  private transient Counter addedDataFileSizeCounter;
  private transient Counter removedDataFileNumCounter;
  private transient Counter removedDataFileSizeCounter;
  private transient Counter removedPositionalDeleteFileNumCounter;
  private transient Counter removedPositionalDeleteFileSizeCounter;
  private transient Counter removedEqualityDeleteFileNumCounter;
  private transient Counter removedEqualityDeleteFileSizeCounter;

  public DataFileUpdater(String name, TableLoader tableLoader) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.name = name;
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(Configuration config) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);
    this.addedDataFileNumCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.ADDED_DATA_FILE_NUM_METRIC);
    this.addedDataFileSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.ADDED_DATA_FILE_SIZE_METRIC);
    this.removedDataFileNumCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_DATA_FILE_NUM_METRIC);
    this.removedDataFileSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_DATA_FILE_SIZE_METRIC);
    this.removedPositionalDeleteFileNumCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_POSITIONAL_FILE_NUM_METRIC);
    this.removedPositionalDeleteFileSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_POSITIONAL_FILE_SIZE_METRIC);
    this.removedEqualityDeleteFileNumCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_EQUALITY_FILE_NUM_METRIC);
    this.removedEqualityDeleteFileSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_EQUALITY_FILE_SIZE_METRIC);

    this.inProgress =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>("dataFileUpdaterInProgress", RewriteFileGroup.class));
    this.snapshotId =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("dataFileUpdaterSnapshotId", Types.LONG));

    tableLoader.open();
    this.table = tableLoader.loadTable();
    this.commitServices = Maps.newHashMapWithExpectedSize(1);
  }

  @Override
  public void processElement(
      Tuple4<Long, Long, Integer, RewriteFileGroup> value, Context ctx, Collector<Boolean> out)
      throws Exception {
    try {
      ctx.timerService().registerEventTimeTimer(value.f0);

      commitService(value.f0, value).offer(value.f3);
      snapshotId.update(value.f1);
      inProgress.add(value.f3);
    } catch (Exception e) {
      LOG.info("Exception processing element for {} at {}", table.name(), ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void onTimer(long ts, OnTimerContext ctx, Collector<Boolean> out) {
    try {
      CommitService service = commitService(ts, null);
      if (service != null) {
        service.close();
      }

      table.refresh();
      LOG.info(
          "Successfully completed data file compaction for {} to {} at {}",
          table.name(),
          table.currentSnapshot().snapshotId(),
          ctx.timestamp());
    } catch (Exception e) {
      LOG.info("Exception closing commit service for {} at {}", table.name(), ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }

    commitServices.remove(ts);
    inProgress.clear();
    snapshotId.clear();
  }

  private CommitService commitService(
      long ts, Tuple4<Long, Long, Integer, RewriteFileGroup> element) {
    return commitServices.computeIfAbsent(
        ts,
        unused -> {
          CommitService service = null;
          Long snapshotIdFromState;
          Iterable<RewriteFileGroup> inProgressFromState;
          try {
            snapshotIdFromState = snapshotId.value();
            inProgressFromState = inProgress.get();
          } catch (Exception e) {
            throw new RuntimeException("Error accessing state", e);
          }

          table.refresh();
          RewriteDataFilesCommitManager manager = null;
          if (element != null) {
            manager = new FlinkRewriteDataFilesCommitManager(table, element.f1);
            service = manager.service(element.f2);
          } else if (snapshotIdFromState != null) {
            manager = new FlinkRewriteDataFilesCommitManager(table, snapshotIdFromState);
            service = manager.service(Integer.MAX_VALUE);
          }

          if (service != null) {
            service.start();

            // If we restore for whatever reason then we try to commit the pending groups
            Set<RewriteFileGroup> fromState = Sets.newHashSet(inProgressFromState);
            if (fromState.size() > 0) {
              try {
                manager.commitFileGroups(Sets.newHashSet(inProgressFromState));
              } catch (Exception e) {
                LOG.info(
                    "Failed committing pending groups {}, so skipping.",
                    Sets.newHashSet(inProgressFromState),
                    e);
              }
            }
          }

          return service;
        });
  }

  private class FlinkRewriteDataFilesCommitManager extends RewriteDataFilesCommitManager {
    FlinkRewriteDataFilesCommitManager(Table table, long startingSnapshotId) {
      super(table, startingSnapshotId);
    }

    @Override
    public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
      super.commitFileGroups(fileGroups);
      LOG.debug("Committed {}", fileGroups);

      updateMetrics(fileGroups);

      try {
        List<RewriteFileGroup> remaining = Lists.newArrayList(inProgress.get());
        remaining.retainAll(fileGroups);
        LOG.debug("Remaining {}", remaining);
        inProgress.update(remaining);
      } catch (Exception e) {
        throw new RuntimeException("Error accessing state", e);
      }
    }

    private void updateMetrics(Set<RewriteFileGroup> fileGroups) {
      for (RewriteFileGroup fileGroup : fileGroups) {
        for (DataFile added : fileGroup.addedFiles()) {
          addedDataFileNumCounter.inc();
          addedDataFileSizeCounter.inc(added.fileSizeInBytes());
        }

        for (DataFile rewritten : fileGroup.rewrittenFiles()) {
          switch (rewritten.content()) {
            case DATA:
              removedDataFileNumCounter.inc();
              removedDataFileSizeCounter.inc(rewritten.fileSizeInBytes());
              break;
            case POSITION_DELETES:
              removedPositionalDeleteFileNumCounter.inc();
              removedPositionalDeleteFileSizeCounter.inc(rewritten.fileSizeInBytes());
              break;
            case EQUALITY_DELETES:
              removedEqualityDeleteFileNumCounter.inc();
              removedEqualityDeleteFileSizeCounter.inc(rewritten.fileSizeInBytes());
              break;
          }
        }
      }
    }
  }

  public static class EventTimeExtractor
      extends ProcessFunction<
          Tuple3<Long, Integer, RewriteFileGroup>, Tuple4<Long, Long, Integer, RewriteFileGroup>> {
    @Override
    public void processElement(
        Tuple3<Long, Integer, RewriteFileGroup> value,
        Context ctx,
        Collector<Tuple4<Long, Long, Integer, RewriteFileGroup>> out)
        throws Exception {
      out.collect(Tuple4.of(ctx.timestamp(), value.f0, value.f1, value.f2));
    }
  }

  public static class EventTimeKeySelector
      implements KeySelector<Tuple4<Long, Long, Integer, RewriteFileGroup>, Long> {
    @Override
    public Long getKey(Tuple4<Long, Long, Integer, RewriteFileGroup> value) throws Exception {
      return value.f0;
    }
  }
}
