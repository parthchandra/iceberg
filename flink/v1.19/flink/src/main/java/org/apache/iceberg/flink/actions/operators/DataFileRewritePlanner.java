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

import java.math.RoundingMode;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.RewriteFileGroupPlanner;
import org.apache.iceberg.actions.SizeBasedDataRewriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans the rewrite groups using the {@link RewriteFileGroupPlanner}. The input is the {@link
 * SerializableTable} which is compacted, the output is a {@link Tuple3} of:
 *
 * <ul>
 *   <li>Serializable table - the table which is compacted
 *   <li>Number of file groups to include in a single commit
 *   <li>A {@link RewriteFileGroup} where the {@link RewriteFileGroup#fileScans()} are set
 * </ul>
 */
public class DataFileRewritePlanner
    extends ProcessFunction<SerializableTable, DataFileRewriteTask> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewritePlanner.class);

  private final String name;
  private final SerializableTable table;
  private final Map<String, String> rewriterOptions;
  private final int partialProgressMaxCommits;
  private final long maxRewriteBytes;
  private transient SizeBasedDataRewriter rewriter;
  private transient RewriteFileGroupPlanner planner;
  private transient Counter errorCounter;

  public DataFileRewritePlanner(
      String name,
      SerializableTable table,
      Map<String, String> rewriterOptions,
      int newPartialProgressMaxCommits,
      long maxRewriteBytes) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(table, "Table should no be null");
    Preconditions.checkNotNull(rewriterOptions, "Options map should no be null");

    this.name = name;
    this.table = table;
    this.rewriterOptions = rewriterOptions;
    this.partialProgressMaxCommits = newPartialProgressMaxCommits;
    this.maxRewriteBytes = maxRewriteBytes;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);

    this.rewriter =
        new SizeBasedDataRewriter(table) {
          @Override
          public Set<DataFile> rewrite(List<FileScanTask> group) {
            // We use the rewriter only for bin-packing the file groups to compact
            throw new UnsupportedOperationException("Should not be called");
          }
        };

    rewriter.init(rewriterOptions);
    this.planner = new RewriteFileGroupPlanner(rewriter, RewriteJobOrder.NONE);
  }

  @Override
  public void processElement(
      SerializableTable value, Context ctx, Collector<DataFileRewriteTask> out) throws Exception {
    LOG.debug("Creating rewrite plan for {} at {}", value.name(), ctx.timestamp());
    try {
      if (value.currentSnapshot() == null) {
        LOG.info("Nothing to plan for in an empty table: {} at {}", value.name(), ctx.timestamp());
        return;
      }

      RewriteFileGroupPlanner.PlanResult plan =
          planner.plan(value, Expressions.alwaysTrue(), value.currentSnapshot().snapshotId());

      long rewriteBytes = 0;
      int maxFileScanListLength = 0;
      List<RewriteFileGroup> groups = plan.fileGroups();
      ListIterator<RewriteFileGroup> iter = groups.listIterator();
      while (iter.hasNext()) {
        RewriteFileGroup group = iter.next();
        if (rewriteBytes + group.sizeInBytes() > maxRewriteBytes) {
          // Keep going, maybe some other group might fit in
          LOG.info(
              "Skipping group {} as max rewrite size reached for {} at {}",
              group,
              value.name(),
              ctx.timestamp());
          iter.remove();
        } else {
          rewriteBytes += group.sizeInBytes();
          maxFileScanListLength = Math.max(maxFileScanListLength, group.fileScans().size());
        }
      }

      int groupsPerCommit =
          IntMath.divide(groups.size(), partialProgressMaxCommits, RoundingMode.CEILING);

      LOG.info(
          "Rewrite plan created {} for {} at {}", plan.fileGroups(), value.name(), ctx.timestamp());

      for (int pos = 0; pos < maxFileScanListLength; ++pos) {
        for (RewriteFileGroup group : groups) {
          List<FileScanTask> scans = group.fileScans();
          long splitSize = rewriter.splitSize(group.sizeInBytes());
          if (scans.size() > pos) {
            DataFileRewriteTask toEmit =
                new DataFileRewriteTask(
                    ctx.timestamp(),
                    value,
                    groupsPerCommit,
                    splitSize,
                    group.info(),
                    scans.get(pos),
                    pos == 0);
            LOG.debug("Emitting {} with for {} at {}", toEmit, value.name(), ctx.timestamp());
            out.collect(toEmit);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "Exception planning data file rewrite groups for {} at {}",
          value.name(),
          ctx.timestamp(),
          e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }
}
