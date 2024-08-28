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

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calls the {@link ExpireSnapshots} to remove the old snapshots and emits the filenames which could
 * be removed.
 */
public class ExpireSnapshotsProcessor extends ProcessFunction<SerializableTable, RunResponse> {
  public static final OutputTag<String> DELETE_STREAM =
      new OutputTag<>("delete-stream", Types.STRING);
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsProcessor.class);

  private final int id;
  private final String name;
  private final TableLoader tableLoader;
  private final Long minAgeMs;
  private final Integer retainLast;
  private final int plannerPoolSize;
  private transient ExecutorService plannerPool;
  private transient Table table;
  private transient Counter errorCounter;

  public ExpireSnapshotsProcessor(
      int id,
      String name,
      TableLoader tableLoader,
      Long minAgeMs,
      Integer retainLast,
      int plannerPoolSize) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.id = id;
    this.name = name;
    this.tableLoader = tableLoader;
    this.minAgeMs = minAgeMs;
    this.retainLast = retainLast;
    this.plannerPoolSize = plannerPoolSize;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);

    tableLoader.open();
    this.table = tableLoader.loadTable();
    this.plannerPool = ThreadPools.newWorkerPool(table.name() + "-table--planner", plannerPoolSize);
  }

  @Override
  public void processElement(SerializableTable notUsed, Context ctx, Collector<RunResponse> out)
      throws Exception {
    long start = ctx.timerService().currentProcessingTime();
    try {
      table.refresh();
      ExpireSnapshots expireSnapshots = table.expireSnapshots();
      if (minAgeMs != null) {
        expireSnapshots = expireSnapshots.expireOlderThan(ctx.timestamp() - minAgeMs);
      }

      if (retainLast != null) {
        expireSnapshots = expireSnapshots.retainLast(retainLast);
      }

      expireSnapshots
          .planWith(plannerPool)
          .deleteWith(file -> ctx.output(DELETE_STREAM, file))
          .cleanExpiredFiles(true)
          .commit();

      LOG.info("Successfully finished expiring snapshots for {} at {}", table, ctx.timestamp());
      long length = ctx.timerService().currentProcessingTime() - start;
      out.collect(new RunResponse(ctx.timestamp(), id, length, true, Collections.emptyList()));
    } catch (Exception e) {
      LOG.info("Exception expiring snapshots for {} at {}", table, ctx.timestamp(), e);
      long length = ctx.timerService().currentProcessingTime() - start;
      out.collect(new RunResponse(ctx.timestamp(), id, length, false, Lists.newArrayList(e)));
      errorCounter.inc();
    }
  }
}
