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

import java.util.concurrent.ExecutorService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Plans the splits to read a metadata table content. */
public class TablePlanner extends ProcessFunction<SerializableTable, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(TablePlanner.class);

  private final String name;
  private final SerializableTable serializableTable;
  private final int workerPoolSize;
  private final ScanContext scanContext;
  private transient ExecutorService workerPool;
  private transient Counter errorCounter;

  public TablePlanner(String name, SerializableTable serializableTable, int workerPoolSize) {
    this(name, serializableTable, ScanContext.builder().streaming(true).build(), workerPoolSize);
  }

  public TablePlanner(
      String name,
      SerializableTable serializableTable,
      ScanContext scanContext,
      int workerPoolSize) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(serializableTable, "Table should no be null");
    Preconditions.checkArgument(scanContext.isStreaming(), "Streaming should be set to true");

    this.name = name;
    this.serializableTable = serializableTable;
    this.scanContext = scanContext;
    this.workerPoolSize = workerPoolSize;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);

    this.workerPool =
        ThreadPools.newWorkerPool(serializableTable.name() + "-table--planner", workerPoolSize);
  }

  @Override
  public void processElement(
      SerializableTable table, Context ctx, Collector<IcebergSourceSplit> out) throws Exception {
    try {
      FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool)
          .forEach(out::collect);
    } catch (Exception e) {
      LOG.info("Exception planning scan for {} at {}", table, ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }
}
