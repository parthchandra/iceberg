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

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.actions.PartitionAwareHiddenPathFilter;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recursively lists the files in the `location` directory. Hidden files, and files younger than the
 * `minAgeMs` are omitted in the result.
 */
public class ListFileSystemFiles extends ProcessFunction<Long, String> {
  private static final Logger LOG = LoggerFactory.getLogger(ListFileSystemFiles.class);

  private final String name;
  private final SupportsPrefixOperations io;
  private final Map<Integer, PartitionSpec> specs;
  private final String location;
  private final long minAgeMs;
  private transient Counter errorCounter;

  public ListFileSystemFiles(
      String name,
      SupportsPrefixOperations io,
      String location,
      Map<Integer, PartitionSpec> specs,
      long minAgeMs) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(io, "FileIO should no be null");
    Preconditions.checkNotNull(location, "Location should no be null");
    Preconditions.checkNotNull(specs, "Specifications should no be null");

    this.name = name;
    this.io = io;
    this.location = location;
    this.specs = specs;
    this.minAgeMs = minAgeMs;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);
  }

  @Override
  public void processElement(Long runTimeMs, Context ctx, Collector<String> out) throws Exception {
    long olderThanTimestamp = runTimeMs - minAgeMs;
    try {
      PathFilter filter = PartitionAwareHiddenPathFilter.forSpecs(specs);
      io.listPrefix(location)
          .forEach(
              file -> {
                if (filter.accept(new org.apache.hadoop.fs.Path(file.location()))
                    && file.createdAtMillis() <= olderThanTimestamp) {
                  out.collect(file.location());
                }
              });
    } catch (Exception e) {
      LOG.info("Exception listing files for {} at {}", location, ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  public static class EventTimeExtractor extends ProcessFunction<SerializableTable, Long> {
    @Override
    public void processElement(SerializableTable value, Context ctx, Collector<Long> out)
        throws Exception {
      out.collect(ctx.timestamp());
    }
  }
}
