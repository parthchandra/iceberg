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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllManifests extends ProcessFunction<SerializableTable, ManifestFile> {
  private static final Logger LOG = LoggerFactory.getLogger(AllManifests.class);

  private final String name;
  private transient Counter errorCounter;

  public AllManifests(String name) {
    Preconditions.checkNotNull(name, "Name should no be null");
    this.name = name;
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
  public void processElement(SerializableTable value, Context ctx, Collector<ManifestFile> out)
      throws Exception {
    try {
      value.currentSnapshot().allManifests(value.io()).stream()
          .filter(m -> m.partitionSpecId() == value.spec().specId())
          .forEach(out::collect);
    } catch (Exception e) {
      LOG.info("Exception fetching manifests for {} at {}", value, ctx.timestamp(), e);
      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }
}
