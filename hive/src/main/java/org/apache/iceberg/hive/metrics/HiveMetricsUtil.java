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

package org.apache.iceberg.hive.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class HiveMetricsUtil {

  private HiveMetricsUtil() {
  }

  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public static MetricRegistry metricRegistry() {
    return metricRegistry;
  }

  public static Timer timer(String metric) {
    return metricRegistry.timer(metric);
  }

  public static HiveMetaStoreClient newMetaStoreClientWithMeterIfConfigured(HiveConf conf) throws MetaException {
    Preconditions.checkArgument(conf != null, "Configuration is null");

    if (conf.getBoolean("iceberg.dropwizard.enable-metrics-collection", false)) {
      return new MeteredHiveMetaStoreClient(metricRegistry, conf);
    } else {
      return new HiveMetaStoreClient(conf);
    }
  }
}
