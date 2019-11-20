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
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

public class MeteredHiveMetaStoreClient extends HiveMetaStoreClient {

  private final MetricRegistry metricRegistry;

  private static final String HIVE_LOCK_TIME = "hive.table.lock.time";
  private static final String HIVE_UNLOCK_TIME = "hive.table.unlock.time";

  public MeteredHiveMetaStoreClient(MetricRegistry metricRegistry, HiveConf conf) throws MetaException {
    super(conf);
    this.metricRegistry = metricRegistry;
  }

  @Override
  public LockResponse lock(LockRequest request) throws TException {
    try (Timer.Context ctx = metricRegistry.timer(HIVE_LOCK_TIME).time()) {
      return super.lock(request);
    }
  }

  @Override
  public void unlock(long lockid) throws TException {
    try (Timer.Context ctx = metricRegistry.timer(HIVE_UNLOCK_TIME).time()) {
      super.unlock(lockid);
    }
  }
}
