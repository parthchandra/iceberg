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

package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

  private static Cache<String, HiveClientPool> clientPoolCache;

  private final Configuration conf;
  private final String metastoreUri;
  private final int clientPoolSize;
  private final long evictionInterval;
  private final String catalog;

  CachedClientPool(Configuration conf, Map<String, String> properties) {
    this.conf = conf;
    this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    this.clientPoolSize = PropertyUtil.propertyAsInt(properties,
            CatalogProperties.CLIENT_POOL_SIZE,
            CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.evictionInterval = PropertyUtil.propertyAsLong(properties,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);

    if (properties.containsKey(CatalogProperties.HIVE_CATALOG)) {
      this.catalog = properties.get(CatalogProperties.HIVE_CATALOG);
    } else {
      this.catalog = MetaStoreUtils.getDefaultCatalog(new HiveConf(conf, CachedClientPool.class));
    }

    init();
  }

  @VisibleForTesting
  HiveClientPool clientPool() {
    return clientPoolCache.get(metastoreUri, k -> new HiveClientPool(clientPoolSize, conf));
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      clientPoolCache = Caffeine.newBuilder().expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
              .removalListener((key, value, cause) -> ((HiveClientPool) value).close())
              .build();
    }
  }

  @VisibleForTesting
  static Cache<String, HiveClientPool> clientPoolCache() {
    return clientPoolCache;
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
    return this.run(action, false);
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
      throws TException, InterruptedException {
    return clientPool().run(client -> {
      String hiveMajorVersion = HiveVersionInfo.getVersion().split("\\.")[0];
      String defaultCatalog = MetaStoreUtils.getDefaultCatalog(new HiveConf(conf, CachedClientPool.class));

      // Tests may return a mock of `HiveMetaStoreClient`, which causes `getInvocationHandler` stuck.
      // HMS catalog is only supported on Apple Hive 2.
      if (hiveMajorVersion.equals("2") && Proxy.isProxyClass(client.getClass())) {
        try {
          InvocationHandler handler = Proxy.getInvocationHandler(client);
          handler.invoke(client,
                  HiveMetaStoreClient.class.getDeclaredMethod("setCurrentCatalog", String.class),
                  new Object[]{this.catalog});
        } catch (NoSuchMethodException e) {
          // If users specify a custom HMS catalog other than default one, we should throw an exception.
          if (!defaultCatalog.equals(this.catalog)) {
            throw new RuntimeMetaException(e, "HiveMetaStoreClient instance doesn't support `setCurrentCatalog` API.");
          }
        } catch (Throwable t) {
          if (!defaultCatalog.equals(this.catalog)) {
            throw new RuntimeMetaException(t,
                    "Failed to invoke `setCurrentCatalog` on the proxy of HiveMetaStoreClient");
          }
        }
      }

      return action.run(client);
    }, retry);
  }
}
