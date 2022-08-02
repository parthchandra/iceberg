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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.junit.Assert;
import org.junit.Test;

public class TestCachedClientPool extends HiveMetastoreTest {

  @Test
  public void testClientPoolCleaner() throws InterruptedException {
    String metastoreUri = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    CachedClientPool clientPool = new CachedClientPool(hiveConf, Collections.emptyMap());
    HiveClientPool clientPool1 = clientPool.clientPool();
    Assert.assertTrue(CachedClientPool.clientPoolCache().getIfPresent(metastoreUri) == clientPool1);
    TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL - TimeUnit.SECONDS.toMillis(2));
    HiveClientPool clientPool2 = clientPool.clientPool();
    Assert.assertTrue(clientPool1 == clientPool2);
    TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL + TimeUnit.SECONDS.toMillis(5));
    Assert.assertNull(CachedClientPool.clientPoolCache().getIfPresent(metastoreUri));
  }

  @Test
  public void testHiveCatalog() throws Exception {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(CatalogProperties.HIVE_CATALOG, "test_catalog");

    CachedClientPool clientPool = new CachedClientPool(hiveConf, properties);

    clientPool.run(client -> {
      InvocationHandler handler = Proxy.getInvocationHandler(client);
      try {
        String catalog = (String) handler.invoke(client,
                HiveMetaStoreClient.class.getDeclaredMethod("getCurrentCatalog"),
                new Object[]{});
        Assert.assertEquals(catalog, "test_catalog");
      } catch (Throwable e) {
        throw new RuntimeException("Test failed to invoke `getCurrentCatalog`", e);
      }
      return null;
    });
  }
}
