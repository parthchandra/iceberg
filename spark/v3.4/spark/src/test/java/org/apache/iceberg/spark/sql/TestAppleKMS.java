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
package org.apache.iceberg.spark.sql;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAppleKMS extends SparkCatalogTestBase {
  public TestAppleKMS(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() throws IOException, URISyntaxException {
    String keyFilePath = getClass().getClassLoader().getResource("master_keys_v1").getPath();
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg "
            + "TBLPROPERTIES ( "
            + "'encryption.table.key.id'='keyA' , "
            + "'kms.client.bridge.key.source.uid'='whisper "
            + "group:sample-usecase@group.apple.com, bucket:uc1-bucket, secret:master_keys_vx ' , "
            + "'encryption.kms.client-impl'='org.apache.iceberg.encryption.kms.BridgeKmsClient' , "
            + "'kms.client.bridge.key.file.path.pattern'='%s')",
        tableName, keyFilePath);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);
  }

  @Test
  public void testSelectWithoutKeys() {
    sql("ALTER TABLE %s UNSET TBLPROPERTIES ('encryption.table.key.id')", tableName);

    AssertHelpers.assertThrows(
        "Must fail to read encrypted data files without key",
        SparkException.class,
        "ParquetCryptoRuntimeException: Trying to read file with encrypted footer. No keys available",
        () -> sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
