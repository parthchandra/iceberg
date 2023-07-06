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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.DefaultEncryptionManagerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestClientSideEncryptionProps extends SparkTestBaseWithCatalog {

  private final String nonExistingKey = "keyABC";

  private static Map<String, String> appendCatalogEncryptionConfigProperties(
      Map<String, String> props) {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.putAll(props);
    newProps.put(
        CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL, "org.apache.iceberg.spark.sql.MockKMS");
    newProps.put(
        CatalogProperties.ENCRYPTION_MANAGER_FACTORY_IMPL,
        DefaultEncryptionManagerFactory.class.getName());
    newProps.put(CatalogProperties.ENCRYPTION_IGNORE_TABLE_PROPS, "true");
    newProps.put(TableProperties.ENCRYPTION_TABLE_KEY, MockKMS.MASTER_KEY_NAME1);
    return newProps;
  }

  // these parameters are broken out to avoid changes that need to modify lots of test suites
  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        appendCatalogEncryptionConfigProperties(SparkCatalogConfig.HIVE.properties())
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        appendCatalogEncryptionConfigProperties(SparkCatalogConfig.HADOOP.properties())
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        appendCatalogEncryptionConfigProperties(SparkCatalogConfig.SPARK.properties())
      }
    };
  }

  public TestClientSideEncryptionProps(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testInsertSelect() {
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg "
            + "TBLPROPERTIES ( 'encryption.table.key.id'='%s' )",
        tableName, nonExistingKey);
    // Insert/select should work ok because a catalog prop sets an existing key that overrides the
    // table prop
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSetTableKeyAPI() {
    System.setProperty(
        DefaultEncryptionManagerFactory.ICEBERG_ENCRYPTION_TABLE_KEY, nonExistingKey);
    sql("CREATE TABLE %s (id bigint, data string, float float) USING iceberg", tableName);
    AssertHelpers.assertThrowsCause(
        "Must fail to encrypt table with non-existing key",
        RuntimeException.class,
        "wrapping key " + nonExistingKey + " is not found",
        () ->
            sql(
                "INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))",
                tableName));
    sql("DROP TABLE IF EXISTS %s", tableName);
    System.clearProperty(DefaultEncryptionManagerFactory.ICEBERG_ENCRYPTION_TABLE_KEY);
  }
}
