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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.encryption.DefaultEncryptionManagerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestTableEncryptionConfiguredWithCatalogProps extends CatalogTestBase {

  private static Map<String, String> appendCatalogEncryptionConfigProperties(
      Map<String, String> props) {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.putAll(props);
    newProps.put(
        CatalogProperties.ITEV0_ENCRYPTION_KMS_CLIENT_IMPL, "org.apache.iceberg.spark.sql.MockKMS");
    newProps.put(
        CatalogProperties.ITEV0_ENCRYPTION_MANAGER_FACTORY_IMPL,
        DefaultEncryptionManagerFactory.class.getName());
    return newProps;
  }

  // these parameters are broken out to avoid changes that need to modify lots of test suites
  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
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

  @BeforeEach
  public void createTables() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg "
            + "TBLPROPERTIES ( 'encryption.table.key.id'='%s' )",
        tableName, MockKMS.MASTER_KEY_NAME1);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
  }

  @TestTemplate
  public void testSelectWithoutKeys() {
    sql("ALTER TABLE %s UNSET TBLPROPERTIES ('encryption.table.key.id')", tableName);

    assertThatThrownBy(() -> sql("SELECT * FROM %s", tableName))
        .isInstanceOf(SparkException.class)
        .hasMessageContaining(
            "ParquetCryptoRuntimeException: Trying to read file with encrypted footer. No keys available");
  }

  @TestTemplate
  public void testDelete() {
    sql("DELETE FROM %s WHERE id == '2'", tableName);

    List<Object[]> expected = ImmutableList.of(row(1L, "a", 1.0F), row(3L, "c", Float.NaN));

    assertEquals(
        "Should return all remaining rows",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDeleteWithoutKeys() {
    sql("ALTER TABLE %s UNSET TBLPROPERTIES ('encryption.table.key.id')", tableName);

    assertThatThrownBy(() -> sql("DELETE FROM %s WHERE id == '2'", tableName))
        .isInstanceOf(SparkException.class)
        .cause()
        .hasMessageContaining(
            "ParquetCryptoRuntimeException: Trying to read file with encrypted footer. No keys available");
  }
}
