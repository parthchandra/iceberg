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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTableEncryption extends SparkCatalogTestBase {
  public TestTableEncryption(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg "
            + "TBLPROPERTIES ( "
            + "'encryption.table.key.id'='%s' , "
            + "'encryption.kms.client-impl'='org.apache.iceberg.spark.sql.MockKMS' )",
        tableName, MockKMS.MASTER_KEY_NAME1);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
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
  public void testDelete() {
    sql("DELETE FROM %s WHERE id == '2'", tableName);

    List<Object[]> expected = ImmutableList.of(row(1L, "a", 1.0F), row(3L, "c", Float.NaN));

    assertEquals(
        "Should return all remaining rows",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDeleteWithoutKeys() {
    sql("ALTER TABLE %s UNSET TBLPROPERTIES ('encryption.table.key.id')", tableName);

    AssertHelpers.assertThrows(
        "Must fail to modify encrypted table without key",
        SparkException.class,
        () -> sql("DELETE FROM %s WHERE id == '2'", tableName));
  }
}
