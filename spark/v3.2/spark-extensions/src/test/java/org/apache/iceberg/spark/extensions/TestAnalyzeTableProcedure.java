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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestAnalyzeTableProcedure extends SparkExtensionsTestBase {

  public TestAnalyzeTableProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output = sql("CALL %s.system.analyze_table('%s')", catalogName, tableIdent);

    Table table = validationCatalog.loadTable(tableIdent);

    String formattedString = (String) output.get(0)[0];

    Assert.assertTrue(formattedString.contains("=== TABLE CONFIGURATION ==="));
    Assert.assertTrue(formattedString.contains("location: " + table.location()));

    Assert.assertTrue(formattedString.contains("=== SNAPSHOTS ==="));
    Assert.assertTrue(formattedString.contains("snapshots-count: 0"));

    Assert.assertTrue(formattedString.contains("=== UNEXPIRED SNAPSHOTS ==="));
    Assert.assertTrue(formattedString.contains("unexpired-snapshots-count: 0"));
  }

  @Test
  public void testDisabledPartitionStats() {
    sql("CREATE TABLE %s (c1 int, c2 int, c3 int, c4 string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 1, 1, '1'), (2, 2, 2, '2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 1, 1, '1'), (2, 2, 2, '2')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    List<Object[]> output =
        sql(
            "CALL %s.system.analyze_table("
                + "  table => '%s',"
                + "  mode => 'full',"
                + "  options => map('partition-stats.enabled', 'false'))",
            catalogName, tableIdent);

    String formattedString = (String) output.get(0)[0];

    Assert.assertTrue(formattedString.contains("partition-stats-file-location: null"));
    Assert.assertFalse(
        formattedString.contains("partition-stats-file-location: " + table.location()));
  }

  @Test
  public void testQuickAnalysis() {
    sql("CREATE TABLE %s (c1 int, c2 int, c3 int, c4 string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 1, 1, '1'), (2, 2, 2, '2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 1, 1, '1'), (2, 2, 2, '2')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.analyze_table(table => '%s', mode => 'quick')",
            catalogName, tableIdent);

    String formattedString = (String) output.get(0)[0];

    Assert.assertTrue(formattedString.contains("=== TABLE CONFIGURATION ==="));
    Assert.assertTrue(formattedString.contains("=== SNAPSHOTS ==="));
    Assert.assertTrue(formattedString.contains("=== UNEXPIRED SNAPSHOTS ==="));
    Assert.assertTrue(formattedString.contains("=== MANIFESTS ==="));

    Assert.assertFalse(formattedString.contains("CONTENT FILES"));
    Assert.assertFalse(formattedString.contains("APPENDS"));
    Assert.assertFalse(formattedString.contains("OVERWRITE OPERATIONS"));
    Assert.assertFalse(formattedString.contains("ROW DELTAS"));
  }

  @Test
  public void testFullAnalysis() {
    sql("CREATE TABLE %s (c1 int, c2 int, c3 int, c4 string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 1, 1, '1'), (2, 2, 2, '2')", tableName);

    sql("ALTER TABLE %s ADD COLUMN c5 INT", tableName);

    sql("INSERT INTO TABLE %s VALUES (3, 3, 3, '3', 3)", tableName);

    sql("ALTER TABLE %s WRITE ORDERED BY c1, c2", tableName);

    sql("ALTER TABLE %s ADD PARTITION FIELD c3", tableName);

    sql("INSERT INTO TABLE %s VALUES (4, 4, 4, '4', 4)", tableName);

    sql("UPDATE %s SET c1 = -1 WHERE c1 %% 2 = 1", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(-1, 1, 1, "1", null),
            row(-1, 3, 3, "3", 3),
            row(2, 2, 2, "2", null),
            row(4, 4, 4, "4", 4)),
        sql("SELECT * FROM %s ORDER BY c1, c2", tableName));

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' '2')", tableName, FORMAT_VERSION);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'merge-on-read')", tableName, UPDATE_MODE);

    sql("UPDATE %s SET c5 = 0", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(-1, 1, 1, "1", 0),
            row(-1, 3, 3, "3", 0),
            row(2, 2, 2, "2", 0),
            row(4, 4, 4, "4", 0)),
        sql("SELECT * FROM %s ORDER BY c1, c2", tableName));

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' '1')", tableName, MAX_SNAPSHOT_AGE_MS);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' '1')", tableName, MIN_SNAPSHOTS_TO_KEEP);

    Table table = validationCatalog.loadTable(tableIdent);

    List<Object[]> output =
        sql(
            "CALL %s.system.analyze_table("
                + "  table => '%s',"
                + "  mode => 'full',"
                + "  output_location => '%s',"
                + "  options => map('partition-stats.enabled', 'true', 'partition-stats.num-output-files', '4'))",
            catalogName, tableIdent, table.location() + "/custom-location");

    String formattedString = (String) output.get(0)[0];

    Assert.assertTrue(formattedString.contains("=== TABLE CONFIGURATION ==="));
    Assert.assertTrue(formattedString.contains("location: " + table.location()));
    Assert.assertTrue(formattedString.contains("format-version: 2"));
    Assert.assertTrue(formattedString.contains("write.update.mode: merge-on-read"));
    Assert.assertTrue(formattedString.contains("write.distribution-mode: range"));
    Assert.assertTrue(formattedString.contains("current-schema-id: 1"));
    Assert.assertTrue(formattedString.contains("last-assigned-column-id: 5"));
    Assert.assertTrue(formattedString.contains("schema (0)"));
    Assert.assertTrue(formattedString.contains("schema (1)"));
    Assert.assertTrue(formattedString.contains("default-spec-id: 1"));
    Assert.assertTrue(formattedString.contains("spec (0)"));
    Assert.assertTrue(formattedString.contains("spec (1)"));
    Assert.assertTrue(formattedString.contains("default-sort-order-id: 1"));
    Assert.assertTrue(formattedString.contains("sort order (0)"));
    Assert.assertTrue(formattedString.contains("sort order (1)"));

    Assert.assertTrue(formattedString.contains("=== SNAPSHOTS ==="));
    Assert.assertTrue(formattedString.contains("snapshots-count: 5"));
    Assert.assertTrue(formattedString.contains("row-deltas-count: 1"));
    Assert.assertTrue(formattedString.contains("overwrites-count: 1"));
    Assert.assertTrue(formattedString.contains("appends-count: 3"));

    Assert.assertTrue(formattedString.contains("=== UNEXPIRED SNAPSHOTS ==="));
    Assert.assertTrue(formattedString.contains("unexpired-snapshots-count: 4"));
    Assert.assertTrue(formattedString.contains("unexpired-data-files-count: 2"));

    Assert.assertTrue(formattedString.contains("=== MANIFESTS ==="));
    Assert.assertTrue(formattedString.contains("total-manifest-files-count: 6"));

    Assert.assertTrue(formattedString.contains("=== CONTENT FILES ==="));
    Assert.assertTrue(formattedString.contains("total-data-files-count: 8"));
    Assert.assertTrue(formattedString.contains("total-data-records-count: 8"));
    Assert.assertTrue(formattedString.contains("total-position-delete-files-count: 4"));
    Assert.assertTrue(formattedString.contains("total-position-delete-records-count: 4"));
    Assert.assertTrue(formattedString.contains("total-equality-delete-files-count: 0"));
    Assert.assertTrue(formattedString.contains("total-equality-delete-records-count: 0"));

    Assert.assertTrue(formattedString.contains("=== APPENDS ==="));
    Assert.assertTrue(formattedString.contains("total-added-data-files-count: 1"));

    Assert.assertTrue(formattedString.contains("=== OVERWRITE OPERATIONS ==="));
    Assert.assertTrue(formattedString.contains("total-added-data-files-count: 2"));
    Assert.assertTrue(formattedString.contains("total-removed-data-files-count: 2"));

    Assert.assertTrue(formattedString.contains("=== ROW DELTAS (merge-on-read operations) ==="));
    Assert.assertTrue(formattedString.contains("total-added-data-files-count: 4"));
    Assert.assertTrue(formattedString.contains("total-added-position-delete-files-count: 4"));
    Assert.assertTrue(formattedString.contains("total-added-equality-delete-files-count: 0"));
  }
}
