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

import static org.apache.iceberg.flink.FlinkCatalogFactory.DEFAULT_CATALOG_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Junit 5 extension for running Flink SQL queries. {@link
 * org.apache.flink.test.junit5.MiniClusterExtension} is used for executing the SQL batch jobs.
 */
public class FlinkSqlExtension implements BeforeEachCallback, AfterEachCallback {
  volatile TableEnvironment tEnv;
  String catalogName;
  Map<String, String> catalogProperties;
  String databaseName;
  Path warehouse;

  public FlinkSqlExtension(
      String catalogName, Map<String, String> catalogProperties, String databaseName) {
    this.catalogName = catalogName;
    this.catalogProperties = Maps.newHashMap(catalogProperties);
    this.databaseName = databaseName;

    // Add temporary dir as a warehouse location
    try {
      this.warehouse = Files.createTempDirectory("warehouse");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.catalogProperties.put(
        CatalogProperties.WAREHOUSE_LOCATION, String.format("file://%s", warehouse));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    exec("CREATE CATALOG %s WITH %s", catalogName, toWithClause(catalogProperties));
    exec("CREATE DATABASE %s.%s", catalogName, databaseName);
    exec("USE CATALOG %s", catalogName);
    exec("USE %s", databaseName);
  }

  @Override
  public void afterEach(ExtensionContext context) throws IOException {
    // Drop tables
    List<Row> tables = exec("SHOW TABLES");
    tables.forEach(t -> exec("DROP TABLE IF EXISTS %s", t.getField(0)));
    exec("USE CATALOG %s", DEFAULT_CATALOG_NAME);
    exec("USE %s", getTableEnv().listDatabases()[0]);
    exec("DROP DATABASE IF EXISTS %s.%s", catalogName, databaseName);
    exec("USE CATALOG default_catalog");
    exec("DROP CATALOG IF EXISTS %s", catalogName);
    Files.walk(warehouse).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
  }

  public List<Row> exec(String query, Object... args) {
    TableResult tableResult = exec(getTableEnv(), query, args);
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  public CatalogLoader catalogLoader() {
    return CatalogLoader.hadoop(catalogName, new Configuration(), catalogProperties);
  }

  public TableLoader tableLoader(String tableName) {
    TableLoader tableLoader =
        TableLoader.fromCatalog(catalogLoader(), TableIdentifier.of(databaseName, tableName));
    tableLoader.open();
    return tableLoader;
  }

  private static TableResult exec(TableEnvironment env, String query, Object... args) {
    return env.executeSql(String.format(query, args));
  }

  private TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

          TableEnvironment env = TableEnvironment.create(settings);
          env.getConfig()
              .getConfiguration()
              .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
          tEnv = env;
        }
      }
    }
    return tEnv;
  }

  private static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder
          .append("'")
          .append(entry.getKey())
          .append("'")
          .append("=")
          .append("'")
          .append(entry.getValue())
          .append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }
}
