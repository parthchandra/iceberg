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

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestDropTable extends SparkCatalogTestBase {
  private String type;

  public TestDropTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    type = config.get("type");
  }

  @After
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDropTableNoPurge() {
    Assume.assumeTrue(type.equals("hive"));
    Assert.assertFalse("Table should not already exist", validationCatalog.tableExists(tableIdent));

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertNotNull("Should load the new table", table);

    sql("INSERT INTO %s VALUES (1, 'foo')", tableName);

    File tableLocation = Paths.get(table.location().replace("file:", "")).toFile();

    List<File> files = FileUtils.listFiles(tableLocation, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
            .stream()
            .filter(file -> !file.toString().endsWith("crc") && !file.toString().contains("_SUCCESS"))
            .collect(Collectors.toList());

    Assert.assertTrue("Table should have files", files.size() != 0);

    sql("DROP TABLE %s", tableName);

    List<File> filesAfterDrop = FileUtils.listFiles(tableLocation, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
            .stream()
            .filter(file -> !file.toString().endsWith("crc") && !file.toString().contains("_SUCCESS"))
            .collect(Collectors.toList());

    Assert.assertEquals("No files should be destroyed by drop", files, filesAfterDrop);
  }
}
