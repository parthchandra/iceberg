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
package org.apache.iceberg.spark;

import static com.apple.boson.parquet.ReadOptions.BOSON_PARQUET_PARALLEL_IO_THREADS_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkUtil extends SparkTestBase {

  public static class ConfigurableHadoopCatalog extends HadoopCatalog {

    @Override
    public Map<String, String> properties() {
      return super.properties();
    }
  }

  public static class MockS3FileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {}
  }

  @Test
  public void testBosonAutoConf() throws IOException {
    SparkSession spark = null;
    try {
      int maxConnections = 10;
      int numExecutorCores = 2;
      File warehouse = File.createTempFile("warehouse", null);
      CaseInsensitiveStringMap options =
          new CaseInsensitiveStringMap(
              ImmutableMap.of(
                  "catalog-impl",
                  "org.apache.iceberg.spark.TestSparkUtil$ConfigurableHadoopCatalog",
                  "warehouse",
                  "file:" + warehouse,
                  "io-impl",
                  "org.apache.iceberg.spark.TestSparkUtil$MockS3FileIO",
                  "http-client.apache.max-connections",
                  Integer.toString(maxConnections)));

      maxConnections = 2 * numExecutorCores * BOSON_PARQUET_PARALLEL_IO_THREADS_DEFAULT;
      spark =
          SparkSession.builder()
              .master("local[2]")
              .config(SparkLauncher.EXECUTOR_CORES, Integer.toString(numExecutorCores))
              .config("spark.boson.enabled", "true")
              .getOrCreate();

      String name = "custom_test";
      Catalog catalog = new SparkCatalog().buildIcebergCatalog(name, options);
      Assert.assertEquals(
          maxConnections,
          Integer.parseInt(
              ((ConfigurableHadoopCatalog) catalog)
                  .properties()
                  .get("http-client.apache.max-connections")));
    } finally {
      if (spark != null) {
        spark.stop();
      }
    }
  }
}
