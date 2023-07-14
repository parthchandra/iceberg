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
package org.apache.iceberg.spark.source;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import com.apple.boson.BosonConf;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBosonSparkReaderDeletes extends TestSparkReaderDeletes {

  public static final Schema SCHEMA_WITH_FIXEDTYPE =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          // BOSON doesn't support fixed type yet, read fixed type will fall back to regular
          // vectorization
          Types.NestedField.required(2, "fixed", Types.FixedType.ofLength(3)));

  public static final Schema SCHEMA_FOR_POS_TEST =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private Table fixedTypeTable = null;
  private List<Record> fixedTypeRecords = null;

  private Table posTestTable = null;
  private List<Record> posTestRecords = null;

  public TestBosonSparkReaderDeletes(String format, boolean vectorized) {
    super(format, vectorized);
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config(BosonConf.BOSON_ENABLED().key(), "true")
            .enableHiveSupport()
            .getOrCreate();

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @After
  public void cleanup() throws IOException {
    dropTable("test");
    dropTable("test2");
    dropTable("test3");
    dropTable("test4");
  }

  private void initFixedTypeTable() throws IOException {
    dropTable("test3");
    this.fixedTypeTable = createTable("test3", SCHEMA_WITH_FIXEDTYPE, null);

    GenericRecord record = GenericRecord.create(fixedTypeTable.schema());

    this.fixedTypeRecords =
        Lists.newArrayList(
            record.copy("id", 1, "fixed", new byte[] {0, 1, 2}),
            record.copy("id", 2, "fixed", new byte[] {3, 4, 5}),
            record.copy("id", 3, "fixed", new byte[] {6, 7, 8}),
            record.copy("id", 4, "fixed", new byte[] {9, 10, 11}),
            record.copy("id", 5, "fixed", new byte[] {12, 13, 14}));

    DataFile dataFileForFixedType =
        FileHelpers.writeDataFile(
            fixedTypeTable,
            Files.localOutput(temp.newFile()),
            TestHelpers.Row.of(0),
            fixedTypeRecords);

    fixedTypeTable.newAppend().appendFile(dataFileForFixedType).commit();
  }

  private void initPosTestTable() throws IOException {
    this.posTestTable = createTable("test4", SCHEMA_FOR_POS_TEST, null);
    GenericRecord record = GenericRecord.create(posTestTable.schema());

    this.posTestRecords = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      this.posTestRecords.add(record.copy("id", i, "data", "test" + i));
    }

    DataFile dataFile =
        FileHelpers.writeDataFile(
            posTestTable, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), posTestRecords);

    posTestTable.newAppend().appendFile(dataFile).commit();
  }

  @Test
  public void testFixedType() throws IOException {
    initFixedTypeTable();
    Types.StructType projection = fixedTypeTable.schema().select("*").asStruct();
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", "test3").toString())
            .selectExpr("*");

    StructLikeSet actual = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actual.add(rowWrapper.wrap(row));
            });

    Assert.assertEquals("Table should contain 5 rows", 5, actual.size());
  }

  @Test
  public void testPositionMetadataColumn() throws IOException {
    initPosTestTable();
    List<Long> expected = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);

    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", "test4").toString())
            .selectExpr("_pos");
    List<Long> actual = Lists.newArrayList();
    df.collectAsList()
        .forEach(
            row -> {
              actual.add(row.getLong(0));
            });

    Assert.assertTrue(actual.containsAll(expected));
  }
}
