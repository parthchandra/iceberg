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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.CopyTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.CopyTableSparkAction.PathPair;
import org.apache.iceberg.spark.source.FourColumnRecord;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.spark.sql.MockKMS;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCopyTableAction extends SparkTestBase {

  @Rule public TemporaryFolder staging = new TemporaryFolder();

  protected ActionsProvider actions() {
    return SparkActions.get();
  }

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;
  private Table table = null;

  private String ns = "testns";
  private String backupNs = "backupns";

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    this.table = createATableWith2Snapshots(tableLocation);
    createNameSpaces();
  }

  @After
  public void cleanupTableSetup() throws Exception {
    dropNameSpaces();
  }

  private Table createATableWith2Snapshots(String location) {
    return createTableWithSnapshots(location, 2);
  }

  private Table createTableWithSnapshots(String location, int snapshotNumber) {
    return createTableWithSnapshots(location, snapshotNumber, Maps.newHashMap());
  }

  protected Table createTableWithSnapshots(
      String location, int snapshotNumber, Map<String, String> properties) {
    Table newTable = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, location);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    for (int i = 0; i < snapshotNumber; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    }

    return newTable;
  }

  private void createNameSpaces() {
    sql("CREATE DATABASE IF NOT EXISTS %s", ns);
    sql("CREATE DATABASE IF NOT EXISTS %s", backupNs);
  }

  private void dropNameSpaces() {
    sql("DROP DATABASE IF EXISTS %s CASCADE", ns);
    sql("DROP DATABASE IF EXISTS %s CASCADE", backupNs);
  }

  @Test
  public void testCopyTable() throws Exception {
    String targetTableLocation = newTableLocation();

    // check the data file location before the rebuild
    List<String> validDataFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("Should be 2 valid data files", 2, validDataFiles.size());

    CopyTable.Result result =
        actions()
            .copyTable(table)
            .rewriteLocationPrefix(tableLocation, targetTableLocation)
            .endVersion("v3.metadata.json")
            .execute();

    Assert.assertEquals("The latest version should be", "v3.metadata.json", result.latestVersion());

    checkMetadataFileNum(3, 2, 2, result);
    checkDataFileNum(2, result);

    // copy the metadata files and data files
    copyTableFiles(tableLocation, targetTableLocation, stagingDir(result));

    // verify the data file path after the rebuild
    List<String> validDataFilesAfterRebuilt =
        spark
            .read()
            .format("iceberg")
            .load(targetTableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("Should be 2 valid data files", 2, validDataFilesAfterRebuilt.size());
    for (String item : validDataFilesAfterRebuilt) {
      Assert.assertTrue(
          "Data file should point to the new location", item.startsWith(targetTableLocation));
    }

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetTableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2", "c3").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testDataFilesDiff() throws Exception {
    CopyTable.Result result =
        actions()
            .copyTable(table)
            .rewriteLocationPrefix(tableLocation, newTableLocation())
            .lastCopiedVersion("v2.metadata.json")
            .execute();

    checkDataFileNum(1, result);

    List<String> rebuiltFiles =
        spark
            .read()
            .format("text")
            .load(result.metadataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();

    // v3.metadata.json, one manifest-list file, one manifest file
    checkMetadataFileNum(3, result);

    String currentSnapshotId = String.valueOf(table.currentSnapshot().snapshotId());
    Assert.assertTrue(
        "Should have the current snapshot file",
        rebuiltFiles.stream().filter(c -> c.contains(currentSnapshotId)).count() == 1);

    String parentSnapshotId = String.valueOf(table.currentSnapshot().parentId());
    Assert.assertTrue(
        "Should NOT have the parent snapshot file",
        rebuiltFiles.stream().filter(c -> c.contains(parentSnapshotId)).count() == 0);
  }

  @Test
  public void testTableWith3Snapshots() throws Exception {
    String location = newTableLocation();
    Table tableWith3Snaps = createTableWithSnapshots(location, 3);
    CopyTable.Result result =
        actions()
            .copyTable(tableWith3Snaps)
            .rewriteLocationPrefix(location, newTableLocation())
            .lastCopiedVersion("v2.metadata.json")
            .execute();

    checkMetadataFileNum(2, 2, 2, result);
    checkDataFileNum(2, result);

    // start from the first version
    CopyTable.Result result1 =
        actions()
            .copyTable(tableWith3Snaps)
            .rewriteLocationPrefix(location, newTableLocation())
            .lastCopiedVersion("v1.metadata.json")
            .execute();

    checkMetadataFileNum(3, 3, 3, result1);
    checkDataFileNum(3, result1);
  }

  @Test
  public void testFullTableCopy() throws Exception {
    CopyTable.Result result =
        actions()
            .copyTable(table)
            .rewriteLocationPrefix(tableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(3, 2, 2, result);
    checkDataFileNum(2, result);
  }

  @Test
  public void testDeleteDataFile() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(location);
    List<String> validDataFiles =
        spark
            .read()
            .format("iceberg")
            .load(location + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();

    sourceTable.newDelete().deleteFile(validDataFiles.stream().findFirst().get()).commit();

    String targetLocation = newTableLocation();
    CopyTable.Result result =
        actions().copyTable(sourceTable).rewriteLocationPrefix(location, targetLocation).execute();

    checkMetadataFileNum(4, 3, 3, result);
    checkDataFileNum(2, result);

    // copy the metadata files and data files
    copyTableFiles(location, targetLocation, stagingDir(result));

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetLocation);
    Assert.assertEquals(
        "There are only one row left since we deleted a data file",
        1,
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).count());
  }

  @Test
  public void testWithDeleteManifestsAndPositionDeletes() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(location);
    String targetLocation = newTableLocation();

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(
                sourceTable
                    .currentSnapshot()
                    .addedDataFiles(sourceTable.io())
                    .iterator()
                    .next()
                    .path(),
                0L));

    File file = new File(removePrefix(sourceTable.location()) + "/data/deeply/nested/file.parquet");
    DeleteFile positionDeletes =
        FileHelpers.writeDeleteFile(
                sourceTable, sourceTable.io().newOutputFile(file.toURI().toString()), deletes)
            .first();

    sourceTable.newRowDelta().addDeletes(positionDeletes).commit();

    Assert.assertEquals(
        "The number of rows should be", 1, spark.read().format("iceberg").load(location).count());

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .stagingLocation(stagingWithScheme())
            .outputTargetFilePath()
            .rewriteLocationPrefix(location, targetLocation)
            .execute();

    // We have one more snapshot, an additional manifest list, and a new (delete) manifest
    checkMetadataFileNum(4, 3, 3, result);
    // We have one additional file for positional deletes
    checkDataFileNum(3, result);

    // copy the metadata files and data files
    copyTableFiles(result);

    // Positional delete affects a single row, so only one row must remain
    Assert.assertEquals(
        "The number of rows should be",
        1,
        spark.read().format("iceberg").load(targetLocation).count());
  }

  @Test
  public void testWithDeleteManifestsAndEqualityDeletes() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createTableWithSnapshots(location, 1);
    String targetLocation = newTableLocation();

    // Add more varied data
    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "BBBB"),
            new ThreeColumnRecord(4, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(5, "DDDDDDDDDD", "DDDD"));
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .coalesce(1)
        .select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(location);

    Schema deleteRowSchema = sourceTable.schema().select("c2");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("c2", "AAAAAAAAAA"), dataDelete.copy("c2", "CCCCCCCCCC"));
    File file = new File(removePrefix(sourceTable.location()) + "/data/deeply/nested/file.parquet");
    DeleteFile equalityDeletes =
        FileHelpers.writeDeleteFile(
            sourceTable,
            sourceTable.io().newOutputFile(file.toURI().toString()),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);
    sourceTable.newRowDelta().addDeletes(equalityDeletes).commit();

    CopyTable.Result result =
        actions().copyTable(sourceTable).rewriteLocationPrefix(location, targetLocation).execute();

    // We have four metadata files: for the table creation, for the initial snapshot, for the
    // second append here, and for commit with equality deletes. Thus, we have three manifest lists
    checkMetadataFileNum(4, 3, 3, result);
    // A data file for each snapshot (two with data, one with equality deletes)
    checkDataFileNum(3, result);

    // copy the metadata files and data files
    copyTableFiles(location, targetLocation, stagingDir(result));

    // Equality deletes affect three rows, so just two rows must remain
    Assert.assertEquals(
        "The number of rows should be",
        2,
        spark.read().format("iceberg").load(targetLocation).count());
  }

  @Test
  public void testCopyWithThreadPool() throws Exception {
    String location = newTableLocation();
    Table sourceTable =
        createTableWithSnapshots(
            location,
            1,
            Collections.singletonMap(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "10000"));

    AtomicInteger threadsIndex = new AtomicInteger(0);
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            8,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("copy-service" + threadsIndex.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            });

    // create 600 more snapshots
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    for (int i = 0; i < 600; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    }

    sourceTable.refresh();
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(location, newTableLocation())
            .lastCopiedVersion("v2.metadata.json")
            .executeWith(executorService)
            .execute();

    checkMetadataFileNum(600, 600, 600, result);
    checkDataFileNum(600, result);
  }

  @Test
  public void testFullTableCopyWithDeletedVersionFiles() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createTableWithSnapshots(location, 2);
    // expire the first snapshot
    Table staticTable = newStaticTable(location + "metadata/v2.metadata.json", table.io());
    actions()
        .expireSnapshots(sourceTable)
        .expireSnapshotId(staticTable.currentSnapshot().snapshotId())
        .execute();

    // create 100 more snapshots
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    for (int i = 0; i < 100; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    }
    sourceTable.refresh();

    // v1/v2/v3.metadata.json has been deleted in v104.metadata.json, and there is no way to find
    // the first snapshot
    // from the version file history
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(location, newTableLocation())
            .execute();

    // you can only find 101 snapshots but the manifest file and data file count should be 102.
    checkMetadataFileNum(101, 101, 102, result);
    checkDataFileNum(102, result);
  }

  protected Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  @Test
  public void testRewriteTableWithoutSnapshot() throws Exception {
    CopyTable.Result result =
        actions()
            .copyTable(table)
            .rewriteLocationPrefix(tableLocation, newTableLocation())
            .endVersion("v1.metadata.json")
            .execute();

    // the only rebuilt file is v1.metadata.json since it contains no snapshot
    checkMetadataFileNum(1, result);
    checkDataFileNum(0, result);
  }

  @Test
  public void testExpireSnapshotBeforeRewrite() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    actions()
        .expireSnapshots(sourceTable)
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(4, 1, 2, result);

    checkDataFileNum(2, result);
  }

  @Test
  public void testStartSnapshotWithoutValidSnapshot() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    actions()
        .expireSnapshots(sourceTable)
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    Assert.assertEquals(
        "1 out 2 snapshot has been removed", 1, ((List) sourceTable.snapshots()).size());

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .lastCopiedVersion("v2.metadata.json")
            .execute();

    // 2 metadata.json, 1 manifest list file, 1 manifest files
    checkMetadataFileNum(4, result);
    checkDataFileNum(1, result);
  }

  @Test
  public void testMoveTheVersionExpireSnapshot() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    actions()
        .expireSnapshots(sourceTable)
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    // only move version v4, which is the version generated by snapshot expiration
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .lastCopiedVersion("v3.metadata.json")
            .execute();

    // only v4.metadata.json needs to move
    checkMetadataFileNum(1, result);
    // no data file needs to move
    checkDataFileNum(0, result);
  }

  @Test
  public void testMoveVersionWithInvalidSnapshots() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // expire one snapshot
    actions()
        .expireSnapshots(sourceTable)
        .expireSnapshotId(sourceTable.currentSnapshot().parentId())
        .execute();

    AssertHelpers.assertThrows(
        "Copy a version with invalid snapshots aren't allowed",
        UnsupportedOperationException.class,
        () ->
            actions()
                .copyTable(sourceTable)
                .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
                .endVersion("v3.metadata.json")
                .execute());
  }

  @Test
  public void testRollBack() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);
    Long secondSnapshotId = sourceTable.currentSnapshot().snapshotId();

    // roll back to the first snapshot(v2)
    sourceTable
        .manageSnapshots()
        .setCurrentSnapshot(sourceTable.currentSnapshot().parentId())
        .commit();

    // add a new snapshot
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(sourceTableLocation);

    sourceTable.refresh();

    // roll back to the second snapshot(v3)
    sourceTable.manageSnapshots().setCurrentSnapshot(secondSnapshotId).commit();
    // copy table
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    // check the result
    checkMetadataFileNum(6, 3, 3, result);
  }

  @Test
  public void testWriteAuditPublish() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // enable WAP
    sourceTable
        .updateProperties()
        .set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true")
        .commit();
    spark.conf().set("spark.wap.id", "1");

    // add a new snapshot without changing the current snapshot of the table
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(sourceTableLocation);

    sourceTable.refresh();

    // copy table
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    // check the result. There are 3 snapshots in total, although the current snapshot is the second
    // one.
    checkMetadataFileNum(5, 3, 3, result);
  }

  @Test
  public void testSchemaChange() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createATableWith2Snapshots(sourceTableLocation);

    // change the schema
    sourceTable.updateSchema().addColumn("c4", Types.StringType.get()).commit();

    // copy table
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    // check the result
    checkMetadataFileNum(4, 2, 2, result);
  }

  @Test
  public void testWithTargetTable() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 3);
    Table targetTable = createATableWith2Snapshots(targetTableLocation);

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .targetTable(targetTable)
            .execute();

    Assert.assertEquals("The latest version should be", "v4.metadata.json", result.latestVersion());

    // 3 files rebuilt from v3 to v4: v4.metadata.json, one manifest list, one manifest file
    checkMetadataFileNum(3, result);
  }

  @Test
  public void testInvalidStartVersion() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 3);
    String targetTableLocation = newTableLocation();
    Table targetTable = createATableWith2Snapshots(targetTableLocation);

    AssertHelpers.assertThrows(
        "The valid start version should be v3",
        IllegalArgumentException.class,
        "The start version isn't the current version of the target table.",
        () ->
            actions()
                .copyTable(sourceTable)
                .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
                .targetTable(targetTable)
                .lastCopiedVersion("v2.metadata.json")
                .execute());
  }

  @Test
  public void testEncryptedTable() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("encryption.table.key.id", MockKMS.MASTER_KEY_NAME1);
    properties.put("encryption.kms.client-impl", "org.apache.iceberg.spark.sql.MockKMS");
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", "encryptedTbl", 1);

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .execute();

    // copy the metadata files and data files
    copyTableFiles(sourceTableLocation, targetTableLocation, stagingDir(result));

    // register the target table
    String versionFile = fileName(currentMetadata(sourceTable).metadataFileLocation());
    String targetTableName = "encryptedTbl1";
    TableIdentifier tableIdentifier = TableIdentifier.of("default", targetTableName);
    catalog.registerTable(tableIdentifier, targetTableLocation + "/metadata/" + versionFile);

    // verify data rows
    assertEquals(
        "Rows should match",
        ImmutableList.of(row(0L, "AAAAAAAAAA", "AAAA")),
        sql("select * from hive.default.%s", targetTableName));

    // verify the target table is encrypted
    sql(
        "ALTER TABLE hive.default.%s UNSET TBLPROPERTIES ('encryption.table.key.id')",
        targetTableName);

    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM hive.default.%s", targetTableName),
            "Must fail to read encrypted data files without key")
        .isInstanceOf(SparkException.class)
        .hasMessageContaining(
            "ParquetCryptoRuntimeException: Trying to read file with encrypted footer. No keys available");
  }

  @Test
  public void testSnapshotIdInheritanceEnabled() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true");

    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2, properties);

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(7, result);
    checkDataFileNum(2, result);
  }

  @Test
  public void testMetadataCompression() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2, properties);

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .endVersion("v2.gz.metadata.json")
            .execute();

    checkMetadataFileNum(4, result);
    checkDataFileNum(1, result);

    result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .lastCopiedVersion("v1.gz.metadata.json")
            .execute();

    checkMetadataFileNum(6, result);
    checkDataFileNum(2, result);
  }

  @Test
  public void testOutputTargetDataFilePath() throws Exception {
    String targetTableLocation = newTableLocation();

    CopyTable.Result result =
        actions()
            .copyTable(table)
            .rewriteLocationPrefix(tableLocation, targetTableLocation)
            .outputTargetFilePath()
            .execute();

    checkMetadataFileNum(7, result);
    checkDataFileNum(2, result);
    List<PathPair> metadataFilesToMove = readPathPairList(result.metadataFileListLocation());
    for (PathPair metadataFileToMove : metadataFilesToMove) {
      Assert.assertTrue(
          "Source Metadata file should point to the old location",
          metadataFileToMove.getSource().startsWith(tableLocation));
      Assert.assertTrue(
          "Target Metadata file should point to the new location",
          metadataFileToMove.getTarget().startsWith(targetTableLocation));
    }
    List<PathPair> dataFilesToMove = readPathPairList(result.dataFileListLocation());
    for (PathPair dataFileToMove : dataFilesToMove) {
      Assert.assertTrue(
          "Source Data file should point to the old location",
          dataFileToMove.getSource().startsWith(tableLocation));
      Assert.assertTrue(
          "Target Data file should point to the new location",
          dataFileToMove.getTarget().startsWith(targetTableLocation));
    }

    copyTableFiles(result);

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetTableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2", "c3").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testInvalidArgs() {
    CopyTable actions = actions().copyTable(table);

    AssertHelpers.assertThrows(
        "",
        IllegalArgumentException.class,
        "Source prefix('') cannot be empty",
        () -> actions.rewriteLocationPrefix("", null));

    AssertHelpers.assertThrows(
        "",
        IllegalArgumentException.class,
        "Source prefix('null') cannot be empty",
        () -> actions.rewriteLocationPrefix(null, null));

    AssertHelpers.assertThrows(
        "",
        IllegalArgumentException.class,
        "Staging location('') cannot be empty",
        () -> actions.stagingLocation(""));

    AssertHelpers.assertThrows(
        "",
        IllegalArgumentException.class,
        "Staging location('null') cannot be empty",
        () -> actions.stagingLocation(null));

    AssertHelpers.assertThrows(
        "",
        IllegalArgumentException.class,
        "Last copied version('null') cannot be empty",
        () -> actions.lastCopiedVersion(null));

    AssertHelpers.assertThrows(
        "Last copied version cannot be empty",
        IllegalArgumentException.class,
        () -> actions.lastCopiedVersion(" "));

    AssertHelpers.assertThrows(
        "End version cannot be empty",
        IllegalArgumentException.class,
        () -> actions.endVersion(" "));

    AssertHelpers.assertThrows(
        "End version cannot be empty",
        IllegalArgumentException.class,
        () -> actions.endVersion(null));
  }

  protected void checkDataFileNum(long count, CopyTable.Result result) {
    List<String> filesToMove =
        spark
            .read()
            .format("text")
            .load(result.dataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("The rebuilt data file number should be", count, filesToMove.size());
  }

  protected void checkMetadataFileNum(int count, CopyTable.Result result) {
    List<String> filesToMove =
        spark
            .read()
            .format("text")
            .load(result.metadataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("The rebuilt metadata file number should be", count, filesToMove.size());
  }

  protected void checkMetadataFileNum(
      int versionFileCount, int manifestListCount, int manifestFileCount, CopyTable.Result result) {
    checkMetadataFileNum(versionFileCount, manifestListCount, manifestFileCount, 0, result);
  }

  protected void checkMetadataFileNum(
      int versionFileCount,
      int manifestListCount,
      int manifestFileCount,
      int statisticsFileCount,
      CopyTable.Result result) {
    List<String> filesToMove =
        spark
            .read()
            .format("text")
            .load(result.metadataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals(
        "The rebuilt version file number should be",
        versionFileCount,
        filesToMove.stream().filter(f -> f.endsWith(".metadata.json")).count());
    Assert.assertEquals(
        "The rebuilt Manifest list file number should be",
        manifestListCount,
        filesToMove.stream().filter(f -> f.contains("snap-")).count());
    Assert.assertEquals(
        "The rebuilt Manifest file number should be",
        manifestFileCount,
        filesToMove.stream().filter(f -> f.endsWith("-m0.avro")).count());
    Assert.assertEquals(
        "The rebuilt table statistics file number should be",
        statisticsFileCount,
        filesToMove.stream().filter(f -> f.endsWith(".stats")).count());
  }

  private String stagingDir(CopyTable.Result result) {
    String metadataFileListPath = result.metadataFileListLocation();
    return metadataFileListPath.substring(0, metadataFileListPath.lastIndexOf(File.separator));
  }

  protected String newTableLocation() throws IOException {
    return temp.newFolder().toURI().toString();
  }

  private void copyTableFiles(String sourceDir, String targetDir, String stagingDir)
      throws Exception {
    FileUtils.copyDirectory(
        new File(removePrefix(sourceDir) + "/data/"), new File(removePrefix(targetDir) + "/data/"));
    FileUtils.copyDirectory(
        new File(removePrefix(stagingDir)), new File(removePrefix(targetDir) + "/metadata/"));
  }

  // copyTableDataAndMetaFiles handles the case there are specific meta path and data path
  // configured for the table.
  private void copyTableDataAndMetaFiles(
      String sourceDir,
      String targetDir,
      String targetMetaDir,
      String sourceTable,
      String targetTable,
      String stagingDir)
      throws Exception {
    String[] ext = {FileFormat.PARQUET.name().toLowerCase()};
    Collection<File> files = FileUtils.listFiles(new File(removePrefix(sourceDir)), ext, true);
    for (File file : files) {
      String newPath =
          (file.getAbsolutePath())
              .replace(removePrefix(sourceDir), removePrefix(targetDir))
              .replace(sourceTable, targetTable);
      FileUtils.copyFile(file, new File(newPath));
    }

    FileUtils.copyDirectory(
        new File(removePrefix(stagingDir)), new File(removePrefix(targetMetaDir) + "/"));
  }

  private void copyTableFiles(CopyTable.Result result) throws Exception {
    List<PathPair> filesToMove = Lists.newArrayList();
    filesToMove.addAll(readPathPairList(result.dataFileListLocation()));
    filesToMove.addAll(readPathPairList(result.metadataFileListLocation()));

    for (PathPair pathPair : filesToMove) {
      FileUtils.copyFile(
          new File(URI.create(pathPair.getSource())), new File(URI.create(pathPair.getTarget())));
    }
  }

  private String removePrefix(String path) {
    return path.substring(path.lastIndexOf(":") + 1);
  }

  // Metastore table tests
  @Test
  public void testMetadataLocationChange() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, Maps.newHashMap(), "default", "tbl", 1);
    String metadataFilePath = currentMetadata(sourceTable).metadataFileLocation();

    String newMetadataDir = "new-metadata-dir";
    sourceTable
        .updateProperties()
        .set(TableProperties.WRITE_METADATA_LOCATION, sourceTableLocation + newMetadataDir)
        .commit();

    spark.sql("insert into hive.default.tbl values (1, 'AAAAAAAAAA', 'AAAA')");
    sourceTable.refresh();

    // copy table
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(4, 2, 2, result);
    checkDataFileNum(2, result);

    // pick up a version from the old metadata dir as the end version
    CopyTable.Result result1 =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .endVersion(fileName(metadataFilePath))
            .execute();

    checkMetadataFileNum(2, 1, 1, result1);

    // pick up a version from the old metadata dir as the last copied version
    CopyTable.Result result2 =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .lastCopiedVersion(fileName(metadataFilePath))
            .execute();

    checkMetadataFileNum(2, 1, 1, result2);
  }

  @Test
  public void testV2Table() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.delete.mode", "merge-on-read");
    String tableName = "v2tbl";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", tableName, 0);
    // ingest data
    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(3, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .saveAsTable("hive.default." + tableName);
    sourceTable.refresh();

    // generate position delete files
    spark.sql(String.format("delete from hive.default.%s where c1 = 1", tableName));
    sourceTable.refresh();

    List<Object[]> originalData =
        rowsToJava(
            spark
                .read()
                .format("iceberg")
                .load("hive.default." + tableName)
                .sort("c1", "c2", "c3")
                .collectAsList());
    // two rows
    Assert.assertEquals(2, originalData.size());

    // copy table and check the results
    String targetTableLocation = newTableLocation();
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .outputTargetFilePath()
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .execute();

    checkMetadataFileNum(3, 2, 2, result);
    // one data and one metadata file
    checkDataFileNum(2, result);
    // copy the metadata files and data files
    copyTableFiles(result);

    // register table
    String versionFile = fileName(currentMetadata(sourceTable).metadataFileLocation());
    String targetTableName = "copiedV2Table";
    TableIdentifier tableIdentifier = TableIdentifier.of("default", targetTableName);
    catalog.registerTable(tableIdentifier, targetTableLocation + "/metadata/" + versionFile);

    List<Object[]> copiedData =
        rowsToJava(
            spark
                .read()
                .format("iceberg")
                .load("hive.default." + targetTableName)
                .sort("c1", "c2", "c3")
                .collectAsList());

    assertEquals("Rows must match", originalData, copiedData);
  }

  @Test
  public void testInvalidLastCopiedVersionConfigInSnapshotMode() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 3);
    String targetTableLocation = newTableLocation();

    Assert.assertThrows(
        "lastCopiedVersion cannot be configured when snapshotIdToCopy is configured",
        IllegalArgumentException.class,
        () ->
            actions()
                .copyTable(sourceTable)
                .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
                .snapshotIdToCopy(sourceTable.currentSnapshot().snapshotId())
                .lastCopiedVersion("test")
                .execute());
  }

  @Test
  public void testInvalidEndVersionConfigInSnapshotMode() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 3);
    String targetTableLocation = newTableLocation();

    AssertHelpers.assertThrows(
        "endVersion cannot be configured when snapshotIdToCopy is configured",
        IllegalArgumentException.class,
        "Cannot configure lastCopiedVersion and endVersion in copy snapshot mode",
        () ->
            actions()
                .copyTable(sourceTable)
                .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
                .snapshotIdToCopy(sourceTable.currentSnapshot().snapshotId())
                .endVersion("test")
                .execute());
  }

  @Test
  public void testInvalidSnapshotToCopyInSnapshotMode() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2);

    // Fake it that three snapshots have been copied.
    Table targetTable = createTableWithSnapshots(targetTableLocation, 3);

    Assert.assertThrows(
        "cannot copy a snapshot which is older than target table's current snapshot",
        IllegalArgumentException.class,
        () ->
            actions()
                .copyTable(sourceTable)
                .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
                .snapshotIdToCopy(sourceTable.currentSnapshot().snapshotId())
                .targetTable(targetTable)
                .execute());
  }

  private void writeToTable(
      String namespace,
      String tableName,
      int id,
      String c2Str,
      String c3Str,
      String c4Str,
      boolean threeColumn) {
    if (threeColumn) {
      List<ThreeColumnRecord> records =
          Lists.newArrayList(
              new ThreeColumnRecord(id, c2Str, c3Str),
              new ThreeColumnRecord(id + 1, c2Str, c3Str),
              new ThreeColumnRecord(id + 2, c2Str, c3Str));

      Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

      df.select("c1", "c2", "c3")
          .write()
          .format("iceberg")
          .mode("append")
          .saveAsTable("hive." + namespace + "." + tableName);
    } else {
      List<FourColumnRecord> recordsd =
          Lists.newArrayList(
              new FourColumnRecord(id, c2Str, c3Str, c4Str),
              new FourColumnRecord(id + 1, c2Str, c3Str, c4Str),
              new FourColumnRecord(id + 2, c2Str, c3Str, c4Str));

      Dataset<Row> dfd = spark.createDataFrame(recordsd, FourColumnRecord.class).coalesce(1);

      dfd.select("c1", "c2", "c3", "c4")
          .write()
          .format("iceberg")
          .mode("append")
          .saveAsTable("hive." + ns + "." + tableName);
    }
  }

  private List<Object[]> getExpectedData(
      long sId, int size, String c2Str, String c3Str, String c4Str, boolean threeColumns) {
    List<Object[]> list = Lists.newArrayList();

    for (int i = 0; i < size; i++) {
      if (threeColumns) {
        list.add(row(sId + i, c2Str, c3Str));
      } else {
        if (sId + i < 7) {
          list.add(row(sId + i, c2Str, c3Str, null));
        } else {
          list.add(row(sId + i, c2Str, c3Str, c4Str));
        }
      }
    }
    return ImmutableList.copyOf(list);
  }

  private void testCopyV2TableSnapshotCases(
      Map<String, String> properties,
      String tableName,
      String backupTableName,
      String sourceTableLocation,
      String sourceTableMetaLocation,
      String targetTableLocation,
      String targetTableMetaLocation,
      boolean backupOldVersion,
      boolean setLocationInTableCreate)
      throws Exception {

    Table sourceTable =
        createMetastoreTable(
            setLocationInTableCreate ? sourceTableLocation : "", properties, ns, tableName, 0);
    // ingest data
    writeToTable(ns, tableName, 1, "AAAAAAAAAA", "AAAA", null, true);
    writeToTable(ns, tableName, 4, "AAAAAAAAAA", "AAAA", null, true);
    sourceTable.refresh();

    // copy table and check the results
    long snapshotId1 = sourceTable.currentSnapshot().snapshotId();
    CopyTable ct =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .snapshotIdToCopy(snapshotId1)
            .outputTargetFilePath();
    // If table meta location needs to be changed.
    if (sourceTableMetaLocation != null) {
      ct.rewriteMetaLocationPrefix(sourceTableMetaLocation, targetTableMetaLocation);
    }

    CopyTable.Result result = ct.execute();

    checkMetadataFileNum(1, 1, 2, result);
    checkDataFileNum(2, result);

    String versionFile = fileName(currentMetadata(sourceTable).metadataFileLocation());
    TableIdentifier tableIdentifier = TableIdentifier.of(backupNs, backupTableName);

    // copy the metadata files and data files, register the table into backup namespace.
    if (sourceTableMetaLocation != null) {
      copyTableDataAndMetaFiles(
          sourceTableLocation,
          targetTableLocation,
          targetTableMetaLocation,
          tableName,
          backupTableName,
          stagingDir(result));

      catalog.registerTable(tableIdentifier, targetTableMetaLocation + "/" + versionFile);
    } else {
      copyTableFiles(sourceTableLocation, targetTableLocation, stagingDir(result));
      catalog.registerTable(tableIdentifier, targetTableLocation + "/metadata/" + versionFile);
    }

    Table targetTable = catalog.loadTable(tableIdentifier);
    Assert.assertEquals(0, currentMetadata(targetTable).previousFiles().size());
    Assert.assertEquals(1, currentMetadata(targetTable).snapshots().size());
    Assert.assertEquals(snapshotId1, currentMetadata(targetTable).currentSnapshot().snapshotId());

    // verify data rows
    assertEquals(
        "Rows should match",
        getExpectedData(1L, 6, "AAAAAAAAAA", "AAAA", null, true),
        sql("select * from hive.%s.%s ORDER BY c1", backupNs, backupTableName));

    // Change Schema
    sourceTable.updateSchema().addColumn("c4", Types.StringType.get()).commit();
    writeToTable(ns, tableName, 7, "AAAAAAAAAA", "AAAA", "ABCD", false);

    long snapshotId2 = sourceTable.currentSnapshot().snapshotId();
    String versionFile2 = fileName(currentMetadata(sourceTable).metadataFileLocation());
    writeToTable(ns, tableName, 10, "AAAAAAAAAA", "AAAA", "ABCD", false);
    sourceTable.refresh();

    // copy table and check the results
    long snapshotId3 = sourceTable.currentSnapshot().snapshotId();
    CopyTable ct1 =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .snapshotIdToCopy(backupOldVersion ? snapshotId2 : snapshotId3)
            .targetTable(targetTable)
            .outputTargetFilePath();
    if (sourceTableMetaLocation != null) {
      ct1.rewriteMetaLocationPrefix(sourceTableMetaLocation, targetTableMetaLocation);
    }

    CopyTable.Result result1 = ct1.execute();

    checkMetadataFileNum(1, 1, backupOldVersion ? 3 : 4, result1);
    checkDataFileNum(backupOldVersion ? 3 : 4, result1);

    // copy the metadata files and data files
    String versionFile3 = fileName(currentMetadata(sourceTable).metadataFileLocation());
    String versionFileNew = backupOldVersion ? versionFile2 : versionFile3;

    HiveMetaStoreClient msc = new HiveMetaStoreClient(hiveConf);
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        msc.getTable(backupNs, tableIdentifier.name());
    Map<String, String> params = hmsTable.getParameters();

    if (sourceTableMetaLocation != null) {
      copyTableDataAndMetaFiles(
          sourceTableLocation,
          targetTableLocation,
          targetTableMetaLocation,
          tableName,
          backupTableName,
          stagingDir(result1));
      params.put(
          BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
          targetTableMetaLocation + "/" + versionFileNew);
    } else {
      copyTableFiles(sourceTableLocation, targetTableLocation, stagingDir(result1));
      params.put(
          BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
          targetTableLocation + "/metadata/" + versionFileNew);
    }

    hmsTable.setParameters(params);
    msc.alter_table(backupNs, tableIdentifier.name(), hmsTable);
    targetTable.refresh();

    Assert.assertEquals(1, currentMetadata(targetTable).previousFiles().size());
    Assert.assertEquals(2, currentMetadata(targetTable).snapshots().size());
    Assert.assertEquals(
        backupOldVersion ? snapshotId2 : snapshotId3,
        currentMetadata(targetTable).currentSnapshot().snapshotId());
    Assert.assertEquals(snapshotId1, currentMetadata(targetTable).snapshots().get(0).snapshotId());

    int size = backupOldVersion ? 9 : 12;
    assertEquals(
        "Rows should match",
        getExpectedData(1L, size, "AAAAAAAAAA", "AAAA", "ABCD", false),
        sql("select * from hive.%s.%s ORDER BY c1", backupNs, backupTableName));

    // Make sure the old snapshot does not break.
    assertEquals(
        "Rows should match",
        getExpectedData(1L, 6, "AAAAAAAAAA", "AAAA", null, true),
        sql(
            "select * from hive.%s.%s VERSION AS OF %d ORDER BY c1",
            backupNs, backupTableName, snapshotId1));
  }

  @Test
  public void testCopyV2TableSnapshot() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();

    String tableName = "v2tbls";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        null,
        targetTableLocation,
        null,
        false,
        true);
  }

  @Test
  public void testCopyV2TableSnapshotWithConfiguredPaths() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    String sourceTableMetaLocation = newTableLocation();
    String targetTableMetaLocation = newTableLocation();

    String tableName = "v2tblsdatapath";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    properties.put("write.data.path", sourceTableLocation);
    properties.put("write.metadata.path", sourceTableMetaLocation);
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        sourceTableMetaLocation,
        targetTableLocation,
        targetTableMetaLocation,
        false,
        false);
  }

  @Test
  public void testCopyV2TableOldSnapshotWithConfiguredPaths() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    String sourceTableMetaLocation = newTableLocation();
    String targetTableMetaLocation = newTableLocation();

    String tableName = "v2tbloldsdatapath";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    properties.put("write.data.path", sourceTableLocation);
    properties.put("write.metadata.path", sourceTableMetaLocation);
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        sourceTableMetaLocation,
        targetTableLocation,
        targetTableMetaLocation,
        true,
        false);
  }

  @Test
  public void testCopyV2TableCompressedMetaData() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();

    String tableName = "metadatacompressed";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        null,
        targetTableLocation,
        null,
        false,
        true);
  }

  @Test
  public void testCopyV2TableCompressedMetaDataWithConfiguredPaths() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    String sourceTableMetaLocation = newTableLocation();
    String targetTableMetaLocation = newTableLocation();

    String tableName = "metadatacompressedpaths";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    properties.put("write.data.path", sourceTableLocation);
    properties.put("write.metadata.path", sourceTableMetaLocation);
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        sourceTableMetaLocation,
        targetTableLocation,
        targetTableMetaLocation,
        false,
        false);
  }

  @Test
  public void testCopyV2TableEncryption() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();

    String tableName = "encryption";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    properties.put("encryption.table.key.id", MockKMS.MASTER_KEY_NAME1);
    properties.put("encryption.kms.client-impl", "org.apache.iceberg.spark.sql.MockKMS");
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        null,
        targetTableLocation,
        null,
        false,
        true);
  }

  @Test
  public void testCopyV2TableEncryptionWithConfiguredPaths() throws Exception {
    String sourceTableLocation = newTableLocation();
    String targetTableLocation = newTableLocation();
    String sourceTableMetaLocation = newTableLocation();
    String targetTableMetaLocation = newTableLocation();

    String tableName = "encryptionpaths";

    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.object-storage.enabled", "true");
    properties.put("write.data.path", sourceTableLocation);
    properties.put("write.metadata.path", sourceTableMetaLocation);
    properties.put("encryption.table.key.id", MockKMS.MASTER_KEY_NAME1);
    properties.put("encryption.kms.client-impl", "org.apache.iceberg.spark.sql.MockKMS");
    testCopyV2TableSnapshotCases(
        properties,
        tableName,
        tableName,
        sourceTableLocation,
        sourceTableMetaLocation,
        targetTableLocation,
        targetTableMetaLocation,
        false,
        false);
  }

  @Test
  public void testTableWithOneStatisticsFile() throws IOException {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    String tableName = "v2tblwithstats";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", tableName, 1);

    actions().computeTableStats(sourceTable).execute();

    Assert.assertEquals(
        "Should include 1 statistics file after compute stats",
        1,
        sourceTable.statisticsFiles().size());

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(3, 1, 1, 1, result);
    checkDataFileNum(1, result);
  }

  @Test
  public void testTableWithManyStatisticsFiles() throws IOException {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    String namespace = "default";
    String tableName = "v2tblwithmanystats";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, namespace, tableName, 0);

    int iterations = 10;
    for (int i = 0; i < iterations; i++) {
      sql("insert into hive.%s.%s values (%s, 'AAAAAAAAAA', 'AAAA')", namespace, tableName, i);
      sourceTable.refresh();
      actions().computeTableStats(sourceTable).execute();
    }
    sourceTable.refresh();
    Assert.assertEquals(
        "Should include desired count of statistics file in latest table metadata",
        iterations,
        sourceTable.statisticsFiles().size());

    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(iterations * 2 + 1, iterations, iterations, iterations, result);
    checkDataFileNum(iterations, result);
  }

  @Test
  public void testMetadataCompressionWithMetastoreTable() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    Table sourceTable =
        createMetastoreTable(
            sourceTableLocation, properties, "default", "testMetadataCompression", 2);

    TableMetadata currentMetadata = currentMetadata(sourceTable);

    // set the second version as the endVersion
    String endVersion = fileName(currentMetadata.previousFiles().get(1).file());
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .endVersion(endVersion)
            .execute();

    checkMetadataFileNum(4, result);
    checkDataFileNum(1, result);

    // set the first version as the lastCopiedVersion
    String firstVersion = fileName(currentMetadata.previousFiles().get(0).file());
    result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .lastCopiedVersion(firstVersion)
            .execute();

    checkMetadataFileNum(6, result);
    checkDataFileNum(2, result);
  }

  private TableMetadata currentMetadata(Table tbl) {
    return ((HasTableOperations) tbl).operations().current();
  }

  @Test
  public void testDataFileLocationChange() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, Maps.newHashMap(), "default", "tbl1", 1);
    String metadataFilePath = currentMetadata(sourceTable).metadataFileLocation();

    String newMetadataDir = "new-data-dir";
    sourceTable
        .updateProperties()
        .set(TableProperties.OBJECT_STORE_PATH, sourceTableLocation + newMetadataDir)
        .set(TableProperties.OBJECT_STORE_ENABLED, "true")
        .commit();

    spark.sql("insert into hive.default.tbl1 values (1, 'AAAAAAAAAA', 'AAAA')");
    sourceTable.refresh();

    // copy table
    CopyTable.Result result =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, newTableLocation())
            .execute();

    checkMetadataFileNum(4, 2, 2, result);
    checkDataFileNum(2, result);

    // pick up a version with the data file in the old data directory as the end version
    String targetTableLocation = newTableLocation();
    CopyTable.Result result1 =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .endVersion(fileName(metadataFilePath))
            .execute();

    checkMetadataFileNum(2, 1, 1, result1);
    checkDataFileNum(1, result1);
    List<String> filesToMove1 =
        spark
            .read()
            .format("text")
            .load(result1.dataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertTrue(
        "The data file should be in the old data directory.",
        filesToMove1.stream().findFirst().get().startsWith(sourceTableLocation + "data"));

    // pick up a version with the data file in the new data directory as the last copied version
    CopyTable.Result result2 =
        actions()
            .copyTable(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .lastCopiedVersion(fileName(metadataFilePath))
            .execute();

    checkMetadataFileNum(2, 1, 1, result2);
    checkDataFileNum(1, result2);
    List<String> filesToMove2 =
        spark
            .read()
            .format("text")
            .load(result2.dataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertTrue(
        "The data file should be in the new data directory.",
        filesToMove2.stream().findFirst().get().startsWith(sourceTableLocation + newMetadataDir));

    // check if table properties have been modified
    List<String> metadataFilesToMove =
        spark
            .read()
            .format("text")
            .load(result2.metadataFileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    metadataFilesToMove.stream()
        .filter(f -> f.endsWith(".metadata.json"))
        .forEach(
            metadataFile -> {
              StaticTableOperations ops = new StaticTableOperations(metadataFile, sourceTable.io());
              Table targetStaticTable = new BaseTable(ops, metadataFile);
              if (targetStaticTable.properties().containsKey(TableProperties.OBJECT_STORE_PATH)) {
                Assert.assertTrue(
                    "The write.object-storage.path should be modified with the target table location.",
                    targetStaticTable
                        .properties()
                        .get(TableProperties.OBJECT_STORE_PATH)
                        .startsWith(targetTableLocation));
              }
            });
  }

  private Table createMetastoreTable(
      String location,
      Map<String, String> properties,
      String namespace,
      String tableName,
      int snapshotNumber) {
    spark.conf().set("spark.sql.catalog.hive", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.hive.type", "hive");
    spark.conf().set("spark.sql.catalog.hive.default-namespace", "default");
    spark.conf().set("spark.sql.catalog.hive.cache-enabled", "false");

    StringBuilder propertiesStr = new StringBuilder();
    properties.forEach((k, v) -> propertiesStr.append("'" + k + "'='" + v + "',"));
    String tblProperties =
        propertiesStr.substring(0, propertiesStr.length() > 0 ? propertiesStr.length() - 1 : 0);

    sql("DROP TABLE IF EXISTS hive.%s.%s", namespace, tableName);
    if (tblProperties.isEmpty()) {
      String sqlStr =
          String.format(
              "CREATE TABLE hive.%s.%s (c1 bigint, c2 string, c3 string)", namespace, tableName);
      if (!location.isEmpty()) {
        sqlStr = String.format("%s USING iceberg LOCATION '%s'", sqlStr, location);
      }
      sql(sqlStr);
    } else {
      String sqlStr =
          String.format(
              "CREATE TABLE hive.%s.%s (c1 bigint, c2 string, c3 string)", namespace, tableName);
      if (!location.isEmpty()) {
        sqlStr = String.format("%s USING iceberg LOCATION '%s'", sqlStr, location);
      }

      sqlStr = String.format("%s TBLPROPERTIES (%s)", sqlStr, tblProperties);
      sql(sqlStr);
    }

    for (int i = 0; i < snapshotNumber; i++) {
      sql("insert into hive.%s.%s values (%s, 'AAAAAAAAAA', 'AAAA')", namespace, tableName, i);
    }
    return catalog.loadTable(TableIdentifier.of(namespace, tableName));
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private List<CopyTableSparkAction.PathPair> readPathPairList(String path) {
    Encoder<PathPair> encoder = Encoders.bean(PathPair.class);
    return spark
        .read()
        .format("csv")
        .schema(encoder.schema())
        .load(path)
        .as(encoder)
        .collectAsList();
  }

  private String stagingWithScheme() {
    String location = staging.getRoot().getAbsolutePath();
    return Paths.get(location).toAbsolutePath().toUri().toString();
  }
}
