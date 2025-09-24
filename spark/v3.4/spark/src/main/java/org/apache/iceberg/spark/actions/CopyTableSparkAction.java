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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestLists;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableMetadataUtil;
import org.apache.iceberg.actions.BaseCopyTableActionResult;
import org.apache.iceberg.actions.CopyTable;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyTableSparkAction extends BaseSparkAction<CopyTableSparkAction>
    implements CopyTable {

  private static final Logger LOG = LoggerFactory.getLogger(CopyTableSparkAction.class);
  private static final String DATA_FILE_LIST_DIR = "data-file-list-to-move";
  private static final String METADATA_FILE_LIST_DIR = "metadata-file-list-to-move";

  private final Table table;
  private ExecutorService executorService = null;
  private final Set<PathPair> metadataFilesToMove = Collections.synchronizedSet(Sets.newHashSet());
  private final Set<String> manifestFilePaths = Collections.synchronizedSet(Sets.newHashSet());
  private final Map<Long, List<ManifestFile>> manifestFilesInAllSnapshots =
      Collections.synchronizedMap(Maps.newHashMap());
  private final Set<ManifestFile> manifestFilesToRewrite =
      Collections.synchronizedSet(Sets.newHashSet());
  private String dataFileListPath = null;
  private String metadataFileListPath = null;

  private final Map<String, String> prefixMappings = Maps.newConcurrentMap();
  private final Map<String, String> prefixMetaMappings = Maps.newConcurrentMap();
  private long snapshotId = 0L;
  private String startVersion = "";
  private String endVersion = "";
  private String stagingDir = "";
  private Table targetTable = null;
  private boolean outputTargetFilePath = false;

  private Table startStaticTable = null;
  private Table endStaticTable = null;

  CopyTableSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected CopyTableSparkAction self() {
    return this;
  }

  @Override
  public CopyTableSparkAction rewriteLocationPrefix(String sPrefix, String tPrefix) {
    Preconditions.checkArgument(
        sPrefix != null && !sPrefix.isEmpty(), "Source prefix('%s') cannot be empty.", sPrefix);
    Preconditions.checkArgument(
        tPrefix != null && !tPrefix.isEmpty(), "Target prefix('%s') cannot be empty.", tPrefix);
    this.prefixMappings.put(sPrefix, tPrefix);
    this.prefixMetaMappings.put(sPrefix, tPrefix);
    return this;
  }

  @Override
  public CopyTableSparkAction rewriteMetaLocationPrefix(String sPrefix, String tPrefix) {
    Preconditions.checkArgument(
        (sPrefix != null) && !sPrefix.isEmpty(),
        "Source meta prefix('%s') cannot be empty.",
        sPrefix);
    Preconditions.checkArgument(
        (tPrefix != null) && !tPrefix.isEmpty(),
        "Target meta prefix('%s') cannot be empty.",
        sPrefix);
    this.prefixMetaMappings.put(sPrefix, tPrefix);
    return this;
  }

  @Override
  public CopyTableSparkAction snapshotIdToCopy(long sId) {
    this.snapshotId = sId;
    return this;
  }

  @Override
  public CopyTableSparkAction lastCopiedVersion(String sVersion) {
    Preconditions.checkArgument(
        sVersion != null && !sVersion.trim().isEmpty(),
        "Last copied version('%s') cannot be empty.",
        sVersion);
    this.startVersion = sVersion;
    return this;
  }

  @Override
  public CopyTableSparkAction endVersion(String eVersion) {
    Preconditions.checkArgument(
        eVersion != null && !eVersion.trim().isEmpty(),
        "End version('%s') cannot be empty.",
        eVersion);
    this.endVersion = eVersion;
    return this;
  }

  @Override
  public CopyTableSparkAction stagingLocation(String stagingLocation) {
    Preconditions.checkArgument(
        stagingLocation != null && !stagingLocation.isEmpty(),
        "Staging location('%s') cannot be empty.",
        stagingLocation);
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public CopyTableSparkAction targetTable(Table tgtTable) {
    this.targetTable = tgtTable;
    return this;
  }

  @Override
  public CopyTable outputTargetFilePath() {
    this.outputTargetFilePath = true;
    return this;
  }

  @Override
  public CopyTable executeWith(ExecutorService service) {
    this.executorService = service;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    JobGroupInfo info = newJobGroupInfo("COPY-TABLE", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    rebuildMetadata();
    return new BaseCopyTableActionResult(
        dataFileListPath, metadataFileListPath, fileName(endVersion));
  }

  private void validateInputs() {
    if (isCopySnapshotMode()) {
      // Make sure that lastCopiedVersion and endVersion are not configured
      if (!startVersion.isEmpty() || !endVersion.isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot configure lastCopiedVersion and endVersion in copy snapshot mode");
      }
      validateAndSetSnapshotVersion();
    } else {
      validateAndSetEndVersion();
    }

    // endStaticTable has the version file for a specific snapshot id
    endStaticTable = newStaticTable(endVersion, table);

    if (!isCopySnapshotMode()) {

      TableMetadata tableMetadata = ((HasTableOperations) endStaticTable).operations().current();

      validateAndSetStartVersion(tableMetadata);

      if (fileExist(startVersion)) {
        startStaticTable = newStaticTable(startVersion, table);
      }
    }

    if (stagingDir.isEmpty()) {
      stagingDir = getMetadataLocation(table) + "copy-table-staging-" + UUID.randomUUID() + "/";
    } else if (!stagingDir.endsWith("/")) {
      stagingDir = stagingDir + "/";
    }
  }

  private void validateAndSetSnapshotVersion() {
    endVersion = getAndValidateEndVersionFromSnapshotId();
    startVersion = endVersion;
  }

  private String getAndValidateEndVersionFromSnapshotId() {
    String resultMetadataFileLocation = null;
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    Snapshot snap = tableMetadata.snapshot(snapshotId);

    if (snap == null) {
      throw new IllegalArgumentException(
          "Cannot find the snapshot "
              + snapshotId
              + " in the source table. "
              + "Please make sure the snapshot exists in source table.");
    }

    // If target table is configured, make sure that input snapshot is later than
    // target table's current snapshot. Copying a old snapshot than target table's current
    // snapshot is not supported.
    if ((targetTable != null)
        && (targetTable.currentSnapshot().sequenceNumber() >= snap.sequenceNumber())) {
      throw new IllegalArgumentException(
          "Snapshot "
              + snapshotId
              + "to be copied is older than the current snapshot in target table, which is not supported");
    }

    // Copy the latest snapshot
    if (snap == table.currentSnapshot()) {
      return tableMetadata.metadataFileLocation();
    }

    // Find the snapshot in the snapshot history.
    for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
      if (metadataLogEntry.timestampMillis() == snap.timestampMillis()) {
        resultMetadataFileLocation = metadataLogEntry.file();
        break;
      }
    }

    Preconditions.checkArgument(
        fileExist(resultMetadataFileLocation),
        "Cannot find the snapshot('%s') in the current table",
        snapshotId);
    return resultMetadataFileLocation;
  }

  private void validateAndSetEndVersion() {
    if (endVersion.isEmpty()) {
      endVersion = currentMetadataPath(table);
    } else {
      TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
      if (versionInFilePath(tableMetadata.metadataFileLocation(), endVersion)) {
        endVersion = tableMetadata.metadataFileLocation();
      }
      for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
        if (versionInFilePath(metadataLogEntry.file(), endVersion)) {
          endVersion = metadataLogEntry.file();
          break;
        }
      }

      Preconditions.checkArgument(
          fileExist(endVersion),
          "Cannot find the end version('%s') in the current version " + "files",
          endVersion);
    }
  }

  private void validateAndSetStartVersion(TableMetadata tableMetadata) {
    if (startVersion.isEmpty()) {
      if (targetTable == null) {
        LOG.warn("No input of the start version. Will do a full copy.");
      } else {
        String tgtTableCurrentVersion = fileName(currentMetadataPath(targetTable));

        for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
          if (metadataLogEntry.file().endsWith(tgtTableCurrentVersion)) {
            startVersion = metadataLogEntry.file();
            break;
          }
        }

        if (fileNotExist(startVersion)) {
          throw new IllegalArgumentException(
              "Cannot find the current version of target table in the source table. "
                  + "Please make sure the target table is a subset of source table.");
        }
      }
    } else {
      for (MetadataLogEntry metadataLogEntry : tableMetadata.previousFiles()) {
        if (versionInFilePath(metadataLogEntry.file(), startVersion)) {
          startVersion = metadataLogEntry.file();
          break;
        }
      }

      Preconditions.checkArgument(
          fileExist(startVersion), "Start version('%s') is NOT valid.", startVersion);

      if (targetTable != null
          && !fileName(startVersion).equals(fileName(currentMetadataPath(targetTable)))) {
        throw new IllegalArgumentException(
            "The start version isn't the current version of the target table. "
                + "Please make sure the target table is a subset of source table.");
      }
    }
  }

  private boolean versionInFilePath(String path, String version) {
    return fileName(path).equals(version);
  }

  private String jobDesc() {
    if (startVersion.isEmpty()) {
      return String.format(
          "Replacing path prefixes '%s' in the metadata files of table %s," + "up to version '%s'.",
          this.prefixMappings, table.name(), endVersion);
    } else {
      return String.format(
          "Replacing path prefixes '%s' in the metadata files of table %s,"
              + "from version '%s' to '%s'.",
          this.prefixMappings, table.name(), startVersion, endVersion);
    }
  }

  /**
   * Here are steps: 1. rebuild version files 2. rebuild manifest files 3. rebuild manifest list
   * files 4. get all data files need to move
   */
  private void rebuildMetadata() {
    TableMetadata tableMetadata = ((HasTableOperations) endStaticTable).operations().current();

    // rebuild version files
    Set<Long> allSnapshotIds = rewriteVersionFiles(tableMetadata);

    // For copySnapshot mode, it will only have the snapshot to copy.
    Set<Long> diffSnapshotIds = getDiffSnapshotIds(allSnapshotIds);

    // get all manifest file paths need to rewrite
    List<String> manifestFilePathToMove = manifestFilesToMove(diffSnapshotIds);
    manifestFilePaths.addAll(manifestFilePathToMove);

    Set<Snapshot> validSnapshots =
        isCopySnapshotMode()
            ? Sets.newHashSet(tableMetadata.currentSnapshot())
            : Sets.difference(snapshotSet(endVersion), snapshotSet(startVersion));

    // prepare manifest files
    Tasks.foreach(validSnapshots)
        .noRetry()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            snapshot -> {
              List<ManifestFile> manifestFiles = manifestFilesInSnapshot(snapshot);
              manifestFilesInAllSnapshots.put(snapshot.snapshotId(), manifestFiles);
              for (ManifestFile manifestFile : manifestFiles) {
                if (manifestFilePaths.contains(manifestFile.path())) {
                  manifestFilesToRewrite.add(manifestFile);
                }
              }
            });

    // rebuild manifest files
    Pair<Set<PathPair>, Map<String, Long>> dataFilesToMovePair =
        rewriteManifests(diffSnapshotIds, tableMetadata);

    Set<PathPair> dataFilesToMove = dataFilesToMovePair.first();
    Map<String, Long> rewrittenManifestSizesMap = dataFilesToMovePair.second();

    // rebuild manifest-list files
    Tasks.foreach(validSnapshots)
        .noRetry()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            snapshot ->
                rewriteManifestList(
                    snapshot, tableMetadata.formatVersion(), rewrittenManifestSizesMap));

    metadataFileListPath = saveFileList(metadataFilesToMove, METADATA_FILE_LIST_DIR);
    dataFileListPath = saveFileList(dataFilesToMove, DATA_FILE_LIST_DIR);
  }

  private String saveFileList(Set<PathPair> filesToMove, String fileListDir) {
    List<PathPair> fileList = Lists.newArrayList();
    fileList.addAll(filesToMove);
    Dataset<PathPair> fileListDataset =
        spark().createDataset(fileList, Encoders.bean(PathPair.class));
    String fileListPath = stagingDir + fileListDir;
    if (outputTargetFilePath) {
      fileListDataset
          .repartition(1)
          .write()
          .mode(SaveMode.Overwrite)
          .format("csv")
          .save(fileListPath);
    } else {
      fileListDataset
          .drop("target")
          .repartition(1)
          .write()
          .mode(SaveMode.Overwrite)
          .format("text")
          .save(fileListPath);
    }
    return fileListPath;
  }

  private boolean isCopySnapshotMode() {
    return snapshotId != 0L;
  }

  private Set<Long> getDiffSnapshotIds(Set<Long> allSnapshotIds) {
    Set<Long> snapshotIdsInStartVersion = Sets.newHashSet();
    if (isCopySnapshotMode()) {
      snapshotIdsInStartVersion.add(snapshotId);
      return snapshotIdsInStartVersion;
    } else if (startStaticTable != null) {
      startStaticTable
          .snapshots()
          .forEach(snapshot -> snapshotIdsInStartVersion.add(snapshot.snapshotId()));
    }
    return Sets.difference(allSnapshotIds, snapshotIdsInStartVersion);
  }

  private Set<Long> rewriteVersionFiles(TableMetadata metadata) {
    Set<Long> allSnapshotIds = Sets.newHashSet();

    TableMetadata newMetadata;
    if (isCopySnapshotMode()) {
      TableMetadata targetTableMetaData =
          (targetTable != null) ? ((HasTableOperations) targetTable).operations().current() : null;
      long currentSnapshotId = metadata.currentSnapshot().snapshotId();
      allSnapshotIds.add(currentSnapshotId);
      newMetadata = TableMetadataUtil.addMetaDataFromTargetTable(metadata, targetTableMetaData);
    } else {
      metadata.snapshots().forEach(snapshot -> allSnapshotIds.add(snapshot.snapshotId()));
      newMetadata = metadata;
    }
    rewriteVersionFile(newMetadata, endVersion);

    // For copy snapshot mode, do not rewrite previous files.
    if (isCopySnapshotMode()) {
      return allSnapshotIds;
    }

    List<MetadataLogEntry> versions = newMetadata.previousFiles();

    // iteratively determine versioned file scope from latest until startVersion
    List<String> versionFilePaths = Lists.newArrayList();
    for (int i = versions.size() - 1; i >= 0; i--) {
      String versionFilePath = versions.get(i).file();
      if (versionFilePath.equals(startVersion)) {
        break;
      }
      Preconditions.checkArgument(
          fileExist(versionFilePath),
          String.format("Version file %s doesn't exist", versionFilePath));
      versionFilePaths.add(versionFilePath);
    }

    Tasks.foreach(versionFilePaths)
        .noRetry()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            versionFilePath -> {
              TableMetadata versionedMeta =
                  new StaticTableOperations(versionFilePath, table.io()).current();
              versionedMeta
                  .snapshots()
                  .forEach(snapshot -> allSnapshotIds.add(snapshot.snapshotId()));
              rewriteVersionFile(versionedMeta, versionFilePath);
            });
    return allSnapshotIds;
  }

  private Set<Snapshot> snapshotSet(String metadataPath) {
    Set<Snapshot> snapshots = Sets.newHashSet();
    if (!metadataPath.isEmpty()) {
      StaticTableOperations ops = new StaticTableOperations(metadataPath, table.io());
      TableMetadata metadata = ops.current();
      snapshots.addAll(metadata.snapshots());
    }
    return snapshots;
  }

  private void rewriteVersionFile(TableMetadata metadata, String versionFilePath) {
    String stagingPath = stagingPath(versionFilePath, stagingDir);
    TableMetadata newTableMetadata =
        TableMetadataUtil.replacePaths(metadata, prefixMetaMappings, prefixMappings, table.io());
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    metadataFilesToMove.add(
        new PathPair(stagingPath, TableMetadataUtil.newPath(versionFilePath, prefixMetaMappings)));

    validateAndStageStatisticsFiles(
        metadata.formatVersion(), metadata.statisticsFiles(), newTableMetadata.statisticsFiles());
  }

  private void validateAndStageStatisticsFiles(
      int formatVersion, List<StatisticsFile> beforeStats, List<StatisticsFile> afterStats) {
    if (beforeStats == null || beforeStats.isEmpty()) {
      return;
    }

    Preconditions.checkArgument(formatVersion <= 2, "Statistics files with v3 is not supported");
    Preconditions.checkArgument(
        beforeStats.size() == afterStats.size(),
        "Before and after path rewrite, statistics file count should be same");

    for (int i = 0; i < beforeStats.size(); i++) {
      StatisticsFile before = beforeStats.get(i);
      StatisticsFile after = afterStats.get(i);
      Preconditions.checkArgument(
          before.fileSizeInBytes() == after.fileSizeInBytes(),
          "Before and after path rewrite, statistics file size should be same");
      metadataFilesToMove.add(new PathPair(before.path(), after.path()));
    }
  }

  private void rewriteManifestList(
      Snapshot snapshot, int formatVersion, Map<String, Long> rewrittenManifestSizesMap) {
    String manifestListPath = snapshot.manifestListLocation();
    String stagingPath = stagingPath(manifestListPath, stagingDir);
    try (FileIO fileIO = table.io();
        FileAppender<ManifestFile> writer =
            ManifestLists.write(
                formatVersion,
                fileIO.newOutputFile(stagingPath),
                snapshot.snapshotId(),
                snapshot.parentId(),
                snapshot.sequenceNumber())) {

      for (ManifestFile file : manifestFilesInAllSnapshots.get(snapshot.snapshotId())) {
        ManifestFile newFile = file.copy();
        Map.Entry<String, String> prefixMap =
            TableMetadataUtil.lookupPrefixMappings(newFile.path(), prefixMetaMappings);
        if (prefixMap != null) {
          String sourceMetaPrefix = prefixMap.getKey();
          String targetMetaPrefix = prefixMap.getValue();
          ((StructLike) newFile)
              .set(
                  0, TableMetadataUtil.newPath(newFile.path(), sourceMetaPrefix, targetMetaPrefix));

          String manifestFilePath = file.path();
          String manifestFileName = getFileName(manifestFilePath);

          if (rewrittenManifestSizesMap.containsKey(manifestFileName)) {
            ((StructLike) newFile).set(1, rewrittenManifestSizesMap.get(manifestFileName));
          } else {
            // This manifest was NOT rewritten but path changed - calculate new size
            InputFile inputFile = fileIO.newInputFile(newFile.path());
            if (inputFile.exists()) {
              long actualSize = inputFile.getLength();
              ((StructLike) newFile).set(1, actualSize);
            }
          }
        }
        writer.add(newFile);

        // need to get the ManifestFile object for manifest file rewriting
        if (manifestFilePaths.contains(file.path())) {
          metadataFilesToMove.add(
              new PathPair(stagingPath(file.path(), stagingDir), newFile.path()));
        }
      }

      metadataFilesToMove.add(
          new PathPair(
              stagingPath, TableMetadataUtil.newPath(manifestListPath, prefixMetaMappings)));
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to rewrite the manifest list file " + manifestListPath, e);
    }
  }

  private static String getFileName(String manifestFilePath) {
    return manifestFilePath.substring(manifestFilePath.lastIndexOf('/') + 1);
  }

  private List<ManifestFile> manifestFilesInSnapshot(Snapshot snapshot) {
    String path = snapshot.manifestListLocation();
    List<ManifestFile> manifestFiles = Lists.newLinkedList();
    try {
      manifestFiles = ManifestLists.read(table.io().newInputFile(path));
    } catch (RuntimeIOException e) {
      LOG.warn("Failed to read manifest list {}", path, e);
    }
    return manifestFiles;
  }

  private List<String> manifestFilesToMove(Set<Long> diffSnapshotIds) {
    try {
      if (isCopySnapshotMode()) {
        Dataset<Row> lastVersionFiles = manifestDS(endStaticTable, diffSnapshotIds).select("path");
        return lastVersionFiles.distinct().as(Encoders.STRING()).collectAsList();
      } else {
        Dataset<Row> lastVersionFiles = manifestDS(endStaticTable).select("path");
        if (startStaticTable == null) {
          return lastVersionFiles.distinct().as(Encoders.STRING()).collectAsList();
        } else {
          return lastVersionFiles
              .distinct()
              .filter(functions.column("added_snapshot_id").isInCollection(diffSnapshotIds))
              .as(Encoders.STRING())
              .collectAsList();
        }
      }
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          "Failed to build the manifest files dataframe, the end version you are "
              + "trying to copy may contain invalid snapshots, please use the younger version which doesn't have invalid "
              + "snapshots",
          e);
    }
  }

  /** Rewrite manifest files in a distributed manner and return rewritten data files path pairs. */
  private Pair<Set<PathPair>, Map<String, Long>> rewriteManifests(
      Set<Long> deltaSnapshotIds, TableMetadata tableMetadata) {
    if (manifestFilesToRewrite.isEmpty()) {
      return Pair.of(Sets.newHashSet(), Maps.newHashMap());
    }

    Encoder<ManifestFile> manifestFileEncoder = Encoders.javaSerialization(ManifestFile.class);
    Dataset<ManifestFile> manifestDS =
        spark().createDataset(Lists.newArrayList(manifestFilesToRewrite), manifestFileEncoder);

    Broadcast<Table> serializableTable = sparkContext().broadcast(SerializableTable.copyOf(table));
    Broadcast<Map<Integer, PartitionSpec>> specsById =
        sparkContext().broadcast(tableMetadata.specsById());
    Broadcast<Set<Long>> serializableDeltaSnapshotIds =
        sparkContext().broadcast(Sets.newHashSet(deltaSnapshotIds));

    List<PathPairWithManifestSize> dataFiles =
        manifestDS
            .repartition(manifestFilesToRewrite.size())
            .mapPartitions(
                toManifests(
                    serializableTable,
                    serializableDeltaSnapshotIds,
                    stagingDir,
                    tableMetadata.formatVersion(),
                    specsById,
                    prefixMappings),
                Encoders.bean(PathPairWithManifestSize.class))
            .collectAsList();

    final Map<String, Long> rewrittenManifestSizesMap =
        dataFiles.stream()
            .collect(
                Collectors.toMap(
                    PathPairWithManifestSize::getFileName,
                    PathPairWithManifestSize::getFileSize,
                    (existing, replacement) -> {
                      // During incremental copy, same manifest can appear multiple times
                      // Verify sizes are consistent and use the existing value
                      if (!existing.equals(replacement)) {
                        LOG.warn(
                            "Manifest file {} has inconsistent sizes: {} vs {}",
                            "unknown",
                            existing,
                            replacement);
                      }
                      return existing;
                    }));

    LOG.info("Prepared rewrittenManifestSizesMap: {}", rewrittenManifestSizesMap);

    // duplicates are expected here as the same data file can have different statuses
    // (e.g. added and deleted)
    return Pair.of(Sets.newHashSet(dataFiles), rewrittenManifestSizesMap);
  }

  private static MapPartitionsFunction<ManifestFile, PathPairWithManifestSize> toManifests(
      Broadcast<Table> tableBroadcast,
      Broadcast<Set<Long>> deltaSnapshotIds,
      String stagingLocation,
      int format,
      Broadcast<Map<Integer, PartitionSpec>> specsById,
      Map<String, String> prefixMappings) {
    return rows -> {
      List<PathPairWithManifestSize> files = Lists.newArrayList();
      while (rows.hasNext()) {
        ManifestFile manifestFile = rows.next();
        switch (manifestFile.content()) {
          case DATA:
            files.addAll(
                writeDataManifest(
                    manifestFile,
                    tableBroadcast,
                    deltaSnapshotIds,
                    stagingLocation,
                    format,
                    specsById,
                    prefixMappings));
            break;
          case DELETES:
            files.addAll(
                writeDeleteManifest(
                    manifestFile,
                    tableBroadcast,
                    stagingLocation,
                    format,
                    specsById,
                    prefixMappings));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported manifest type: " + manifestFile.content());
        }
      }
      return files.iterator();
    };
  }

  private static List<PathPairWithManifestSize> writeDataManifest(
      ManifestFile manifestFile,
      Broadcast<Table> tableBroadcast,
      Broadcast<Set<Long>> snapshotIds,
      String stagingLocation,
      int format,
      Broadcast<Map<Integer, PartitionSpec>> specsByIdBroadcast,
      Map<String, String> prefixMappings)
      throws IOException {
    String stagingPath = stagingPath(manifestFile.path(), stagingLocation);
    FileIO io = tableBroadcast.getValue().io();
    OutputFile outputFile = io.newOutputFile(stagingPath);
    Map<Integer, PartitionSpec> specsById = specsByIdBroadcast.getValue();
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());
    Set<Long> deltaSnapshotIds = snapshotIds.value();

    List<PathPair> pathPairList;
    Pair<String, Long> rewrittenManifestFileSize;

    try (ManifestWriter<DataFile> writer =
            ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DataFile> reader =
            ManifestFiles.read(manifestFile, io, specsById).select(Arrays.asList("*"))) {
      pathPairList =
          StreamSupport.stream(reader.entries().spliterator(), false)
              .map(entry -> newDataFile(entry, deltaSnapshotIds, spec, prefixMappings, writer))
              .filter(PathPair::valid)
              .collect(Collectors.toList());
    } finally {
      rewrittenManifestFileSize = getRewrittenManifestFileSize(outputFile);
    }

    return pathPairList.stream()
        .map(
            pathPair ->
                new PathPairWithManifestSize(
                    pathPair,
                    rewrittenManifestFileSize.first(),
                    rewrittenManifestFileSize.second()))
        .collect(Collectors.toList());
  }

  private static Pair<String, Long> getRewrittenManifestFileSize(OutputFile outputFile) {
    InputFile inputFile = outputFile.toInputFile();
    String inputFileLocation = inputFile.location();
    String manifestFileName = getFileName(inputFileLocation);
    return Pair.of(manifestFileName, inputFile.getLength());
  }

  private static PathPair newDataFile(
      ManifestEntry<DataFile> entry,
      Set<Long> snapshotIds,
      PartitionSpec spec,
      Map<String, String> prefixMappings,
      ManifestWriter<DataFile> writer) {
    DataFile dataFile = entry.file();
    String sourceDataFilePath = dataFile.path().toString();
    Map.Entry<String, String> prefixMap =
        TableMetadataUtil.lookupPrefixMappings(sourceDataFilePath, prefixMappings);
    if (prefixMap != null) {
      String sourcePrefix = prefixMap.getKey();
      String targetPrefix = prefixMap.getValue();
      String targetDataFilePath =
          TableMetadataUtil.newPath(sourceDataFilePath, sourcePrefix, targetPrefix);
      dataFile = DataFiles.builder(spec).copy(entry.file()).withPath(targetDataFilePath).build();
    }
    appendEntryWithFile(entry, writer, dataFile);
    // keep the following entries in metadata but exclude them from copyPlan
    // 1) deleted data files
    // 2) entries not changed by snapshotIds
    if (entry.isLive() && snapshotIds.contains(entry.snapshotId())) {
      return new PathPair(sourceDataFilePath, dataFile.path().toString());
    } else {
      return new PathPair();
    }
  }

  private static List<PathPairWithManifestSize> writeDeleteManifest(
      ManifestFile manifestFile,
      Broadcast<Table> tableBroadcast,
      String stagingLocation,
      int format,
      Broadcast<Map<Integer, PartitionSpec>> specsByIdBroadcast,
      Map<String, String> prefixMappings)
      throws IOException {
    String stagingPath = stagingPath(manifestFile.path(), stagingLocation);
    FileIO io = tableBroadcast.getValue().io();
    OutputFile outputFile = io.newOutputFile(stagingPath);
    Map<Integer, PartitionSpec> specsById = specsByIdBroadcast.getValue();
    PartitionSpec spec = specsById.get(manifestFile.partitionSpecId());

    List<PathPair> pathPairList;
    Pair<String, Long> rewrittenManifestFileSize;

    try (ManifestWriter<DeleteFile> writer =
            ManifestFiles.writeDeleteManifest(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifestFile, io, specsById)
                .select(Arrays.asList("*"))) {
      pathPairList =
          StreamSupport.stream(reader.entries().spliterator(), false)
              .map(
                  entry -> {
                    try {
                      return newDeleteFile(
                          entry, io, spec, prefixMappings, stagingLocation, writer);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .filter(PathPair::valid)
              .collect(Collectors.toList());
    } finally {
      rewrittenManifestFileSize = getRewrittenManifestFileSize(outputFile);
    }

    return pathPairList.stream()
        .map(
            pathPair ->
                new PathPairWithManifestSize(
                    pathPair,
                    rewrittenManifestFileSize.first(),
                    rewrittenManifestFileSize.second()))
        .collect(Collectors.toList());
  }

  private static PathPair newDeleteFile(
      ManifestEntry<DeleteFile> entry,
      FileIO io,
      PartitionSpec spec,
      Map<String, String> prefixMappings,
      String stagingLocation,
      ManifestWriter<DeleteFile> writer)
      throws IOException {

    DeleteFile file = entry.file();

    switch (file.content()) {
      case POSITION_DELETES:
        DeleteFile posDeleteFile =
            rewritePositionDeleteFile(io, file, spec, stagingLocation, prefixMappings);
        String targetDeleteFilePath = newPath(file.path().toString(), prefixMappings);
        DeleteFile movedFile =
            FileMetadata.deleteFileBuilder(spec)
                .copy(posDeleteFile)
                .withPath(targetDeleteFilePath)
                .build();
        appendEntryWithFile(entry, writer, movedFile);
        // Keep non-live entry but exclude deleted position delete files as part of copyPlan
        if (entry.isLive()) {
          return new PathPair(posDeleteFile.path().toString(), movedFile.path().toString());
        } else {
          return new PathPair();
        }
      case EQUALITY_DELETES:
        DeleteFile eqDeleteFile = newEqualityDeleteFile(file, spec, prefixMappings);
        appendEntryWithFile(entry, writer, eqDeleteFile);
        // Keep non-live entry but exclude deleted equality delete files as part of copyPlan
        if (entry.isLive()) {
          // No need to rewrite equality delete files as they do not contain absolute file paths.
          return new PathPair(file.path().toString(), eqDeleteFile.path().toString());
        } else {
          return new PathPair();
        }
      default:
        throw new UnsupportedOperationException("Unsupported delete file type: " + file.content());
    }
  }

  private static DeleteFile newEqualityDeleteFile(
      DeleteFile file, PartitionSpec spec, Map<String, String> prefixMappings) {
    String path = file.path().toString();
    Map.Entry<String, String> prefixMap =
        TableMetadataUtil.lookupPrefixMappings(path, prefixMappings);
    if (prefixMap == null) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix map: "
              + prefixMappings
              + " but was "
              + path);
    }
    int[] equalityFieldIds = file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
    String newPath = newPath(path, prefixMappings);
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldIds)
        .copy(file)
        .withPath(newPath)
        .withSplitOffsets(file.splitOffsets())
        .build();
  }

  private static PositionDelete newPositionDeleteRecord(
      Record record, Map<String, String> prefixMappings) {
    PositionDelete delete = PositionDelete.create();
    String oldPath = (String) record.get(0);
    String newPath = oldPath;
    Map.Entry<String, String> mapPrefix =
        TableMetadataUtil.lookupPrefixMappings(oldPath, prefixMappings);
    if (mapPrefix != null) {
      String sourcePrefix = mapPrefix.getKey();
      String targetPrefix = mapPrefix.getValue();
      newPath = newPath(oldPath, sourcePrefix, targetPrefix);
    }
    delete.set(newPath, (Long) record.get(1), record.get(2));
    return delete;
  }

  private static DeleteFile rewritePositionDeleteFile(
      FileIO io,
      DeleteFile current,
      PartitionSpec spec,
      String stagingLocation,
      Map<String, String> prefixMappings)
      throws IOException {
    String path = current.path().toString();
    Map.Entry<String, String> mapPrefix =
        TableMetadataUtil.lookupPrefixMappings(path, prefixMappings);
    if (mapPrefix == null) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix mappings: "
              + prefixMappings
              + " but was "
              + path);
    }
    String newPath = stagingPath(path, stagingLocation);

    OutputFile targetFile = io.newOutputFile(newPath);
    InputFile sourceFile = io.newInputFile(path);

    try (CloseableIterable<Record> reader =
        positionDeletesReader(sourceFile, current.format(), spec)) {
      Record record = null;
      Schema rowSchema = null;
      CloseableIterator<Record> recordIt = reader.iterator();

      if (recordIt.hasNext()) {
        record = recordIt.next();
        rowSchema = record.get(2) != null ? spec.schema() : null;
      }

      PositionDeleteWriter<Record> writer =
          positionDeletesWriter(targetFile, current.format(), spec, current.partition(), rowSchema);

      try {
        if (record != null) {
          writer.write(newPositionDeleteRecord(record, prefixMappings));
        }

        while (recordIt.hasNext()) {
          record = recordIt.next();
          writer.write(newPositionDeleteRecord(record, prefixMappings));
        }
      } finally {
        writer.close();
      }
      return writer.toDeleteFile();
    }
  }

  private static CloseableIterable<Record> positionDeletesReader(
      InputFile inputFile, FileFormat format, PartitionSpec spec) throws IOException {
    Schema deleteSchema = DeleteSchemaUtil.posDeleteSchema(spec.schema());
    switch (format) {
      case AVRO:
        return Avro.read(inputFile)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(DataReader::create)
            .build();

      case PARQUET:
        return Parquet.read(inputFile)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema))
            .build();

      case ORC:
        return ORC.read(inputFile)
            .project(deleteSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(deleteSchema, fileSchema))
            .build();

      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
  }

  private static PositionDeleteWriter<Record> positionDeletesWriter(
      OutputFile outputFile,
      FileFormat format,
      PartitionSpec spec,
      StructLike partition,
      Schema rowSchema)
      throws IOException {
    switch (format) {
      case AVRO:
        return Avro.writeDeletes(outputFile)
            .createWriterFunc(DataWriter::create)
            .withPartition(partition)
            .rowSchema(rowSchema)
            .withSpec(spec)
            .buildPositionWriter();
      case PARQUET:
        return Parquet.writeDeletes(outputFile)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .withPartition(partition)
            .rowSchema(rowSchema)
            .withSpec(spec)
            .buildPositionWriter();
      case ORC:
        return ORC.writeDeletes(outputFile)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .withPartition(partition)
            .rowSchema(rowSchema)
            .withSpec(spec)
            .buildPositionWriter();
      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
  }

  private static <F extends ContentFile<F>> void appendEntryWithFile(
      ManifestEntry<F> entry, ManifestWriter<F> writer, F file) {

    switch (entry.status()) {
      case ADDED:
        writer.add(file);
        break;
      case EXISTING:
        writer.existing(
            file, entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
      case DELETED:
        writer.delete(file, entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
    }
  }

  private boolean fileNotExist(String path) {
    return !fileExist(path);
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }

  private static String relativize(String path, String prefix) {
    String toRemove = prefix;
    if (!toRemove.endsWith("/")) {
      toRemove += "/";
    }
    if (!path.startsWith(toRemove)) {
      throw new IllegalArgumentException(
          String.format("Path %s does not start with %s", path, toRemove));
    }
    return path.substring(toRemove.length());
  }

  private static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return combinePaths(targetPrefix, relativize(path, sourcePrefix));
  }

  private static String newPath(String path, Map<String, String> prefixMappings) {
    Map.Entry<String, String> mapPrefix =
        TableMetadataUtil.lookupPrefixMappings(path, prefixMappings);
    if (mapPrefix == null) {
      throw new IllegalArgumentException("unable to find prefix mapping for path: " + path);
    }
    String sourcePrefix = mapPrefix.getKey();
    String targetPrefix = mapPrefix.getValue();
    return combinePaths(targetPrefix, relativize(path, sourcePrefix));
  }

  private static String stagingPath(String originalPath, String stagingLocation) {
    return stagingLocation + fileName(originalPath);
  }

  private String currentMetadataPath(Table tbl) {
    return ((HasTableOperations) tbl).operations().current().metadataFileLocation();
  }

  private static String combinePaths(String absolutePath, String relativePath) {
    String combined = absolutePath;
    if (!combined.endsWith("/")) {
      combined += "/";
    }
    combined += relativePath;
    return combined;
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath =
        ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(File.separator);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    Preconditions.checkArgument(
        !metadataDir.isEmpty(), "Failed to get the metadata file root directory");
    return metadataDir;
  }

  public static class PathPairWithManifestSize extends PathPair {

    private String fileName;
    private Long fileSize;

    public PathPairWithManifestSize() {}

    public PathPairWithManifestSize(PathPair pathPair, String fileName, Long fileSize) {
      this.fileName = fileName;
      this.fileSize = fileSize;
      setSource(pathPair.getSource());
      setTarget(pathPair.getTarget());
    }

    public Long getFileSize() {
      return fileSize;
    }

    public void setFileSize(Long fileSize) {
      this.fileSize = fileSize;
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      PathPairWithManifestSize obj = (PathPairWithManifestSize) object;
      return Objects.equals(getSource(), obj.getSource())
          && Objects.equals(getTarget(), obj.getTarget())
          && Objects.equals(fileName, obj.getFileName())
          && Objects.equals(fileSize, obj.getFileSize());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getSource(), getTarget(), fileName, fileSize);
    }
  }

  public static class PathPair implements Serializable {
    private String source;
    private String target;

    public PathPair() {}

    public PathPair(String source, String target) {
      this.source = source;
      this.target = target;
    }

    public boolean valid() {
      return this.source != null && this.target != null;
    }

    public static PathPair of(String source, String target) {
      return new PathPair(source, target);
    }

    public void setSource(String source) {
      this.source = source;
    }

    public void setTarget(String target) {
      this.target = target;
    }

    public String getSource() {
      return source;
    }

    public String getTarget() {
      return target;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PathPair pathPair = (PathPair) o;
      return Objects.equals(source, pathPair.source) && Objects.equals(target, pathPair.target);
    }

    @Override
    public int hashCode() {
      return Objects.hash(source, target);
    }
  }
}
