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

import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.iceberg.MetadataTableType.FILES;
import static org.apache.iceberg.MetadataTableType.MANIFESTS;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.AnalyzeTable;
import org.apache.iceberg.actions.BaseAnalyzeTableActionResult;
import org.apache.iceberg.actions.BaseAppendsSummary;
import org.apache.iceberg.actions.BaseContentFileGroupInfo;
import org.apache.iceberg.actions.BaseContentFilesSummary;
import org.apache.iceberg.actions.BaseFileGroupStats;
import org.apache.iceberg.actions.BaseManifestsSummary;
import org.apache.iceberg.actions.BaseOverwritesSummary;
import org.apache.iceberg.actions.BaseRowDeltasSummary;
import org.apache.iceberg.actions.BaseSnapshotsSummary;
import org.apache.iceberg.actions.BaseTableConfigSummary;
import org.apache.iceberg.actions.BaseUnexpiredSnapshotsSummary;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalyzeTableSparkAction extends BaseSparkAction<AnalyzeTableSparkAction>
    implements AnalyzeTable {

  private static final int MAX_SNAPSHOTS_PER_OPERATION = 5;

  private final Table table;
  private final TableMetadata metadata;
  private AnalysisMode mode;
  private String outputLocation;

  AnalyzeTableSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.metadata = ((HasTableOperations) table).operations().current();
    this.mode = AnalysisMode.FULL;
    this.outputLocation = table.location() + "/analysis/summaries-" + metadataFileName();
  }

  @Override
  protected AnalyzeTableSparkAction self() {
    return this;
  }

  @Override
  public AnalyzeTableSparkAction mode(AnalysisMode newMode) {
    this.mode = newMode;
    return this;
  }

  @Override
  public AnalyzeTableSparkAction outputLocation(String newOutputLocation) {
    this.outputLocation = newOutputLocation;
    return this;
  }

  @Override
  public Result execute() {
    String desc = String.format("Analyzing (mode=%s) table %s", mode, table.name());
    JobGroupInfo info = newJobGroupInfo("ANALYZE-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    List<Summary> summaries = Lists.newArrayList();

    summaries.add(analyzeTableConfig());
    summaries.add(analyzeSnapshots());
    summaries.add(analyzeUnexpiredSnapshots());

    if (table.currentSnapshot() != null) {
      summaries.add(analyzeManifests());

      if (mode == AnalysisMode.FULL) {
        summaries.add(analyzeContentFiles());
        summaries.add(analyzeAppends());
        summaries.add(analyzeOverwrites());
        summaries.add(analyzeDeltaOperations());
      }
    }

    return new BaseAnalyzeTableActionResult(summaries);
  }

  private TableConfigSummary analyzeTableConfig() {
    return new BaseTableConfigSummary(metadata);
  }

  private SnapshotsSummary analyzeSnapshots() {
    List<Snapshot> lineageSnapshots = ImmutableList.copyOf(SnapshotUtil.currentAncestors(table));
    Set<Long> lineageSnapshotIds =
        lineageSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Iterable<Snapshot> otherSnapshots =
        Iterables.filter(table.snapshots(), s -> !lineageSnapshotIds.contains(s.snapshotId()));

    return new BaseSnapshotsSummary(lineageSnapshots, otherSnapshots);
  }

  private UnexpiredSnapshotsSummary analyzeUnexpiredSnapshots() {
    long maxSnapshotAgeMs =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.MAX_SNAPSHOT_AGE_MS,
            TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT);
    long olderThanTimestampMs = System.currentTimeMillis() - maxSnapshotAgeMs;

    int minNumSnapshots =
        PropertyUtil.propertyAsInt(
            table.properties(),
            TableProperties.MIN_SNAPSHOTS_TO_KEEP,
            TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

    Set<Long> keptSnapshotIds =
        SnapshotUtil.currentAncestorIds(table).stream()
            .limit(minNumSnapshots)
            .collect(Collectors.toSet());

    Iterable<Snapshot> unexpiredSnapshots =
        Iterables.filter(
            table.snapshots(),
            snapshot -> {
              long snapshotId = snapshot.snapshotId();
              long timestampMs = snapshot.timestampMillis();
              return timestampMs < olderThanTimestampMs && !keptSnapshotIds.contains(snapshotId);
            });

    int unexpiredSnapshotsCount = 0;
    long unexpiredDataFilesCount = 0L;
    long unexpiredPositionDeleteFilesCount = 0L;
    long unexpiredEqualityDeleteFilesCount = 0L;

    for (Snapshot snapshot : unexpiredSnapshots) {
      Map<String, String> summary = snapshot.summary();

      unexpiredSnapshotsCount += 1;

      long removedDataFilesCount =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.DELETED_FILES_PROP, 0L);
      unexpiredDataFilesCount += removedDataFilesCount;

      long removedPositionDeleteFilesCount =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP, 0L);
      unexpiredPositionDeleteFilesCount += removedPositionDeleteFilesCount;

      long removedEqualityDeleteFilesCount =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.REMOVED_EQ_DELETE_FILES_PROP, 0L);
      unexpiredEqualityDeleteFilesCount += removedEqualityDeleteFilesCount;
    }

    return new BaseUnexpiredSnapshotsSummary(
        olderThanTimestampMs,
        unexpiredSnapshotsCount,
        unexpiredDataFilesCount,
        unexpiredPositionDeleteFilesCount,
        unexpiredEqualityDeleteFilesCount);
  }

  private ManifestsSummary analyzeManifests() {
    Snapshot snapshot = table.currentSnapshot();
    Map<String, String> readOptions =
        ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, String.valueOf(snapshot.snapshotId()));
    Dataset<Row> manifestDF = loadMetadataTable(table, MANIFESTS, readOptions);

    Dataset<Row> manifestInfoDF =
        manifestDF.selectExpr(
            "partition_spec_id AS spec_id",
            "content = 0 AS is_data_manifest_file",
            "content = 1 AS is_delete_manifest_file",
            "length AS file_size",
            "IF(content = 0, added_data_files_count + existing_data_files_count, added_delete_files_count + existing_delete_files_count) AS record_count");

    return withReusableDS(
        manifestInfoDF,
        useCaching(),
        df ->
            new BaseManifestsSummary(
                snapshot,
                computeSpecStats(df, "data_manifest"),
                computeSpecStats(df, "delete_manifest")));
  }

  private ContentFilesSummary analyzeContentFiles() {
    Snapshot snapshot = table.currentSnapshot();
    Map<String, String> readOptions =
        ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, String.valueOf(snapshot.snapshotId()));
    Dataset<Row> fileDF = loadMetadataTable(table, FILES, readOptions);

    boolean isPartitioned = table.spec().isPartitioned() || table.specs().size() > 1;

    Dataset<Row> fileInfoDF =
        fileDF.selectExpr(
            "spec_id",
            isPartitioned ? "partition" : "null AS partition",
            "content = 0 AS is_data_file",
            "content = 1 AS is_position_delete_file",
            "content = 2 AS is_equality_delete_file",
            "file_size_in_bytes AS file_size",
            "record_count");

    return withReusableDS(
        fileInfoDF,
        useCaching(),
        df ->
            new BaseContentFilesSummary(
                snapshot,
                computeFileGroupInfo(df, snapshot, "data"),
                computeFileGroupInfo(df, snapshot, "position_delete"),
                computeFileGroupInfo(df, snapshot, "equality_delete")));
  }

  private AppendsSummary analyzeAppends() {
    Iterable<Snapshot> appendSnapshots =
        Iterables.filter(
            SnapshotUtil.currentAncestors(table),
            snapshot -> DataOperations.APPEND.equals(snapshot.operation()));

    Map<Long, ContentFileGroupInfo> infoBySnapshot = Maps.newHashMap();

    for (Snapshot snapshot : Iterables.limit(appendSnapshots, MAX_SNAPSHOTS_PER_OPERATION)) {
      long snapshotId = snapshot.snapshotId();
      Map<String, String> readOptions =
          ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, String.valueOf(snapshotId));
      Dataset<Row> entryDF = loadMetadataTable(table, ENTRIES, readOptions);

      boolean isPartitioned = table.spec().isPartitioned() || table.specs().size() > 1;

      Dataset<Row> fileInfoDF =
          entryDF
              .filter("status = 1 AND snapshot_id = " + snapshotId)
              .selectExpr(
                  "data_file.spec_id AS spec_id",
                  isPartitioned ? "data_file.partition AS partition" : "null AS partition",
                  "data_file.content = 0 AS is_added_data_file",
                  "data_file.file_size_in_bytes AS file_size",
                  "data_file.record_count AS record_count");

      withReusableDS(
          fileInfoDF,
          useCaching(),
          df -> {
            ContentFileGroupInfo info = computeFileGroupInfo(df, snapshot, "added_data");
            infoBySnapshot.put(snapshotId, info);
            return null;
          });
    }

    return new BaseAppendsSummary(table, infoBySnapshot);
  }

  private OverwritesSummary analyzeOverwrites() {
    Iterable<Snapshot> overwriteSnapshots =
        Iterables.filter(SnapshotUtil.currentAncestors(table), SnapshotUtil::isOverwriteFiles);

    Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot = Maps.newHashMap();
    Map<Long, ContentFileGroupInfo> removedDataFilesInfoBySnapshot = Maps.newHashMap();

    for (Snapshot snapshot : Iterables.limit(overwriteSnapshots, MAX_SNAPSHOTS_PER_OPERATION)) {
      long snapshotId = snapshot.snapshotId();
      Map<String, String> readOptions =
          ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, String.valueOf(snapshotId));
      Dataset<Row> entryDF = loadMetadataTable(table, ENTRIES, readOptions);

      boolean isPartitioned = table.spec().isPartitioned() || table.specs().size() > 1;

      Dataset<Row> fileInfoDF =
          entryDF
              .filter("status IN (1, 2) AND snapshot_id = " + snapshotId)
              .selectExpr(
                  "data_file.spec_id AS spec_id",
                  isPartitioned ? "data_file.partition AS partition" : "null AS partition",
                  "status = 1 AND data_file.content = 0 AS is_added_data_file",
                  "status = 2 AND data_file.content = 0 AS is_removed_data_file",
                  "data_file.file_size_in_bytes AS file_size",
                  "data_file.record_count AS record_count");

      withReusableDS(
          fileInfoDF,
          useCaching(),
          df -> {
            ContentFileGroupInfo addedDataFilesInfo =
                computeFileGroupInfo(df, snapshot, "added_data");
            addedDataFilesInfoBySnapshot.put(snapshotId, addedDataFilesInfo);

            ContentFileGroupInfo removedDataFilesInfo =
                computeFileGroupInfo(df, snapshot, "removed_data");
            removedDataFilesInfoBySnapshot.put(snapshotId, removedDataFilesInfo);

            return null;
          });
    }

    return new BaseOverwritesSummary(
        table, addedDataFilesInfoBySnapshot, removedDataFilesInfoBySnapshot);
  }

  private RowDeltasSummary analyzeDeltaOperations() {
    Iterable<Snapshot> rowDeltaSnapshots =
        Iterables.filter(SnapshotUtil.currentAncestors(table), SnapshotUtil::isRowDelta);

    Map<Long, ContentFileGroupInfo> dataFilesInfoBySnapshot = Maps.newHashMap();
    Map<Long, ContentFileGroupInfo> positionDeleteFilesInfoBySnapshot = Maps.newHashMap();
    Map<Long, ContentFileGroupInfo> equalityDeleteFilesInfoBySnapshot = Maps.newHashMap();

    for (Snapshot snapshot : Iterables.limit(rowDeltaSnapshots, MAX_SNAPSHOTS_PER_OPERATION)) {
      long snapshotId = snapshot.snapshotId();
      Map<String, String> readOptions =
          ImmutableMap.of(SparkReadOptions.SNAPSHOT_ID, String.valueOf(snapshotId));
      Dataset<Row> entryDF = loadMetadataTable(table, ENTRIES, readOptions);

      boolean isPartitioned = table.spec().isPartitioned() || table.specs().size() > 1;

      Dataset<Row> fileInfoDF =
          entryDF
              .filter("status = 1 AND snapshot_id = " + snapshotId)
              .selectExpr(
                  "data_file.spec_id AS spec_id",
                  isPartitioned ? "data_file.partition AS partition" : "null AS partition",
                  "data_file.content = 0 AS is_added_data_file",
                  "data_file.content = 1 AS is_added_position_delete_file",
                  "data_file.content = 2 AS is_added_equality_delete_file",
                  "data_file.file_size_in_bytes AS file_size",
                  "data_file.record_count AS record_count");

      withReusableDS(
          fileInfoDF,
          useCaching(),
          df -> {
            ContentFileGroupInfo dataFilesInfo = computeFileGroupInfo(df, snapshot, "added_data");
            dataFilesInfoBySnapshot.put(snapshotId, dataFilesInfo);

            ContentFileGroupInfo positionDeleteFilesInfo =
                computeFileGroupInfo(df, snapshot, "added_position_delete");
            positionDeleteFilesInfoBySnapshot.put(snapshotId, positionDeleteFilesInfo);

            ContentFileGroupInfo equalityDeleteFilesInfo =
                computeFileGroupInfo(df, snapshot, "added_equality_delete");
            equalityDeleteFilesInfoBySnapshot.put(snapshotId, equalityDeleteFilesInfo);

            return null;
          });
    }

    return new BaseRowDeltasSummary(
        table,
        dataFilesInfoBySnapshot,
        positionDeleteFilesInfoBySnapshot,
        equalityDeleteFilesInfoBySnapshot);
  }

  private String computePartitionStats(Dataset<Row> fileInfoDF, Snapshot snapshot, String key) {
    String fileName = partitionStatsFileName(snapshot, key);
    String partitionStatsFileLocation = newFileLocation(fileName);

    Dataset<Row> partitionStatsDF =
        fileInfoDF
            .filter(expr("is_%s_file = true", key))
            .groupBy("spec_id", "partition")
            .agg(expr("count(*) AS count_start"), fileStatsAggExprs(key))
            .drop("count_start");

    persist(partitionStatsDF, partitionStatsFileLocation, partitionStatsNumOutputFiles());

    return partitionStatsFileLocation;
  }

  private String partitionStatsFileName(Snapshot snapshot, String key) {
    return String.format("snap-%d-%s-files-partition-stats.parquet", snapshot.snapshotId(), key);
  }

  private Map<Integer, FileGroupStats> computeSpecStats(Dataset<Row> fileInfoDF, String key) {
    List<Row> specStatsRows =
        fileInfoDF
            .filter(expr("is_%s_file = true", key))
            .groupBy("spec_id")
            .agg(expr("count(*) AS count_start"), fileStatsAggExprs(key))
            .drop("count_start")
            .collectAsList();

    Map<Integer, FileGroupStats> specStats = Maps.newHashMap();

    for (Row specStatsRow : specStatsRows) {
      int specId = specStatsRow.getAs("spec_id");
      specStats.put(specId, extractStats(key, specStatsRow));
    }

    return specStats;
  }

  private ContentFileGroupInfo computeFileGroupInfo(
      Dataset<Row> fileInfoDF, Snapshot snapshot, String key) {

    if (partitionStatsEnabled()) {
      Map<Integer, FileGroupStats> specStats = computeSpecStats(fileInfoDF, key);
      String partitionStatsFileLocation = computePartitionStats(fileInfoDF, snapshot, key);
      return new BaseContentFileGroupInfo(specStats, partitionStatsFileLocation);
    } else {
      Map<Integer, FileGroupStats> specStats = computeSpecStats(fileInfoDF, key);
      return new BaseContentFileGroupInfo(specStats);
    }
  }

  private FileGroupStats extractStats(String key, Row row) {
    long filesCount = row.getAs("total_" + key + "_files_count");
    long recordsCount = row.getAs("total_" + key + "_records_count");
    long totalSize = row.getAs("total_" + key + "_files_size");
    double avgSize = row.getAs("avg_" + key + "_file_size");
    long minSize = row.getAs("min_" + key + "_file_size");
    long maxSize = row.getAs("max_" + key + "_file_size");
    double percentile25Size = row.getAs("25th_percentile_" + key + "_file_size");
    double percentile75Size = row.getAs("75th_percentile_" + key + "_file_size");

    return new BaseFileGroupStats(
        filesCount,
        recordsCount,
        totalSize,
        avgSize,
        minSize,
        maxSize,
        percentile25Size,
        percentile75Size);
  }

  private String metadataFileName() {
    Path metadataFilePath = new Path(metadata.metadataFileLocation());
    return metadataFilePath.getName();
  }

  private String newFileLocation(String fileName) {
    return new Path(outputLocation, fileName).toString();
  }

  private boolean useCaching() {
    return PropertyUtil.propertyAsBoolean(options(), USE_CACHING, USE_CACHING_DEFAULT);
  }

  private boolean partitionStatsEnabled() {
    return PropertyUtil.propertyAsBoolean(
        options(), PARTITION_STATS_ENABLED, PARTITION_STATS_ENABLED_DEFAULT);
  }

  private int partitionStatsNumOutputFiles() {
    return PropertyUtil.propertyAsInt(
        options(), PARTITION_STATS_NUM_OUTPUT_FILES, PARTITION_STATS_NUM_OUTPUT_FILES_DEFAULT);
  }

  private void persist(Dataset<Row> df, String location, int numFiles) {
    df.coalesce(numFiles).write().mode("overwrite").parquet(location);
  }

  private Column[] fileStatsAggExprs(String key) {
    return new Column[] {
      expr("count(*) AS total_%s_files_count", key),
      expr("sum(record_count) AS total_%s_records_count", key),
      expr("sum(file_size) AS total_%s_files_size", key),
      expr("avg(file_size) AS avg_%s_file_size", key),
      expr("min(file_size) AS min_%s_file_size", key),
      expr("max(file_size) AS max_%s_file_size", key),
      expr("percentile(file_size, 0.25) AS 25th_percentile_%s_file_size", key),
      expr("percentile(file_size, 0.75) AS 75th_percentile_%s_file_size", key)
    };
  }
}
