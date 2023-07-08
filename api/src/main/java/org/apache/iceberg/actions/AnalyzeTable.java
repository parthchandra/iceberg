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
package org.apache.iceberg.actions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;

/**
 * An action that analyzes the current table state and prepares helpful summaries that can be used
 * for debugging and tuning.
 *
 * <p>Note this is an evolving internal API that can change even in minor releases. No production
 * code should depend on this API.
 */
public interface AnalyzeTable extends Action<AnalyzeTable, AnalyzeTable.Result> {
  /**
   * An option that enables computing and persisting partition stats, which may be too large to be
   * held in memory.
   */
  String PARTITION_STATS_ENABLED = "partition-stats.enabled";

  boolean PARTITION_STATS_ENABLED_DEFAULT = true;

  /** An option that defines the number of output files to use when persisting partition stats. */
  String PARTITION_STATS_NUM_OUTPUT_FILES = "partition-stats.num-output-files";

  int PARTITION_STATS_NUM_OUTPUT_FILES_DEFAULT = 1;

  /** An option that enables caching common information to speed up the execution. */
  String USE_CACHING = "use-caching";

  boolean USE_CACHING_DEFAULT = true;

  /** Sets the analysis mode. If not set, defaults to {@link AnalysisMode#FULL} mode. */
  AnalyzeTable mode(AnalysisMode mode);

  /**
   * Sets a location where to store stats that may be too large to be held in memory. If not set,
   * defaults to the analysis subfolder under the root table location.
   */
  AnalyzeTable outputLocation(String location);

  /** The action analysis mode. */
  enum AnalysisMode {
    QUICK,
    FULL
  }

  /** The action result that contains computed summaries. */
  interface Result {
    /** Returns a formatted string containing all summaries. */
    String toFormattedString();

    /** Returns all summaries that were computed. */
    List<Summary> summaries();

    /** Returns a particular summary by its class or null if no such summary was computed. */
    <T extends Summary> T summary(Class<T> clazz);
  }

  /** A summary containing useful information for debugging and tuning. */
  interface Summary {
    /** Returns a formatted string for this summary. */
    String toFormattedString();
  }

  /** A summary that provides basic information about the table configuration. */
  interface TableConfigSummary extends Summary {
    /** Returns the table format version. */
    int formatVersion();

    /** Returns the table UUID. */
    String uuid();

    /** Returns the table location. */
    String location();

    /** Returns the location of the current metadata version file. */
    String metadataFileLocation();

    /** Returns the timestamp when the table was modified for the last time (in milliseconds). */
    long lastUpdatedMs();

    /** Returns the last assigned sequence number. */
    long lastSequenceNumber();

    /** Returns the last assigned column ID. */
    int lastAssignedColumnId();

    /** Returns the last assigned partition column ID. */
    int lastAssignedPartitionId();

    /** Returns the current schema. */
    Schema schema();

    /** Returns all known schemas. */
    Map<Integer, Schema> schemas();

    /** Returns the default partition spec. */
    PartitionSpec spec();

    /** Returns all known partition specs. */
    Map<Integer, PartitionSpec> specs();

    /** Returns the default sort order. */
    SortOrder sortOrder();

    /** Returns all known sort orders. */
    Map<Integer, SortOrder> sortOrders();

    /** Returns explicitly set table properties. */
    Map<String, String> properties();
  }

  /** A summary that provides information about all known snapshots. */
  interface SnapshotsSummary extends Summary {

    String APPENDS_COUNT = "appends-count";
    String OVERWRITES_COUNT = "overwrites-count";
    String ROW_DELTAS_COUNT = "row-deltas-count";
    String DELETES_COUNT = "deletes-count";
    String METADATA_REWRITES_COUNT = "metadata-rewrites-count";
    String DATA_FILE_REWRITES_COUNT = "data-file-rewrites-count";
    String DELETE_FILE_REWRITES_COUNT = "delete-file-rewrites-count";

    /** The number of known snapshots. */
    int snapshotsCount();

    /** Returns the snapshot lineage (from current to last known). */
    List<Snapshot> lineage();

    /** Returns numbers of snapshots by operation in the lineage. */
    Map<String, Integer> lineageOperationCounts();

    /** Returns all other valid snapshots not part of the lineage (e.g. write-audit-publish). */
    Iterable<Snapshot> otherSnapshots();

    /** Returns numbers of snapshots by operation among other snapshots not part of the lineage. */
    Map<String, Integer> otherSnapshotsOperationCounts();
  }

  /** A summary that provides estimates about unexpired snapshots and files. */
  interface UnexpiredSnapshotsSummary extends Summary {
    /** Returns the timestamp that was used to look for snapshots that can be expired. */
    long olderThanTimestampMs();

    /**
     * Returns the estimated number of snapshots that can be expired based on the current table
     * configuration.
     */
    int unexpiredSnapshotsCount();

    /**
     * Returns the estimated number of data files that can be deleted as a result of expiring
     * snapshots.
     */
    long unexpiredDataFilesCount();

    /**
     * Returns the estimated number of position delete files that can be deleted as a result of
     * expiring snapshots.
     */
    long unexpiredPositionDeleteFilesCount();

    /**
     * Returns the estimated number of equality delete files that can be deleted as a result of
     * expiring snapshots.
     */
    long unexpiredEqualityDeleteFilesCount();
  }

  /** A summary that provides information about manifests in a snapshot. */
  interface ManifestsSummary extends Summary {
    /** Returns the snapshot for which this summary was computed. */
    Snapshot snapshot();

    /** Returns stats for data manifests across all specs. */
    FileGroupStats dataManifestsStats();

    /** Returns stats for data manifests by spec. */
    Map<Integer, FileGroupStats> dataManifestsStatsBySpec();

    /** Returns stats for delete manifests across all specs. */
    FileGroupStats deleteManifestsStats();

    /** Returns stats for delete manifests by spec. */
    Map<Integer, FileGroupStats> deleteManifestsStatsBySpec();
  }

  /** A summary that provides information about live content files in a snapshot. */
  interface ContentFilesSummary extends Summary {
    /** Returns the snapshot for which this summary was computed. */
    Snapshot snapshot();

    /** Returns information for data files. */
    ContentFileGroupInfo dataFilesInfo();

    /** Returns information for position delete files. */
    ContentFileGroupInfo positionDeleteFilesInfo();

    /** Returns information for equality delete files. */
    ContentFileGroupInfo equalityDeleteFilesInfo();
  }

  /** A summary that provides information about recent appends. */
  interface AppendsSummary extends Summary {
    /** Returns information for added data files by snapshot. */
    Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot();
  }

  /** A summary that provides information about recent overwrite operations. */
  interface OverwritesSummary extends Summary {
    /** Returns information for added data files by snapshot. */
    Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot();

    /** Returns information for removed data files by snapshot. */
    Map<Long, ContentFileGroupInfo> removedDataFilesInfoBySnapshot();
  }

  /** A summary that provides information about recent delta (aka merge-on-read) operations. */
  interface RowDeltasSummary extends Summary {
    /** Returns information for added data files by snapshot. */
    Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot();

    /** Returns information for added position delete files by snapshot. */
    Map<Long, ContentFileGroupInfo> addedPositionDeleteFilesInfoBySnapshot();

    /** Returns information for added equality delete files by snapshot. */
    Map<Long, ContentFileGroupInfo> addedEqualityDeleteFilesInfoBySnapshot();
  }

  /** Information about a group of content files (e.g. data files, equality delete files). */
  interface ContentFileGroupInfo {
    /** Returns combined stats across all specs and partitions. */
    FileGroupStats stats();

    /** Returns stats by spec. */
    Map<Integer, FileGroupStats> statsBySpec();

    /** Returns the location of the file with stats by partition. */
    String statsByPartitionFileLocation();
  }

  /** Stats for a group of files. */
  interface FileGroupStats {
    /** Returns the number of files in the group. */
    long totalFilesCount();

    /** Returns the number of records in the group. */
    long totalRecordsCount();

    /** Returns the total size of all files in the group. */
    long totalFilesSizeBytes();

    /** Returns the average file size in the group. */
    Double avgFileSizeBytes();

    /** Returns the min file size in the group. */
    Long minFileSizeBytes();

    /** Returns the max file size in the group. */
    Long maxFileSizeBytes();

    /** Returns the 25th percentile file size in the group. */
    Double percentile25thFileSizeBytes();

    /** Returns the 75th percentile file size in the group. */
    Double percentile75thFileSizeBytes();

    /** Combines stats for two groups. */
    FileGroupStats combine(FileGroupStats other);
  }
}
