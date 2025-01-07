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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class TableMetadataUtil {
  private TableMetadataUtil() {}

  public static TableMetadata replacePaths(
      TableMetadata metadata,
      String srcMetaPrefix,
      String tgtMetaPrefix,
      String srcPrefix,
      String tgtPrefix,
      FileIO io) {
    String newLocation = newPath(metadata.location(), srcMetaPrefix, tgtMetaPrefix);

    List<Snapshot> newSnapshots = updatePathInSnapshots(metadata, srcMetaPrefix, tgtMetaPrefix, io);
    List<MetadataLogEntry> metadataLogEntries =
        updatePathInMetadataLogs(metadata, srcMetaPrefix, tgtMetaPrefix);
    long snapshotId =
        metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();
    Map<String, String> properties =
        updateProperties(metadata.properties(), srcMetaPrefix, tgtMetaPrefix, srcPrefix, tgtPrefix);

    return new TableMetadata(
        null,
        metadata.formatVersion(),
        metadata.uuid(),
        newLocation,
        metadata.lastSequenceNumber(),
        metadata.lastUpdatedMillis(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.schemas(),
        metadata.defaultSpecId(),
        metadata.specs(),
        metadata.lastAssignedPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.sortOrders(),
        properties,
        snapshotId,
        newSnapshots,
        null,
        metadata.snapshotLog(),
        metadataLogEntries,
        metadata.refs(),
        updatePathInStatisticsFiles(metadata.statisticsFiles(), srcMetaPrefix, tgtMetaPrefix),
        metadata.partitionStatisticsFiles(),
        metadata.changes());
  }

  public static TableMetadata addMetaDataFromTargetTable(
      TableMetadata metadata, TableMetadata targetTableMetadata) {
    List<Snapshot> targetSnapshots = Lists.newArrayList();
    List<HistoryEntry> targetLogs = Lists.newArrayList();
    List<MetadataLogEntry> targetMetadataLogs = Lists.newArrayList();
    if (targetTableMetadata != null) {
      // Add the snapshot to its snapshot list.
      targetSnapshots.addAll(targetTableMetadata.snapshots());
      targetLogs.addAll(targetTableMetadata.snapshotLog());
      targetMetadataLogs.addAll(targetTableMetadata.previousFiles());
      // add target table's current snapshot into previousFiles list.
      targetMetadataLogs.add(
          new MetadataLogEntry(
              targetTableMetadata.lastUpdatedMillis(), targetTableMetadata.metadataFileLocation()));
    }
    targetSnapshots.add(metadata.currentSnapshot());
    targetLogs.add(metadata.snapshotLog().get(metadata.snapshotLog().size() - 1));

    return new TableMetadata(
        metadata.metadataFileLocation(),
        metadata.formatVersion(),
        metadata.uuid(),
        metadata.location(),
        metadata.lastSequenceNumber(),
        metadata.lastUpdatedMillis(),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.schemas(),
        metadata.defaultSpecId(),
        metadata.specs(),
        metadata.lastAssignedPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.sortOrders(),
        metadata.properties(),
        metadata.currentSnapshot().snapshotId(),
        targetSnapshots,
        null,
        targetLogs,
        targetMetadataLogs,
        metadata.refs(),
        metadata.statisticsFiles(),
        metadata.partitionStatisticsFiles(),
        metadata.changes());
  }

  private static Map<String, String> updateProperties(
      Map<String, String> tableProperties,
      String srcMetaPrefix,
      String tgtMetaPrefix,
      String srcPrefix,
      String tgtPrefix) {
    Map properties = Maps.newHashMap(tableProperties);
    updatePathInProperty(properties, srcPrefix, tgtPrefix, TableProperties.OBJECT_STORE_PATH);
    updatePathInProperty(
        properties, srcPrefix, tgtPrefix, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
    updatePathInProperty(properties, srcPrefix, tgtPrefix, TableProperties.WRITE_DATA_LOCATION);
    updatePathInProperty(
        properties, srcMetaPrefix, tgtMetaPrefix, TableProperties.WRITE_METADATA_LOCATION);

    return properties;
  }

  private static List<StatisticsFile> updatePathInStatisticsFiles(
      List<StatisticsFile> statisticsFiles, String srcMetaPrefix, String tgtMetaPrefix) {
    return statisticsFiles.stream()
        .map(
            existing ->
                new GenericStatisticsFile(
                    existing.snapshotId(),
                    newPath(existing.path(), srcMetaPrefix, tgtMetaPrefix),
                    existing.fileSizeInBytes(),
                    existing.fileFooterSizeInBytes(),
                    existing.blobMetadata()))
        .collect(Collectors.toList());
  }

  private static void updatePathInProperty(
      Map<String, String> properties,
      String sourcePrefix,
      String targetPrefix,
      String propertyName) {
    if (properties.containsKey(propertyName)) {
      properties.put(
          propertyName, newPath(properties.get(propertyName), sourcePrefix, targetPrefix));
    }
  }

  private static List<MetadataLogEntry> updatePathInMetadataLogs(
      TableMetadata metadata, String sourcePrefix, String targetPrefix) {
    List<MetadataLogEntry> metadataLogEntries =
        Lists.newArrayListWithCapacity(metadata.previousFiles().size());
    for (MetadataLogEntry metadataLog : metadata.previousFiles()) {
      MetadataLogEntry newMetadataLog =
          new MetadataLogEntry(
              metadataLog.timestampMillis(),
              newPath(metadataLog.file(), sourcePrefix, targetPrefix));
      metadataLogEntries.add(newMetadataLog);
    }
    return metadataLogEntries;
  }

  private static List<Snapshot> updatePathInSnapshots(
      TableMetadata metadata, String sourcePrefix, String targetPrefix, FileIO io) {
    List<Snapshot> newSnapshots = Lists.newArrayListWithCapacity(metadata.snapshots().size());
    for (Snapshot snapshot : metadata.snapshots()) {
      String newManifestListLocation =
          newPath(snapshot.manifestListLocation(), sourcePrefix, targetPrefix);
      Snapshot newSnapshot =
          new BaseSnapshot(
              snapshot.sequenceNumber(),
              snapshot.snapshotId(),
              snapshot.parentId(),
              snapshot.timestampMillis(),
              snapshot.operation(),
              snapshot.summary(),
              snapshot.schemaId(),
              newManifestListLocation);
      newSnapshots.add(newSnapshot);
    }
    return newSnapshots;
  }

  public static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return path.replaceFirst(sourcePrefix, targetPrefix);
  }
}
