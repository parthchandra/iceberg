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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.AnalyzeTable.ContentFileGroupInfo;
import org.apache.iceberg.actions.AnalyzeTable.RowDeltasSummary;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.SnapshotUtil;

public class BaseRowDeltasSummary implements RowDeltasSummary {

  private final Table table;
  private final Map<Long, ContentFileGroupInfo> dataFilesInfoMap;
  private final Map<Long, ContentFileGroupInfo> positionDeleteFilesInfoMap;
  private final Map<Long, ContentFileGroupInfo> equalityDeleteFilesInfoMap;

  public BaseRowDeltasSummary(
      Table table,
      Map<Long, ContentFileGroupInfo> dataFilesInfoMap,
      Map<Long, ContentFileGroupInfo> positionDeleteFilesInfoMap,
      Map<Long, ContentFileGroupInfo> equalityDeleteFilesInfoMap) {
    this.table = table;
    this.dataFilesInfoMap = dataFilesInfoMap;
    this.positionDeleteFilesInfoMap = positionDeleteFilesInfoMap;
    this.equalityDeleteFilesInfoMap = equalityDeleteFilesInfoMap;
  }

  @Override
  public Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot() {
    return dataFilesInfoMap;
  }

  @Override
  public Map<Long, ContentFileGroupInfo> addedPositionDeleteFilesInfoBySnapshot() {
    return positionDeleteFilesInfoMap;
  }

  @Override
  public Map<Long, ContentFileGroupInfo> addedEqualityDeleteFilesInfoBySnapshot() {
    return equalityDeleteFilesInfoMap;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("ROW DELTAS (merge-on-read operations)");

    Iterable<Long> analyzedSnapshotIds = analyzedSnapshotIds();

    if (Iterables.isEmpty(analyzedSnapshotIds)) {
      formatter.object("<empty>");
      return formatter.compileString();
    }

    formatter.object("(showing only recent snapshots)");

    for (long snapshotId : analyzedSnapshotIds) {
      Snapshot snapshot = table.snapshot(snapshotId);
      ValidationException.check(snapshot != null, "Can't find snapshot %d", snapshotId);

      formatter.subHeader(String.format("Snapshot (%d)", snapshotId));

      formatter.property("timestamp-ms", snapshot.timestampMillis());
      formatter.property("schema-id", snapshot.schemaId());
      formatter.propertyMap("summary", snapshot.summary());

      ContentFileGroupInfo dataFilesInfo = dataFilesInfoMap.get(snapshotId);
      if (dataFilesInfo != null) {
        formatter.subSubHeader("added data files");
        formatter.contentFileGroupInfo("added-data", dataFilesInfo);
      }

      ContentFileGroupInfo positionDeleteFilesInfo = positionDeleteFilesInfoMap.get(snapshotId);
      if (positionDeleteFilesInfo != null) {
        formatter.subSubHeader("added position delete files");
        formatter.contentFileGroupInfo("added-position-delete", positionDeleteFilesInfo);
      }

      ContentFileGroupInfo equalityDeleteFilesInfo = equalityDeleteFilesInfoMap.get(snapshotId);
      if (equalityDeleteFilesInfo != null) {
        formatter.subSubHeader("added equality delete files");
        formatter.contentFileGroupInfo("added-equality-delete", equalityDeleteFilesInfo);
      }
    }

    return formatter.compileString();
  }

  private Iterable<Long> analyzedSnapshotIds() {
    Set<Long> snapshotIds =
        ImmutableSet.<Long>builder()
            .addAll(dataFilesInfoMap.keySet())
            .addAll(positionDeleteFilesInfoMap.keySet())
            .addAll(equalityDeleteFilesInfoMap.keySet())
            .build();
    return Iterables.filter(SnapshotUtil.currentAncestorIds(table), snapshotIds::contains);
  }
}
