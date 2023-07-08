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
import org.apache.iceberg.actions.AnalyzeTable.OverwritesSummary;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;

public class BaseOverwritesSummary implements OverwritesSummary {

  private final Table table;
  private final Iterable<Long> orderedSnapshotIds;
  private final Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot;
  private final Map<Long, ContentFileGroupInfo> removedDataFilesInfoBySnapshot;

  public BaseOverwritesSummary(
      Table table,
      Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot,
      Map<Long, ContentFileGroupInfo> removedDataFilesInfoBySnapshot) {
    this.table = table;
    Set<Long> snapshotIds =
        Sets.union(addedDataFilesInfoBySnapshot.keySet(), removedDataFilesInfoBySnapshot.keySet());
    this.orderedSnapshotIds =
        Iterables.filter(SnapshotUtil.currentAncestorIds(table), snapshotIds::contains);
    this.addedDataFilesInfoBySnapshot = addedDataFilesInfoBySnapshot;
    this.removedDataFilesInfoBySnapshot = removedDataFilesInfoBySnapshot;
  }

  @Override
  public Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot() {
    return addedDataFilesInfoBySnapshot;
  }

  @Override
  public Map<Long, ContentFileGroupInfo> removedDataFilesInfoBySnapshot() {
    return removedDataFilesInfoBySnapshot;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("OVERWRITE OPERATIONS");

    if (Iterables.isEmpty(orderedSnapshotIds)) {
      formatter.object("<empty>");
      return formatter.compileString();
    }

    formatter.object("(showing only recent snapshots)");

    for (long snapshotId : orderedSnapshotIds) {
      Snapshot snapshot = table.snapshot(snapshotId);
      ValidationException.check(snapshot != null, "Can't find snapshot %d", snapshotId);

      formatter.subHeader(String.format("Snapshot (%d)", snapshotId));

      formatter.property("timestamp-ms", snapshot.timestampMillis());
      formatter.property("schema-id", snapshot.schemaId());
      formatter.propertyMap("summary", snapshot.summary());

      ContentFileGroupInfo addedFilesInfo = addedDataFilesInfoBySnapshot.get(snapshotId);
      if (addedFilesInfo != null) {
        formatter.subSubHeader("added data files");
        formatter.contentFileGroupInfo("added-data", addedFilesInfo);
      }

      ContentFileGroupInfo removedFilesInfo = removedDataFilesInfoBySnapshot.get(snapshotId);
      if (removedFilesInfo != null) {
        formatter.subSubHeader("removed data files");
        formatter.contentFileGroupInfo("removed-data", removedFilesInfo);
      }
    }

    return formatter.compileString();
  }
}
