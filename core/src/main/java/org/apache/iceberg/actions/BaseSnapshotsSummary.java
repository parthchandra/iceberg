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

import static org.apache.iceberg.DataOperations.APPEND;
import static org.apache.iceberg.DataOperations.DELETE;
import static org.apache.iceberg.DataOperations.OVERWRITE;
import static org.apache.iceberg.DataOperations.REPLACE;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.actions.AnalyzeTable.SnapshotsSummary;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SnapshotUtil;

public class BaseSnapshotsSummary implements SnapshotsSummary {

  private static final int MAX_NUM_SHOWN_SNAPSHOTS = 20;

  private final List<Snapshot> lineage;
  private final Map<String, Integer> lineageOperationCounts;
  private final Iterable<Snapshot> otherSnapshots;
  private final Map<String, Integer> otherSnapshotsOperationCounts;
  private final int totalValidSnapshotsCount;

  public BaseSnapshotsSummary(List<Snapshot> lineage, Iterable<Snapshot> otherSnapshots) {
    this.lineage = lineage;
    this.lineageOperationCounts = buildOperationCounts(lineage);
    this.otherSnapshots = otherSnapshots;
    this.otherSnapshotsOperationCounts = buildOperationCounts(otherSnapshots);
    this.totalValidSnapshotsCount = lineage.size() + Iterables.size(otherSnapshots);
  }

  @Override
  public int snapshotsCount() {
    return totalValidSnapshotsCount;
  }

  @Override
  public List<Snapshot> lineage() {
    return lineage;
  }

  @Override
  public Map<String, Integer> lineageOperationCounts() {
    return lineageOperationCounts;
  }

  @Override
  public Iterable<Snapshot> otherSnapshots() {
    return otherSnapshots;
  }

  @Override
  public Map<String, Integer> otherSnapshotsOperationCounts() {
    return otherSnapshotsOperationCounts;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("SNAPSHOTS");

    formatter.property("snapshots-count", totalValidSnapshotsCount);

    formatter.subHeader("Lineage");
    formatSnapshots(formatter, lineage, lineageOperationCounts);

    formatter.subHeader("Other Snapshots");
    formatSnapshots(formatter, otherSnapshots, otherSnapshotsOperationCounts);

    return formatter.compileString();
  }

  private void formatSnapshots(
      SummaryFormatter formatter, Iterable<Snapshot> snapshots, Map<String, Integer> counts) {

    if (Iterables.isEmpty(snapshots) && counts.isEmpty()) {
      formatter.object("<empty>");

    } else {
      counts.forEach(formatter::property);

      for (Snapshot snapshot : Iterables.limit(snapshots, MAX_NUM_SHOWN_SNAPSHOTS)) {
        formatter.subSubHeader(String.format("snapshot (%d)", snapshot.snapshotId()));

        formatter.property("parent-id", snapshot.parentId());
        formatter.property("timestamp-ms", snapshot.timestampMillis());
        formatter.property("operation", snapshot.operation());
        formatter.property("schema-id", snapshot.schemaId());
        formatter.propertyMap("summary", snapshot.summary());
      }

      if (Iterables.size(snapshots) > MAX_NUM_SHOWN_SNAPSHOTS) {
        formatter.newLine();
        formatter.object("<...>");
      }
    }
  }

  private Map<String, Integer> buildOperationCounts(Iterable<Snapshot> snapshots) {
    Map<String, Integer> counts = Maps.newHashMap();

    for (Snapshot snapshot : snapshots) {
      String operation = snapshot.operation();

      switch (operation) {
        case APPEND:
          counts.merge(APPENDS_COUNT, 1, Integer::sum);
          break;

        case OVERWRITE:
          if (SnapshotUtil.isRowDelta(snapshot)) {
            counts.merge(ROW_DELTAS_COUNT, 1, Integer::sum);

          } else {
            counts.merge(OVERWRITES_COUNT, 1, Integer::sum);
          }
          break;

        case DELETE:
          counts.merge(DELETES_COUNT, 1, Integer::sum);
          break;

        case REPLACE:
          if (SnapshotUtil.isDataRewrite(snapshot)) {
            counts.merge(DATA_FILE_REWRITES_COUNT, 1, Integer::sum);

          } else if (SnapshotUtil.isDeleteRewrite(snapshot)) {
            counts.merge(DELETE_FILE_REWRITES_COUNT, 1, Integer::sum);

          } else {
            counts.merge(METADATA_REWRITES_COUNT, 1, Integer::sum);
          }
          break;
      }
    }

    return counts;
  }
}
