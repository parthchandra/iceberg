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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.AnalyzeTable.AppendsSummary;
import org.apache.iceberg.actions.AnalyzeTable.ContentFileGroupInfo;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.SnapshotUtil;

public class BaseAppendsSummary implements AppendsSummary {

  private final Table table;
  private final Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot;

  public BaseAppendsSummary(
      Table table, Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot) {
    this.table = table;
    this.addedDataFilesInfoBySnapshot = addedDataFilesInfoBySnapshot;
  }

  @Override
  public Map<Long, ContentFileGroupInfo> addedDataFilesInfoBySnapshot() {
    return addedDataFilesInfoBySnapshot;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("APPENDS");

    if (addedDataFilesInfoBySnapshot.isEmpty()) {
      formatter.object("<empty>");
      return formatter.compileString();
    }

    formatter.object("(showing only recent snapshots)");

    for (long snapshotId : SnapshotUtil.currentAncestorIds(table)) {
      if (addedDataFilesInfoBySnapshot.containsKey(snapshotId)) {
        Snapshot snapshot = table.snapshot(snapshotId);
        ValidationException.check(snapshot != null, "Can't find snapshot %d", snapshotId);

        formatter.subHeader(String.format("Snapshot (%d)", snapshotId));

        formatter.property("timestamp-ms", snapshot.timestampMillis());
        formatter.property("schema-id", snapshot.schemaId());
        formatter.propertyMap("summary", snapshot.summary());

        ContentFileGroupInfo info = addedDataFilesInfoBySnapshot.get(snapshotId);
        formatter.subSubHeader("added data files");
        formatter.contentFileGroupInfo("added-data", info);
      }
    }

    return formatter.compileString();
  }
}
