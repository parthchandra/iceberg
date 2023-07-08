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
import org.apache.iceberg.actions.AnalyzeTable.FileGroupStats;
import org.apache.iceberg.actions.AnalyzeTable.ManifestsSummary;

public class BaseManifestsSummary implements ManifestsSummary {

  private final Snapshot snapshot;
  private final FileGroupStats dataManifestsStats;
  private final Map<Integer, FileGroupStats> dataManifestsStatsBySpec;
  private final FileGroupStats deleteManifestsStats;
  private final Map<Integer, FileGroupStats> deleteManifestsStatsBySpec;
  private final long totalManifestsCount;
  private final long totalManifestRecordsCount;
  private final long totalManifestSizeBytes;

  public BaseManifestsSummary(
      Snapshot snapshot,
      Map<Integer, FileGroupStats> dataManifestsStatsBySpec,
      Map<Integer, FileGroupStats> deleteManifestsStatsBySpec) {
    this.snapshot = snapshot;

    this.dataManifestsStats = BaseFileGroupStats.combine(dataManifestsStatsBySpec);
    this.dataManifestsStatsBySpec = dataManifestsStatsBySpec;

    this.deleteManifestsStats = BaseFileGroupStats.combine(deleteManifestsStatsBySpec);
    this.deleteManifestsStatsBySpec = deleteManifestsStatsBySpec;

    this.totalManifestsCount =
        dataManifestsStats.totalFilesCount() + deleteManifestsStats.totalFilesCount();
    this.totalManifestRecordsCount =
        dataManifestsStats.totalRecordsCount() + deleteManifestsStats.totalRecordsCount();
    this.totalManifestSizeBytes =
        dataManifestsStats.totalFilesSizeBytes() + deleteManifestsStats.totalFilesSizeBytes();
  }

  @Override
  public Snapshot snapshot() {
    return snapshot;
  }

  @Override
  public FileGroupStats dataManifestsStats() {
    return dataManifestsStats;
  }

  @Override
  public Map<Integer, FileGroupStats> dataManifestsStatsBySpec() {
    return dataManifestsStatsBySpec;
  }

  @Override
  public FileGroupStats deleteManifestsStats() {
    return deleteManifestsStats;
  }

  @Override
  public Map<Integer, FileGroupStats> deleteManifestsStatsBySpec() {
    return deleteManifestsStatsBySpec;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("MANIFESTS");

    formatter.property("snapshot-id", snapshot.snapshotId());
    formatter.property("total-manifest-files-count", totalManifestsCount);
    formatter.property("total-manifest-records-count", totalManifestRecordsCount);
    formatter.property("total-manifest-files-size-bytes", totalManifestSizeBytes);

    formatter.subHeader("Data Manifests");
    formatter.property("total-data-manifest-files-count", dataManifestsStats.totalFilesCount());
    formatter.property(
        "total-data-manifest-files-size-bytes", dataManifestsStats.totalFilesSizeBytes());
    formatter.statsBySpec("data-manifest", dataManifestsStatsBySpec);

    formatter.subHeader("Delete Manifests");
    formatter.property("total-delete-manifest-files-count", deleteManifestsStats.totalFilesCount());
    formatter.property(
        "total-delete-manifest-files-size-bytes", deleteManifestsStats.totalFilesSizeBytes());
    formatter.statsBySpec("delete-manifest", deleteManifestsStatsBySpec);

    return formatter.compileString();
  }
}
