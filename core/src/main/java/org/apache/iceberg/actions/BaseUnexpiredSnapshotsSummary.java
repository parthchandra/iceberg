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

import org.apache.iceberg.actions.AnalyzeTable.UnexpiredSnapshotsSummary;

public class BaseUnexpiredSnapshotsSummary implements UnexpiredSnapshotsSummary {

  private final long olderThanTimestampMs;
  private final int unexpiredSnapshotsCount;
  private final long unexpiredDataFilesCount;
  private final long unexpiredPositionDeleteFilesCount;
  private final long unexpiredEqualityDeleteFilesCount;

  public BaseUnexpiredSnapshotsSummary(
      long olderThanTimestampMs,
      int unexpiredSnapshotsCount,
      long unexpiredDataFilesCount,
      long unexpiredPositionDeleteFilesCount,
      long unexpiredEqualityDeleteFilesCount) {
    this.olderThanTimestampMs = olderThanTimestampMs;
    this.unexpiredSnapshotsCount = unexpiredSnapshotsCount;
    this.unexpiredDataFilesCount = unexpiredDataFilesCount;
    this.unexpiredPositionDeleteFilesCount = unexpiredPositionDeleteFilesCount;
    this.unexpiredEqualityDeleteFilesCount = unexpiredEqualityDeleteFilesCount;
  }

  @Override
  public long olderThanTimestampMs() {
    return olderThanTimestampMs;
  }

  @Override
  public int unexpiredSnapshotsCount() {
    return unexpiredSnapshotsCount;
  }

  @Override
  public long unexpiredDataFilesCount() {
    return unexpiredDataFilesCount;
  }

  @Override
  public long unexpiredPositionDeleteFilesCount() {
    return unexpiredPositionDeleteFilesCount;
  }

  @Override
  public long unexpiredEqualityDeleteFilesCount() {
    return unexpiredEqualityDeleteFilesCount;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("UNEXPIRED SNAPSHOTS");

    formatter.property("snapshots-older-than-ms", olderThanTimestampMs);
    formatter.property("unexpired-snapshots-count", unexpiredSnapshotsCount);
    formatter.property("unexpired-data-files-count", unexpiredDataFilesCount);
    formatter.property("unexpired-position-delete-files-count", unexpiredPositionDeleteFilesCount);
    formatter.property("unexpired-equality-delete-files-count", unexpiredEqualityDeleteFilesCount);

    return formatter.compileString();
  }
}
