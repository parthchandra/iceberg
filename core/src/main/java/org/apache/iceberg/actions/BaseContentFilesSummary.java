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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.actions.AnalyzeTable.ContentFileGroupInfo;
import org.apache.iceberg.actions.AnalyzeTable.ContentFilesSummary;

public class BaseContentFilesSummary implements ContentFilesSummary {

  private final Snapshot snapshot;
  private final ContentFileGroupInfo dataFilesInfo;
  private final ContentFileGroupInfo positionDeleteFilesInfo;
  private final ContentFileGroupInfo equalityDeleteFilesInfo;

  public BaseContentFilesSummary(
      Snapshot snapshot,
      ContentFileGroupInfo dataFilesInfo,
      ContentFileGroupInfo positionDeleteFilesInfo,
      ContentFileGroupInfo equalityDeleteFilesInfo) {
    this.snapshot = snapshot;
    this.dataFilesInfo = dataFilesInfo;
    this.positionDeleteFilesInfo = positionDeleteFilesInfo;
    this.equalityDeleteFilesInfo = equalityDeleteFilesInfo;
  }

  @Override
  public Snapshot snapshot() {
    return snapshot;
  }

  @Override
  public ContentFileGroupInfo dataFilesInfo() {
    return dataFilesInfo;
  }

  @Override
  public ContentFileGroupInfo positionDeleteFilesInfo() {
    return positionDeleteFilesInfo;
  }

  @Override
  public ContentFileGroupInfo equalityDeleteFilesInfo() {
    return equalityDeleteFilesInfo;
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("CONTENT FILES");

    formatter.property("snapshot-id", snapshot.snapshotId());

    long dataFilesCount = dataFilesInfo.stats().totalFilesCount();
    long positionDeleteFilesCount = positionDeleteFilesInfo.stats().totalFilesCount();
    long equalityDeleteFilesCount = equalityDeleteFilesInfo.stats().totalFilesCount();
    long totalFilesCount = dataFilesCount + positionDeleteFilesCount + equalityDeleteFilesCount;
    formatter.property("total-files-count", totalFilesCount);

    formatter.subHeader("Data Files");
    formatter.contentFileGroupInfo("data", dataFilesInfo);

    formatter.subHeader("Position Delete Files");
    formatter.contentFileGroupInfo("position-delete", positionDeleteFilesInfo);

    formatter.subHeader("Equality Delete Files");
    formatter.contentFileGroupInfo("equality-delete", equalityDeleteFilesInfo);

    return formatter.compileString();
  }
}
