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
import org.apache.iceberg.actions.AnalyzeTable.FileGroupStats;

public class BaseFileGroupStats implements FileGroupStats {

  private final long totalFilesCount;
  private final long totalRecordsCount;
  private final long totalFileSizeBytes;
  private final Double avgFileSizeBytes;
  private final Long minFileSizeBytes;
  private final Long maxFileSizeBytes;
  private final Double percentile25thFileSizeBytes;
  private final Double percentile75thFileSizeBytes;

  public BaseFileGroupStats(
      long totalFilesCount,
      long totalRecordsCount,
      long totalFileSizeBytes,
      Double avgFileSizeBytes,
      Long minFileSizeBytes,
      Long maxFileSizeBytes) {
    this(
        totalFilesCount,
        totalRecordsCount,
        totalFileSizeBytes,
        avgFileSizeBytes,
        minFileSizeBytes,
        maxFileSizeBytes,
        null,
        null);
  }

  public BaseFileGroupStats(
      long totalFilesCount,
      long totalRecordsCount,
      long totalFileSizeBytes,
      Double avgFileSizeBytes,
      Long minFileSizeBytes,
      Long maxFileSizeBytes,
      Double percentile25thFileSizeBytes,
      Double percentile75thFileSizeBytes) {
    this.totalFilesCount = totalFilesCount;
    this.totalRecordsCount = totalRecordsCount;
    this.totalFileSizeBytes = totalFileSizeBytes;
    this.avgFileSizeBytes = avgFileSizeBytes;
    this.minFileSizeBytes = minFileSizeBytes;
    this.maxFileSizeBytes = maxFileSizeBytes;
    this.percentile25thFileSizeBytes = percentile25thFileSizeBytes;
    this.percentile75thFileSizeBytes = percentile75thFileSizeBytes;
  }

  public static FileGroupStats empty() {
    return new BaseFileGroupStats(0L, 0L, 0L, null, null, null);
  }

  public static FileGroupStats combine(Map<Integer, FileGroupStats> statsMap) {
    return statsMap.values().stream()
        .reduce(FileGroupStats::combine)
        .orElseGet(BaseFileGroupStats::empty);
  }

  @Override
  public long totalFilesCount() {
    return totalFilesCount;
  }

  @Override
  public long totalRecordsCount() {
    return totalRecordsCount;
  }

  @Override
  public long totalFilesSizeBytes() {
    return totalFileSizeBytes;
  }

  @Override
  public Double avgFileSizeBytes() {
    return avgFileSizeBytes;
  }

  @Override
  public Long minFileSizeBytes() {
    return minFileSizeBytes;
  }

  @Override
  public Long maxFileSizeBytes() {
    return maxFileSizeBytes;
  }

  @Override
  public Double percentile25thFileSizeBytes() {
    return percentile25thFileSizeBytes;
  }

  @Override
  public Double percentile75thFileSizeBytes() {
    return percentile75thFileSizeBytes;
  }

  @Override
  public FileGroupStats combine(FileGroupStats other) {
    long newTotalFilesCount = totalFilesCount + other.totalFilesCount();
    long newTotalRecordsCount = totalRecordsCount + other.totalRecordsCount();
    long newTotalFilesSizeBytes = totalFileSizeBytes + other.totalFilesSizeBytes();

    Double newAvgFileSizeBytes;
    if (newTotalFilesSizeBytes == 0 || newTotalFilesCount == 0) {
      newAvgFileSizeBytes = null;
    } else {
      newAvgFileSizeBytes = ((double) newTotalFilesSizeBytes) / newTotalFilesCount;
    }

    Long newMinFileSizeBytes;
    if (minFileSizeBytes == null || other.minFileSizeBytes() == null) {
      newMinFileSizeBytes = null;
    } else {
      newMinFileSizeBytes = Math.min(minFileSizeBytes, other.minFileSizeBytes());
    }

    Long newMaxFileSizeBytes;
    if (maxFileSizeBytes == null || other.maxFileSizeBytes() == null) {
      newMaxFileSizeBytes = null;
    } else {
      newMaxFileSizeBytes = Math.max(maxFileSizeBytes, other.maxFileSizeBytes());
    }

    return new BaseFileGroupStats(
        newTotalFilesCount,
        newTotalRecordsCount,
        newTotalFilesSizeBytes,
        newAvgFileSizeBytes,
        newMinFileSizeBytes,
        newMaxFileSizeBytes);
  }
}
