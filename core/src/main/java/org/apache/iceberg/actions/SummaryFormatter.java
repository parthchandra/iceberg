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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.actions.AnalyzeTable.ContentFileGroupInfo;
import org.apache.iceberg.actions.AnalyzeTable.FileGroupStats;

public class SummaryFormatter {

  private static final String NEW_LINE = "\n";

  private final StringBuilder builder;

  public SummaryFormatter() {
    this.builder = new StringBuilder();
  }

  public SummaryFormatter header(String header) {
    builder.append(String.format("=== %s ===", header));
    builder.append(NEW_LINE).append(NEW_LINE);
    return this;
  }

  public SummaryFormatter subHeader(String subHeader) {
    builder.append(NEW_LINE);
    builder.append(String.format(":: %s ::", subHeader));
    builder.append(NEW_LINE).append(NEW_LINE);
    return this;
  }

  public SummaryFormatter subSubHeader(String subSubHeader) {
    builder.append(NEW_LINE);
    builder.append(String.format("++ %s ++", subSubHeader));
    builder.append(NEW_LINE).append(NEW_LINE);
    return this;
  }

  public SummaryFormatter newLine() {
    builder.append(NEW_LINE);
    return this;
  }

  public SummaryFormatter object(Object object) {
    builder.append(object).append(NEW_LINE);
    return this;
  }

  public SummaryFormatter propertyMap(String name, Map<String, ?> map) {
    builder.append(name).append(":").append(NEW_LINE);
    map.keySet().stream()
        .sorted()
        .forEach(
            key -> {
              Object value = map.get(key);
              builder.append("  ").append(key).append(": ").append(value).append("\n");
            });
    return this;
  }

  public SummaryFormatter property(String name, Object value) {
    builder.append(String.format("%s: %s", name, value)).append(NEW_LINE);
    return this;
  }

  public SummaryFormatter contentFileGroupInfo(String key, ContentFileGroupInfo info) {
    property(String.format("total-%s-files-count", key), info.stats().totalFilesCount());
    property(String.format("total-%s-records-count", key), info.stats().totalRecordsCount());
    property(String.format("total-%s-files-size-bytes", key), info.stats().totalFilesSizeBytes());
    property("partition-stats-file-location", info.statsByPartitionFileLocation());
    statsBySpec(key, info.statsBySpec());
    return this;
  }

  public SummaryFormatter statsBySpec(String key, Map<Integer, FileGroupStats> statsBySpec) {
    List<Integer> specIds =
        statsBySpec.keySet().stream()
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toList());

    for (int specId : specIds) {
      FileGroupStats specStats = statsBySpec.get(specId);

      newLine();

      property("spec-id", specId);
      property(String.format("total-%s-files-count", key), specStats.totalFilesCount());
      property(String.format("total-%s-records-count", key), specStats.totalRecordsCount());
      property(String.format("total-%s-files-size-bytes", key), specStats.totalFilesSizeBytes());
      property(String.format("avg-%s-file-size-bytes", key), specStats.avgFileSizeBytes());
      property(String.format("min-%s-file-size-bytes", key), specStats.minFileSizeBytes());
      property(String.format("max-%s-file-size-bytes", key), specStats.maxFileSizeBytes());
      property(
          String.format("25th-percentile-%s-file-size-bytes", key),
          specStats.percentile25thFileSizeBytes());
      property(
          String.format("75th-percentile-%s-file-size-bytes", key),
          specStats.percentile75thFileSizeBytes());
    }

    return this;
  }

  public String compileString() {
    return builder.toString();
  }
}
