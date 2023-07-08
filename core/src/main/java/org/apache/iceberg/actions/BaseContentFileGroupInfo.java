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
import org.apache.iceberg.actions.AnalyzeTable.ContentFileGroupInfo;
import org.apache.iceberg.actions.AnalyzeTable.FileGroupStats;

public class BaseContentFileGroupInfo implements ContentFileGroupInfo {

  private final FileGroupStats stats;
  private final Map<Integer, FileGroupStats> statsBySpec;
  private final String statsByPartitionFileLocation;

  public BaseContentFileGroupInfo(Map<Integer, FileGroupStats> statsBySpec) {
    this(statsBySpec, null);
  }

  public BaseContentFileGroupInfo(
      Map<Integer, FileGroupStats> statsBySpec, String statsByPartitionFileLocation) {
    this.stats = BaseFileGroupStats.combine(statsBySpec);
    this.statsBySpec = statsBySpec;
    this.statsByPartitionFileLocation = statsByPartitionFileLocation;
  }

  @Override
  public FileGroupStats stats() {
    return stats;
  }

  @Override
  public Map<Integer, FileGroupStats> statsBySpec() {
    return statsBySpec;
  }

  @Override
  public String statsByPartitionFileLocation() {
    return statsByPartitionFileLocation;
  }
}
