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
package org.apache.iceberg.spark.actions;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseCheckSnapshotIntegrityResult;
import org.apache.iceberg.actions.CheckSnapshotIntegrity;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckSnapshotIntegritySparkAction
    extends BaseSparkAction<CheckSnapshotIntegritySparkAction> implements CheckSnapshotIntegrity {

  private static final Logger LOG =
      LoggerFactory.getLogger(CheckSnapshotIntegritySparkAction.class);
  private static final ExecutorService DEFAULT_EXECUTOR_SERVICE = null;

  private final Table table;
  private final Set<String> missingFiles = Collections.synchronizedSet(Sets.newHashSet());
  private ExecutorService executorService = DEFAULT_EXECUTOR_SERVICE;
  private String targetVersion;
  private Table targetTable;
  private boolean completeCheck = false;

  private Consumer<String> validateFunc =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          try {
            if (!table.io().newInputFile(file).exists()) {
              missingFiles.add(file);
            }
          } catch (Exception e) {
            LOG.warn("Failed to check the existence of file: {}. Marking it as missing.", file, e);
            missingFiles.add(file);
          }
        }
      };

  CheckSnapshotIntegritySparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected CheckSnapshotIntegritySparkAction self() {
    return this;
  }

  @Override
  public CheckSnapshotIntegritySparkAction executeWith(ExecutorService service) {
    this.executorService = service;
    return this;
  }

  @Override
  public CheckSnapshotIntegritySparkAction targetVersion(String tVersion) {
    Preconditions.checkArgument(
        tVersion != null && !tVersion.isEmpty(),
        "Target version file('%s') cannot be empty.",
        tVersion);

    String tVersionFile = tVersion;
    if (!tVersionFile.contains(File.separator)) {
      tVersionFile = ((HasTableOperations) table).operations().metadataFileLocation(tVersionFile);
    }

    Preconditions.checkArgument(
        fileExist(tVersionFile), "Version file('%s') doesn't exist.", tVersionFile);
    this.targetVersion = tVersionFile;
    return this;
  }

  @Override
  public CheckSnapshotIntegrity completeCheck(boolean completeCheckFlag) {
    this.completeCheck = completeCheckFlag;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    JobGroupInfo info = newJobGroupInfo("CHECK-SNAPSHOT-INTEGRITY", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private void validateInputs() {
    Preconditions.checkArgument(
        !this.completeCheck || this.targetVersion != null,
        "completeCheck can only be used when targetVersion is set.");
  }

  private String jobDesc() {
    return String.format(
        "Checking integrity of version '%s' of table %s.", targetVersion, table.name());
  }

  private Result doExecute() {
    targetTable = newStaticTable(targetVersion, table);

    List<String> filesToCheck = filesToCheck();

    Tasks.foreach(filesToCheck)
        .noRetry()
        .suppressFailureWhenFinished()
        .executeWith(executorService)
        .run(validateFunc::accept);

    return new BaseCheckSnapshotIntegrityResult(missingFiles);
  }

  private List<String> filesToCheck() {
    Dataset<Row> targetFileDF = fileDS(targetTable).select("path");
    if (!completeCheck) {
      // check only incremental files
      Dataset<Row> currentFileDF = fileDS(table).select("path");
      targetFileDF = targetFileDF.except(currentFileDF);
    }
    return targetFileDF.as(Encoders.STRING()).collectAsList();
  }

  private Dataset<FileInfo> fileDS(Table tbl) {
    Dataset<FileInfo> contentFileDS = contentFileDS(tbl);
    Dataset<FileInfo> metadataFileDS = metadataFileDS(tbl);
    return contentFileDS.union(metadataFileDS);
  }

  private Dataset<FileInfo> metadataFileDS(Table tbl) {
    return manifestDS(tbl).union(manifestListDS(tbl)).union(otherMetadataFileDS(tbl));
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }
}
