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
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.BaseRemoveExpiredFilesActionResult;
import org.apache.iceberg.actions.RemoveExpiredFiles;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveExpiredFilesSparkAction extends BaseSparkAction<RemoveExpiredFilesSparkAction>
    implements RemoveExpiredFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveExpiredFilesSparkAction.class);
  private static final ExecutorService DEFAULT_EXECUTOR_SERVICE = null;
  private static final String DATA_FILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";

  private final Table table;
  private ExecutorService executorService = DEFAULT_EXECUTOR_SERVICE;
  private String targetVersion;
  private Table targetTable;

  private final Consumer<String> deleteFunc =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          table.io().deleteFile(file);
        }
      };

  RemoveExpiredFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RemoveExpiredFilesSparkAction self() {
    return this;
  }

  @Override
  public RemoveExpiredFilesSparkAction executeWith(ExecutorService service) {
    this.executorService = service;
    return this;
  }

  @Override
  public RemoveExpiredFilesSparkAction targetVersion(String tVersion) {
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
  public Result execute() {
    JobGroupInfo info = newJobGroupInfo("REMOVE-EXPIRED-FILES", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private String jobDesc() {
    return String.format(
        "Remove expired files in version '%s' of table %s.", targetVersion, table.name());
  }

  private Result doExecute() {
    targetTable = newStaticTable(targetVersion, table);

    return deleteFiles(expiredFiles().collectAsList().iterator());
  }

  private Dataset<FileInfo> expiredFiles() {
    Dataset<FileInfo> originalFiles = buildValidFileDS(currentMetadata(table));
    Dataset<FileInfo> validFiles = buildValidFileDS(currentMetadata(targetTable));
    return originalFiles.except(validFiles);
  }

  private TableMetadata currentMetadata(Table tbl) {
    return ((HasTableOperations) tbl).operations().current();
  }

  private Dataset<FileInfo> buildValidFileDS(TableMetadata metadata) {
    Table staticTable = newStaticTable(metadata.metadataFileLocation(), table);
    return contentFileDS(staticTable)
        .union(manifestDS(staticTable))
        .union(manifestListDS(staticTable));
  }

  private Result deleteFiles(Iterator<FileInfo> filesToDelete) {
    DeleteSummary summary = deleteFiles(executorService, deleteFunc, filesToDelete);
    LOG.info("Deleted {} total files", summary.totalFilesCount());

    return new BaseRemoveExpiredFilesActionResult(
        summary.dataFilesCount(), summary.manifestsCount(), summary.manifestListsCount());
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }
}
