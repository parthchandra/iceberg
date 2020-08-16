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

package org.apache.iceberg.spark.source;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitOperations {

  private static final Logger LOG = LoggerFactory.getLogger(CommitOperations.class);

  public interface CommitOperation<T> {
    SnapshotUpdate<T> prepareSnapshotUpdate(Table table, Iterable<DataFile> newFiles);
    String description();
    default boolean withLocking() {
      return false;
    }
  }

  public static class Append implements CommitOperation<AppendFiles> {
    private static final Append INSTANCE = new Append();

    public static Append get() {
      return INSTANCE;
    }

    @Override
    public AppendFiles prepareSnapshotUpdate(Table table, Iterable<DataFile> newFiles) {
      AppendFiles append = table.newAppend();

      int numFiles = 0;
      for (DataFile file : newFiles) {
        numFiles += 1;
        append.appendFile(file);
      }

      LOG.info("Preparing {} with {} files", description(), numFiles);

      return append;
    }

    @Override
    public String description() {
      return "append";
    }
  }

  public static class DynamicPartitionOverwrite implements CommitOperation<ReplacePartitions> {
    private static final DynamicPartitionOverwrite INSTANCE = new DynamicPartitionOverwrite();

    public static DynamicPartitionOverwrite get() {
      return INSTANCE;
    }

    @Override
    public ReplacePartitions prepareSnapshotUpdate(Table table, Iterable<DataFile> newFiles) {
      ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

      int numFiles = 0;
      for (DataFile file : newFiles) {
        numFiles += 1;
        dynamicOverwrite.addFile(file);
      }

      LOG.info("Preparing {} with {} files", description(), numFiles);

      return dynamicOverwrite;
    }

    @Override
    public String description() {
      return "dynamic partition overwrite";
    }
  }

  public static class Overwrite implements CommitOperation<OverwriteFiles> {
    private final Expression overwriteRowFilter;
    private final boolean validateAddedFilesMatchOverwriteFilter;
    private final Iterable<DataFile> deletedFiles;
    private final Long readSnapshotId;
    private final Expression conflictDetectionFilter;

    private Overwrite(
        Expression overwriteRowFilter,
        boolean validateAddedFilesMatchOverwriteFilter,
        Iterable<DataFile> deletedFiles,
        Long readSnapshotId,
        Expression conflictDetectionFilter) {

      this.overwriteRowFilter = overwriteRowFilter;
      this.validateAddedFilesMatchOverwriteFilter = validateAddedFilesMatchOverwriteFilter;
      this.deletedFiles = deletedFiles;
      this.readSnapshotId = readSnapshotId;
      this.conflictDetectionFilter = conflictDetectionFilter;
    }

    public static Overwrite byRowFilter(
        Expression overwriteRowFilter,
        boolean validateAddedFilesMatchOverwriteFilter) {

      return new Overwrite(overwriteRowFilter, validateAddedFilesMatchOverwriteFilter, ImmutableSet.of(), null, null);
    }

    public static Overwrite files(
        Iterable<DataFile> deletedFiles,
        Long readSnapshotId,
        Expression conflictDetectionFilter) {

      return new Overwrite(null, false, deletedFiles, readSnapshotId, conflictDetectionFilter);
    }

    public static Overwrite files(Iterable<DataFile> deletedFiles) {
      return new Overwrite(null, false, deletedFiles, null, null);
    }

    @Override
    public OverwriteFiles prepareSnapshotUpdate(Table table, Iterable<DataFile> newFiles) {
      OverwriteFiles overwrite = table.newOverwrite();

      deletedFiles.forEach(overwrite::deleteFile);
      newFiles.forEach(overwrite::addFile);

      if (overwriteRowFilter != null) {
        overwrite.overwriteByRowFilter(overwriteRowFilter);
      }

      if (validateAddedFilesMatchOverwriteFilter) {
        overwrite.validateAddedFilesMatchOverwriteFilter();
      }

      if (conflictDetectionFilter != null) {
        overwrite.validateNoConflictingAppends(readSnapshotId, conflictDetectionFilter);
      }

      LOG.info(
          "Preparing overwrite files with {} deleted and {} new files, overwrite filter {}, " +
              "validateAddedFiles {}, readSnapshotId {}, conflict detection filter {}",
          Iterables.size(deletedFiles), Iterables.size(newFiles), overwriteRowFilter,
          validateAddedFilesMatchOverwriteFilter, readSnapshotId, conflictDetectionFilter);

      return overwrite;
    }

    @Override
    public String description() {
      return "overwrite files";
    }
  }

  public static class Rewrite implements CommitOperation<RewriteFiles> {
    private final Iterable<DataFile> deletedFiles;
    private final boolean withLocking;

    private Rewrite(Iterable<DataFile> deletedFiles, boolean withLocking) {
      this.deletedFiles = deletedFiles;
      this.withLocking = withLocking;
    }

    public static Rewrite files(Iterable<DataFile> deletedFiles, boolean lock) {
      return new Rewrite(deletedFiles, lock);
    }

    @Override
    public RewriteFiles prepareSnapshotUpdate(Table table, Iterable<DataFile> newFiles) {
      RewriteFiles rewriteFiles = table.newRewrite()
          .rewriteFiles(ImmutableSet.copyOf(deletedFiles), ImmutableSet.copyOf(newFiles));

      LOG.info(
          "Preparing rewrite files with {} deleted and {} new files",
          Iterables.size(deletedFiles), Iterables.size(newFiles));

      return rewriteFiles;
    }

    @Override
    public boolean withLocking() {
      return withLocking;
    }

    @Override
    public String description() {
      return String.format("rewrite files (with locking: %b)", withLocking);
    }
  }
}
