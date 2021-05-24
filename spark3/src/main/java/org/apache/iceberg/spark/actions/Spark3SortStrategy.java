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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.actions.SortStrategy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.execution.datasources.v2.DistributionAndOrderingUtils$;
import org.apache.spark.sql.internal.SQLConf;

public class Spark3SortStrategy extends SortStrategy {

  public static final String SIZE_ESTIMATE_MULTIPLE = "size-estimate-multiple";

  private final Table table;
  private final SparkSession spark;
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();
  private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

  private double sizeEstimateMultiple;

  public Spark3SortStrategy(Table table, SparkSession spark) {
    this.table = table;
    this.spark = spark;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(SIZE_ESTIMATE_MULTIPLE)
        .build();
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    sizeEstimateMultiple = PropertyUtil.propertyAsDouble(options,
        SIZE_ESTIMATE_MULTIPLE,
        1.0);

    Preconditions.checkArgument(sizeEstimateMultiple > 0,
        "Cannot use Spark3Sort Strategy without %s being positive, found %s",
        SIZE_ESTIMATE_MULTIPLE, sizeEstimateMultiple);

    return super.options(options);
  }


  @Override
  public Set<DataFile> rewriteFiles(String groupID, List<FileScanTask> filesToRewrite) {
    SortOrder[] ordering = Spark3Util.convert(sortOrder());
    Distribution distribution = Distributions.ordered(ordering);

    manager.stageTasks(table, groupID, filesToRewrite);

    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    SparkSession cloneSession = spark.cloneSession();
    cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);

    // Reset Shuffle Partitions for our sort
    long estOutputPartitions = numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple));
    cloneSession.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, estOutputPartitions));

    Dataset<Row> scanDF = cloneSession.read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .load(table.name());

    // write the packed data into new files where each split becomes a new file
    try {
      LogicalPlan sortPlan = DistributionAndOrderingUtils$.MODULE$.prepareQuery(
          distribution, ordering, scanDF.logicalPlan(), cloneSession.sessionState().conf());
      Dataset<Row> sortedDf = new Dataset<>(cloneSession, sortPlan, scanDF.encoder());

      sortedDf.write()
          .format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .option(SparkWriteOptions.DISTRIBUTION_MODE, "none")
          .option(SparkWriteOptions.IGNORE_SORT_ORDER, "true")
          .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
          .mode("append")
          .save(table.name());
    } catch (Exception e) {
      try {
        rewriteCoordinator.abortRewrite(table, groupID);
        manager.removeTasks(table, groupID);
      } finally {
        throw e;
      }
    }

    // Actual commit is performed with the groupID
    return rewriteCoordinator.fetchNewDataFiles(table, ImmutableSet.of(groupID));
  }
}
