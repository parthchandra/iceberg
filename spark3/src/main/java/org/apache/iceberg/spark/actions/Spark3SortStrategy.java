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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
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
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.Partition;
import org.apache.spark.rdd.PartitionCoalescer;
import org.apache.spark.rdd.PartitionGroup;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalesce;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.execution.datasources.v2.DistributionAndOrderingUtils$;
import org.apache.spark.sql.internal.SQLConf;
import scala.Option;
import scala.collection.JavaConverters;

public class Spark3SortStrategy extends SortStrategy {

  public static final String SIZE_ESTIMATE_MULTIPLE = "size-estimate-multiple";

  public static final String SHUFFLE_TASKS_PER_FILE = "shuffle-tasks-per-file";
  public static final int SHUFFLE_TASKS_PER_FILE_DEFAULT = 1;

  private final Table table;
  private final SparkSession spark;
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();
  private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

  private double sizeEstimateMultiple;
  private int shuffleTasksPerFile;

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
        .add(SHUFFLE_TASKS_PER_FILE)
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

    shuffleTasksPerFile = PropertyUtil.propertyAsInt(options,
        SHUFFLE_TASKS_PER_FILE,
        SHUFFLE_TASKS_PER_FILE_DEFAULT);

    Preconditions.checkArgument(shuffleTasksPerFile >= 1,
        "Cannot use Spark3Sort Strategy as option %s must be >= 1, found %s",
        SHUFFLE_TASKS_PER_FILE, shuffleTasksPerFile);

    return super.options(options);
  }


  @Override
  public Set<DataFile> rewriteFiles(String groupID, List<FileScanTask> filesToRewrite) {
    boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table.spec());
    SortOrder[] ordering;
    Distribution distribution;
    ordering = Spark3Util.convert(sortOrder());
    if (requiresRepartition) {
      distribution = Spark3Util.buildRequiredDistribution(DistributionMode.RANGE, table);
      ordering = Spark3Util.buildRequiredOrdering(distribution, table().schema(), table.spec(), sortOrder());
    } else {
      distribution = Distributions.ordered(ordering);
    }

    manager.stageTasks(table, groupID, filesToRewrite);

    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    SparkSession cloneSession = spark.cloneSession();
    cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);

    // Reset Shuffle Partitions for our sort
    long numOutputFiles = numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple));
    long numShufflePartitions = numOutputFiles * shuffleTasksPerFile;
    cloneSession.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, numShufflePartitions));

    Dataset<Row> scanDF = cloneSession.read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .load(table.name());

    // write the packed data into new files where each split becomes a new file
    try {
      SQLConf sqlConf = cloneSession.sessionState().conf();
      LogicalPlan sortPlan = sortPlan(distribution, ordering, numOutputFiles, scanDF.logicalPlan(), sqlConf);
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

  private LogicalPlan sortPlan(Distribution distribution, SortOrder[] ordering, long numOutputFiles,
                               LogicalPlan plan, SQLConf conf) {
    LogicalPlan sortPlan = DistributionAndOrderingUtils$.MODULE$.prepareQuery(distribution, ordering, plan, conf);
    if (shuffleTasksPerFile == 1) {
      return sortPlan;
    } else {
      OrderAwareCoalescer coalescer = new OrderAwareCoalescer(shuffleTasksPerFile);
      // it should be safe to assume we have less than 2 billion files at this point
      return new OrderAwareCoalesce((int) numOutputFiles, coalescer, sortPlan);
    }
  }

  private static class OrderAwareCoalescer implements PartitionCoalescer, scala.Serializable {
    private final int shuffleTasksPerFile;

    OrderAwareCoalescer(int shuffleTasksPerFile) {
      this.shuffleTasksPerFile = shuffleTasksPerFile;
    }

    @Override
    public PartitionGroup[] coalesce(int maxPartitions, RDD<?> parent) {
      // use lookback as 1 to preserve the ordering
      BinPacking.ListPacker<Partition> packer = new BinPacking.ListPacker<>(shuffleTasksPerFile, 1, false);
      List<List<Partition>> partitionBins = packer.pack(Arrays.asList(parent.partitions()), partition -> 1L);
      return partitionBins.stream().map(bin -> {
        PartitionGroup partitionGroup = new PartitionGroup(Option.empty());
        JavaConverters.bufferAsJavaList(partitionGroup.partitions()).addAll(bin);
        return partitionGroup;
      }).toArray(PartitionGroup[]::new);
    }
  }
}
