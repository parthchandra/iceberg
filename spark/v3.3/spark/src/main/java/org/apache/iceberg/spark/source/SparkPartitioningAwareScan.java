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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;

abstract class SparkPartitioningAwareScan<T extends PartitionScanTask> extends SparkScan
    implements SupportsReportPartitioning {

  private final Scan<?, ? extends ScanTask, ?> scan;
  private final boolean preserveDataGrouping;

  private Set<PartitionSpec> specs = null; // lazy cache of scanned specs
  private List<T> tasks = null; // lazy cache of uncombined tasks
  private List<ScanTaskGroup<T>> taskGroups = null; // lazy cache of task groups
  private StructType taskGroupKeyType = null; // lazy task group key type
  private StructLikeSet taskGroupKeys = null; // lazy task group keys

  SparkPartitioningAwareScan(
      SparkSession spark,
      Table table,
      Scan<?, ? extends ScanTask, ?> scan,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {

    super(spark, table, readConf, expectedSchema, filters);

    this.scan = scan;
    this.preserveDataGrouping = readConf.preserveDataGrouping();

    if (scan == null) {
      this.specs = Collections.emptySet();
      this.tasks = Collections.emptyList();
      this.taskGroups = Collections.emptyList();
    }
  }

  protected Scan<?, ? extends ScanTask, ?> scan() {
    return scan;
  }

  @Override
  public Partitioning outputPartitioning() {
    Preconditions.checkState(taskGroups() != null, "Task groups must be planned");

    if (taskGroupKeyType().fields().size() > 0) {
      return new KeyGroupedPartitioning(taskGroupKeyTransforms(), taskGroups().size());
    } else {
      return new UnknownPartitioning(taskGroups().size());
    }
  }

  // the task group key type must be computed only once and must not change during runtime filtering
  private StructType taskGroupKeyType() {
    if (taskGroupKeyType == null) {
      if (preserveDataGrouping) {
        this.taskGroupKeyType = org.apache.iceberg.Partitioning.groupingKeyType(specs());
      } else {
        this.taskGroupKeyType = StructType.of();
      }
    }

    return taskGroupKeyType;
  }

  private Transform[] taskGroupKeyTransforms() {
    Set<Integer> keyFieldIds =
        taskGroupKeyType().fields().stream()
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toSet());

    List<PartitionField> keyFields = Lists.newArrayList();
    Set<Integer> addedKeyFieldIds = Sets.newHashSet();

    for (PartitionSpec spec : specs()) {
      for (PartitionField field : spec.fields()) {
        if (keyFieldIds.contains(field.fieldId()) && !addedKeyFieldIds.contains(field.fieldId())) {
          keyFields.add(field);
          addedKeyFieldIds.add(field.fieldId());
        }
      }
    }

    return Spark3Util.toTransforms(table().schema(), keyFields);
  }

  protected Set<PartitionSpec> specs() {
    if (specs == null) {
      Set<PartitionSpec> taskSpecs = Sets.newHashSet();
      for (T task : tasks()) {
        taskSpecs.add(task.spec());
      }
      this.specs = taskSpecs;
    }

    return specs;
  }

  @SuppressWarnings("unchecked")
  protected List<T> tasks() {
    if (tasks == null) {
      try (CloseableIterable<? extends ScanTask> tasksIterable = scan.planFiles()) {
        List<T> partitionScanTasks = Lists.newArrayList();
        for (ScanTask task : tasksIterable) {
          partitionScanTasks.add((T) task);
        }
        this.tasks = partitionScanTasks;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close scan: " + scan, e);
      }
    }

    return tasks;
  }

  @Override
  protected List<? extends ScanTaskGroup<? extends ScanTask>> taskGroups() {
    if (taskGroups == null) {
      if (taskGroupKeyType().fields().size() > 0) {
        List<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                tasks(),
                scan.targetSplitSize(),
                scan.splitLookback(),
                scan.splitOpenFileCost(),
                taskGroupKeyType());

        if (taskGroupKeys == null) {
          this.taskGroups = plannedTaskGroups;
          this.taskGroupKeys = groupingKeys(plannedTaskGroups);

        } else {
          // honor the reported partitioning while re-planing task groups after runtime filtering
          // the number of task groups may change but the set of task group keys must be same
          // that's why an empty task group is added for each filtered out key
          StructLikeSet plannedTaskGroupKeys = groupingKeys(plannedTaskGroups);
          StructLikeSet missingTaskGroupKeys = StructLikeSet.create(taskGroupKeyType());

          for (StructLike key : taskGroupKeys) {
            if (!plannedTaskGroupKeys.contains(key)) {
              missingTaskGroupKeys.add(key);
            }
          }

          if (missingTaskGroupKeys.size() > 0) {
            for (StructLike key : missingTaskGroupKeys) {
              plannedTaskGroups.add(
                  new BaseScanTaskGroup<>(taskGroupKeyType(), key, Collections.emptyList()));
            }
          }

          this.taskGroups = plannedTaskGroups;
        }

      } else {
        CloseableIterable<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                CloseableIterable.withNoopClose(tasks()),
                scan.targetSplitSize(),
                scan.splitLookback(),
                scan.splitOpenFileCost());
        this.taskGroups = Lists.newArrayList(plannedTaskGroups);
      }
    }

    return taskGroups;
  }

  private StructLikeSet groupingKeys(List<ScanTaskGroup<T>> scanTaskGroups) {
    StructLikeSet keys = StructLikeSet.create(taskGroupKeyType());

    for (ScanTaskGroup<T> taskGroup : scanTaskGroups) {
      keys.add(taskGroup.groupingKey());
    }

    return keys;
  }

  protected void resetTasks(List<T> filteredTasks) {
    this.taskGroups = null;
    this.tasks = filteredTasks;
  }
}
