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
package org.apache.iceberg.flink.actions.operators;

import java.util.Objects;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class DataFileRewriteTask {
  private Key key;
  private Init init;
  private FileScanTask task;

  public DataFileRewriteTask(
      long timestamp,
      SerializableTable table,
      int groupsPerCommit,
      long splitSize,
      RewriteDataFiles.FileGroupInfo info,
      FileScanTask task,
      boolean first) {
    this.key = new Key(timestamp, info);
    this.init = first ? new Init(table, groupsPerCommit, splitSize) : null;
    this.task = task;
  }

  public DataFileRewriteTask() {
    // So this is a POJO
  }

  public Key getKey() {
    return key;
  }

  public void setKey(Key key) {
    this.key = key;
  }

  public Init getInit() {
    return init;
  }

  public void setInit(Init init) {
    this.init = init;
  }

  public FileScanTask getTask() {
    return task;
  }

  public void setTask(FileScanTask task) {
    this.task = task;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("init", init)
        .add("task", task)
        .toString();
  }

  public static class Key {
    private long timestamp;
    private int globalIndex;
    private int partitionIndex;
    private StructLike partition;

    public Key() {
      // So this is a POJO
    }

    public Key(long timestamp, RewriteDataFiles.FileGroupInfo info) {
      this.timestamp = timestamp;
      this.globalIndex = info.globalIndex();
      this.partitionIndex = info.partitionIndex();
      this.partition = info.partition();
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public int getGlobalIndex() {
      return globalIndex;
    }

    public void setGlobalIndex(int globalIndex) {
      this.globalIndex = globalIndex;
    }

    public int getPartitionIndex() {
      return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
      this.partitionIndex = partitionIndex;
    }

    public StructLike getPartition() {
      return partition;
    }

    public void setPartition(StructLike partition) {
      this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key key = (Key) o;
      return timestamp == key.timestamp
          && globalIndex == key.globalIndex
          && partitionIndex == key.partitionIndex
          && Objects.equals(partition.toString(), key.partition.toString());
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, globalIndex, partitionIndex, partition.toString());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("timestamp", timestamp)
          .add("globalIndex", globalIndex)
          .add("partitionIndex", partitionIndex)
          .add("partition", partition)
          .toString();
    }
  }

  public static class Init {
    private SerializableTable table;
    private int groupsPerCommit;
    private long splitSize;

    public Init() {
      // So this is a POJO
    }

    public Init(SerializableTable table, int groupsPerCommit, long splitSize) {
      this.table = table;
      this.groupsPerCommit = groupsPerCommit;
      this.splitSize = splitSize;
    }

    public SerializableTable getTable() {
      return table;
    }

    public void setTable(SerializableTable table) {
      this.table = table;
    }

    public int getGroupsPerCommit() {
      return groupsPerCommit;
    }

    public void setGroupsPerCommit(int groupsPerCommit) {
      this.groupsPerCommit = groupsPerCommit;
    }

    public long getSplitSize() {
      return splitSize;
    }

    public void setSplitSize(long splitSize) {
      this.splitSize = splitSize;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Init init = (Init) o;
      return groupsPerCommit == init.groupsPerCommit
          && splitSize == init.splitSize
          && Objects.equals(table.currentSnapshot(), init.table.currentSnapshot());
    }

    @Override
    public int hashCode() {
      return Objects.hash(table.currentSnapshot(), groupsPerCommit, splitSize);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("table", table.name())
          .add("groupsPerCommit", groupsPerCommit)
          .add("splitSize", splitSize)
          .toString();
    }
  }
}
