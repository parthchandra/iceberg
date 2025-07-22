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
package org.apache.iceberg.flink.actions.streams;

import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.SizeBasedDataRewriter;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.flink.actions.operators.DataFileRewriteExecutor;
import org.apache.iceberg.flink.actions.operators.DataFileRewritePlanner;
import org.apache.iceberg.flink.actions.operators.DataFileRewriteTask;
import org.apache.iceberg.flink.actions.operators.DataFileUpdater;
import org.apache.iceberg.flink.actions.operators.ErrorAggregator;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Creates the data file rewriter data stream. Which runs a single iteration of the task for every
 * input event.
 *
 * <p>The input is a {@link DataStream} with {@link SerializableTable} events and every event should
 * be immediately followed by a {@link org.apache.flink.streaming.api.watermark.Watermark} with the
 * same timestamp as the event.
 *
 * <p>The output is a {@link DataStream} with the result of the run (success or failure) followed by
 * the {@link org.apache.flink.streaming.api.watermark.Watermark}.
 */
public class RewriteDataFiles {
  static final String PLANNER_TASK_NAME = "RDF Planner";
  static final String REWRITE_TASK_NAME = "Rewrite";
  static final String COMMIT_TASK_NAME = "Rewrite commit";

  private RewriteDataFiles() {
    // Do not instantiate directly
  }

  /** Creates the builder for creating a stream which rewrites data files for the table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<Builder> {
    private boolean partialProgressEnabled = false;
    private int partialProgressMaxCommits = 10;
    private final Map<String, String> rewriteOptions = Maps.newHashMapWithExpectedSize(6);
    private long maxRewriteBytes = Long.MAX_VALUE;

    /**
     * Allows committing compacted data files in batches. For more details description see {@link
     * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_ENABLED}.
     *
     * @param newPartialProgressEnabled to enable partial commits
     * @return for chained calls
     */
    public Builder partialProgressEnabled(boolean newPartialProgressEnabled) {
      this.partialProgressEnabled = newPartialProgressEnabled;
      return this;
    }

    /**
     * Configures the size of batches if {@link #partialProgressEnabled}. For more details
     * description see {@link
     * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_MAX_COMMITS}.
     *
     * @param newPartialProgressMaxCommits to target number of the commits per run
     * @return for chained calls
     */
    public Builder partialProgressMaxCommits(int newPartialProgressMaxCommits) {
      this.partialProgressMaxCommits = newPartialProgressMaxCommits;
      return this;
    }

    /**
     * Configures the target file size. For more details description see {@link
     * org.apache.iceberg.actions.RewriteDataFiles#TARGET_FILE_SIZE_BYTES}.
     *
     * @param targetFileSizeBytes target file size
     * @return for chained calls
     */
    public Builder targetFileSizeBytes(long targetFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSizeBytes));
      return this;
    }

    /**
     * Configures the min file size considered for rewriting. For more details description see
     * {@link SizeBasedFileRewriter#MIN_FILE_SIZE_BYTES}.
     *
     * @param minFileSizeBytes min file size
     * @return for chained calls
     */
    public Builder minFileSizeBytes(long minFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSizeBytes));
      return this;
    }

    /**
     * Configures the max file size considered for rewriting. For more details description see
     * {@link SizeBasedFileRewriter#MAX_FILE_SIZE_BYTES}.
     *
     * @param maxFileSizeBytes max file size
     * @return for chained calls
     */
    public Builder maxFileSizeBytes(long maxFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSizeBytes));
      return this;
    }

    /**
     * Configures the minimum file number after a rewrite is always initiated. For more details
     * description see {@link SizeBasedFileRewriter#MIN_INPUT_FILES}.
     *
     * @param minInputFiles min file number
     * @return for chained calls
     */
    public Builder minInputFiles(int minInputFiles) {
      this.rewriteOptions.put(SizeBasedFileRewriter.MIN_INPUT_FILES, String.valueOf(minInputFiles));
      return this;
    }

    /**
     * Configures the minimum delete file number for a file after a rewrite is always initiated. For
     * more details description see {@link SizeBasedDataRewriter#DELETE_FILE_THRESHOLD}.
     *
     * @param deleteFileThreshold min delete file number
     * @return for chained calls
     */
    public Builder deleteFileThreshold(int deleteFileThreshold) {
      this.rewriteOptions.put(
          SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, String.valueOf(deleteFileThreshold));
      return this;
    }

    /**
     * Every other option is overridden, and all the files are rewritten.
     *
     * @param rewriteAll enables a full rewrite
     * @return for chained calls
     */
    public Builder rewriteAll(boolean rewriteAll) {
      this.rewriteOptions.put(SizeBasedFileRewriter.REWRITE_ALL, String.valueOf(rewriteAll));
      return this;
    }

    /**
     * Configures the group size for rewriting. For more details description see {@link
     * SizeBasedDataRewriter#MAX_FILE_GROUP_SIZE_BYTES}.
     *
     * @param maxFileGroupSizeBytes file group size for rewrite
     * @return for chained calls
     */
    public Builder maxFileGroupSizeBytes(long maxFileGroupSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.MAX_FILE_GROUP_SIZE_BYTES, String.valueOf(maxFileGroupSizeBytes));
      return this;
    }

    /**
     * Configures the maximum byte size of the rewrites for one scheduled compaction. This could be
     * used to limit the resources used by the compaction.
     *
     * @param newMaxRewriteBytes to limit the size of the rewrites
     * @return for chained calls
     */
    public Builder maxRewriteBytes(long newMaxRewriteBytes) {
      this.maxRewriteBytes = newMaxRewriteBytes;
      return this;
    }

    @Override
    public DataStream<RunResponse> buildInternal(DataStream<SerializableTable> trigger) {
      SerializableTable serializedTable =
          (SerializableTable) SerializableTable.copyOf(tableLoader().loadTable());

      // Create start trigger event
      KeyedStream<Tuple2<Long, Long>, Long> withTime = extractTimes(trigger, "-rd-trigger");

      SingleOutputStreamOperator<DataFileRewriteTask> planned =
          trigger
              .process(
                  new DataFileRewritePlanner(
                      name(),
                      serializedTable,
                      rewriteOptions,
                      partialProgressEnabled ? partialProgressMaxCommits : 1,
                      maxRewriteBytes))
              .name(PLANNER_TASK_NAME)
              .uid(uidPrefix() + "-planner")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<Tuple3<Long, Integer, RewriteFileGroup>> rewritten =
          planned
              .keyBy(DataFileRewriteTask::getKey)
              .process(new DataFileRewriteExecutor(name()))
              .name(REWRITE_TASK_NAME)
              .uid(uidPrefix() + "-rewriter")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<Boolean> updated =
          rewritten
              .process(new DataFileUpdater.EventTimeExtractor())
              .name("Time extractor")
              .uid(uidPrefix() + "-rewriter-time-extractor")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(rewritten.getParallelism())
              .keyBy(new DataFileUpdater.EventTimeKeySelector())
              .process(new DataFileUpdater(name(), tableLoader()))
              .name(COMMIT_TASK_NAME)
              .uid(uidPrefix() + "-data-file-updater")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      return withTime
          .connect(
              decorateUpdated(updated)
                  .union(
                      prepareError(planned, "-rd-planned-error"),
                      prepareError(rewritten, "-rd-rewritten-error"),
                      prepareError(updated, "rd-update-error"))
                  .keyBy(value -> value.f0))
          .process(new ErrorAggregator(id()))
          .name("RD Result")
          .uid(uidPrefix() + "-rd-result-aggregator")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private DataStream<Tuple2<Long, Exception>> decorateUpdated(DataStream<Boolean> updated) {
      return withEventTime(
          updated
              .map(unused -> new Exception("Should not happen"))
              .name("Result chainer")
              .uid(uidPrefix() + "-rewriter-result-chainer")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(updated.getParallelism()),
          "-rd-updated");
    }
  }
}
