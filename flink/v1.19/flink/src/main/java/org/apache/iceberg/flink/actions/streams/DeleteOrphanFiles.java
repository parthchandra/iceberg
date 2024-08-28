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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.actions.operators.AntiJoin;
import org.apache.iceberg.flink.actions.operators.AsyncDeleteFiles;
import org.apache.iceberg.flink.actions.operators.ErrorAggregator;
import org.apache.iceberg.flink.actions.operators.ListFileSystemFiles;
import org.apache.iceberg.flink.actions.operators.ListFileSystemFiles.EventTimeExtractor;
import org.apache.iceberg.flink.actions.operators.ListMetadataFiles;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.flink.actions.operators.SimpleOperators;
import org.apache.iceberg.flink.actions.operators.SkipOnError;
import org.apache.iceberg.flink.actions.operators.TablePlanner;
import org.apache.iceberg.flink.actions.operators.TableReader;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the delete orphan files data stream. Which runs a single iteration of the task for every
 * input event.
 *
 * <p>The input is a {@link DataStream} with {@link SerializableTable} events and every event should
 * be immediately followed by a {@link org.apache.flink.streaming.api.watermark.Watermark} with the
 * same timestamp as the event.
 *
 * <p>The output is a {@link DataStream} with the result of the run (success or failure) followed by
 * the {@link org.apache.flink.streaming.api.watermark.Watermark}.
 */
public class DeleteOrphanFiles {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteOrphanFiles.class);
  private static final Schema FILE_PATH_SCHEMA = new Schema(DataFile.FILE_PATH);
  private static final ScanContext FILE_PATH_SCAN_CONTEXT =
      ScanContext.builder().streaming(true).project(FILE_PATH_SCHEMA).build();

  static final String PLANNER_TASK_NAME = "Planner";
  static final String READER_TASK_NAME = "Reader";
  static final String FILESYSTEM_FILES_TASK_NAME = "Filesystem files";
  static final String METADATA_FILES_TASK_NAME = "List metadata files";
  static final String DELETE_FILES_TASK_NAME = "Delete file";

  private DeleteOrphanFiles() {
    // Do not instantiate directly
  }

  /**
   * Creates the builder for creating a stream which deletes the orphan files under the table root
   * directory.
   *
   * @param env the checkpoint interval of the environment is used to prevent removing temporary
   *     files
   */
  public static Builder builder(StreamExecutionEnvironment env) {
    return new Builder(env);
  }

  public static class Builder extends MaintenanceTaskBuilder<Builder> {
    private final StreamExecutionEnvironment env;

    private String location = null;
    private Duration minAge = Duration.ofDays(1);
    private int planningWorkerPoolSize = 10;
    private int deleteAttemptNum = 3;
    private int deleteWorkerPoolSize = 10;

    private Builder(StreamExecutionEnvironment env) {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      this.env = env;
    }

    /**
     * The location to start the recursive listing the candidate files for removal. By default, the
     * {@link Table#location()} is used.
     *
     * @param newLocation the task will scan
     * @return for chained calls
     */
    public Builder location(String newLocation) {
      this.location = newLocation;
      return this;
    }

    /**
     * The files newer than this age will not be removed.
     *
     * @param newMinAge of the files to be removed
     * @return for chained calls
     */
    public Builder minAge(Duration newMinAge) {
      this.minAge = newMinAge;
      return this;
    }

    /**
     * The worker pool size used for planning the scan of the {@link MetadataTableType#ALL_FILES}
     * table. This scan is used for determining the files used by the table.
     *
     * @param newPlanningWorkerPoolSize for scanning
     * @return for chained calls
     */
    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    /**
     * The number of retries on the failed delete attempts.
     *
     * @param newDeleteAttemptNum number of retries
     * @return for chained calls
     */
    public Builder deleteAttemptNum(int newDeleteAttemptNum) {
      this.deleteAttemptNum = newDeleteAttemptNum;
      return this;
    }

    /**
     * The worker pool size used for deleting files.
     *
     * @param newDeleteWorkerPoolSize for scanning
     * @return for chained calls
     */
    public Builder deleteWorkerPoolSize(int newDeleteWorkerPoolSize) {
      this.deleteWorkerPoolSize = newDeleteWorkerPoolSize;
      return this;
    }

    @Override
    public DataStream<RunResponse> buildInternal(DataStream<SerializableTable> sourceStream) {
      long minAgeMs = env.getCheckpointInterval() * 2L;
      if (minAge != null) {
        minAgeMs = minAge.toMillis();
        if (minAgeMs < env.getCheckpointInterval()) {
          LOG.info(
              "Checkpoint interval is {} which is smaller than the configured minAge {}. "
                  + "This could lead to table corruption with concurrent Iceberg sink(s).",
              env.getCheckpointInterval(),
              minAge);
        }
      }

      SerializableTable serializedTable =
          (SerializableTable) SerializableTable.copyOf(tableLoader().loadTable());

      Preconditions.checkArgument(
          serializedTable.io() instanceof SupportsPrefixOperations,
          "Can't list files with {}",
          serializedTable.io());

      SerializableTable allFilesTable = allFiles(serializedTable);

      // Create start trigger event
      KeyedStream<Tuple2<Long, Long>, Long> withTime = extractTimes(sourceStream, "-do-trigger");

      SingleOutputStreamOperator<String> tableMetadataFiles =
          sourceStream
              .process(new ListMetadataFiles(name()))
              .name(METADATA_FILES_TASK_NAME)
              .uid(uidPrefix() + "-metadata-file-list")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // Collect all files
      SingleOutputStreamOperator<IcebergSourceSplit> splits =
          sourceStream
              .map(Builder::allFiles)
              .name("All files table")
              .uid(uidPrefix() + "-to-all-files")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel()
              .process(
                  new TablePlanner(
                      name(), allFilesTable, FILE_PATH_SCAN_CONTEXT, planningWorkerPoolSize))
              .name(PLANNER_TASK_NAME)
              .uid(uidPrefix() + "-all-data-files-planner")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // Read the records
      SingleOutputStreamOperator<RowData> rowData =
          splits
              .rebalance()
              .process(new TableReader(name(), allFilesTable, FILE_PATH_SCHEMA))
              .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(FILE_PATH_SCHEMA)))
              .name(READER_TASK_NAME)
              .uid(uidPrefix() + "-all-data-files-reader")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      // Extract the file name from the records
      DataStream<String> tableDataFiles =
          rowData
              .flatMap(new FirstStringColumn())
              .returns(Types.STRING)
              .name("File name")
              .uid(uidPrefix() + "-file-name-extractor")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      // List the file system files which were present at the event time
      SingleOutputStreamOperator<String> fsFiles = fsFiles(sourceStream, serializedTable, minAgeMs);

      // Key by event time and calculate the files to delete
      KeyedStream<Tuple2<Long, String>, Long> filesToDelete =
          withEventTime(tableMetadataFiles, "-meta-files")
              .union(withEventTime(tableDataFiles, "-table-files"))
              .keyBy(new SimpleOperators.FullRecordKeySelector<>())
              .connect(
                  withEventTime(fsFiles, "-file-system-files")
                      .keyBy(new SimpleOperators.FullRecordKeySelector<>()))
              .process(new AntiJoin())
              .slotSharingGroup(slotSharingGroup())
              .name("Filter files")
              .uid(uidPrefix() + "-filter-unreachable")
              .setParallelism(parallelism())
              .keyBy(value -> value.f0);

      // Aggregate the errors for event time
      KeyedStream<Tuple2<Long, Exception>, Long> keyedErrorStream =
          prepareError(tableMetadataFiles, "do-metadata-files-error")
              .union(
                  prepareError(splits, "-do-splits-error"),
                  prepareError(rowData, "-do-row-data-error"),
                  prepareError(fsFiles, "-do-fs-files-error"))
              .keyBy(value -> value.f0);

      // Stop deleting the files if there is an error
      SingleOutputStreamOperator<String> filesOrSkip =
          filesToDelete
              .connect(keyedErrorStream)
              .process(new SkipOnError())
              .name("Skip on error")
              .uid(uidPrefix() + "-do-skip-on-error")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      // Delete the files
      asyncDelete(filesOrSkip);

      // Deleting the files is asynchronous, ignore its results when calculating the return value
      return withTime
          .connect(
              keyedErrorStream
                  .union(prepareError(filesOrSkip, "-do-skip-error"))
                  .keyBy(value -> value.f0))
          .process(new ErrorAggregator(id()))
          .name("DOF Result")
          .uid(uidPrefix() + "-do-result-aggregator")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private SingleOutputStreamOperator<String> fsFiles(
        DataStream<SerializableTable> sourceStream,
        SerializableTable serializedTable,
        long minAgeMs) {
      return sourceStream
          .process(new EventTimeExtractor())
          .name("Time extractor")
          .uid(uidPrefix() + "-file-system-files-time-extractor")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel()
          .process(
              new ListFileSystemFiles(
                  name(),
                  (SupportsPrefixOperations) serializedTable.io(),
                  location != null ? location : serializedTable.location(),
                  serializedTable.specs(),
                  minAgeMs))
          .name(FILESYSTEM_FILES_TASK_NAME)
          .uid(uidPrefix() + "-list-filesystem-files")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private void asyncDelete(DataStream<String> files) {
      AsyncRetryStrategy<Boolean> retryStrategy =
          new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Boolean>(
                  deleteAttemptNum, 10, 1000, 1.5)
              .ifResult(AsyncDeleteFiles.FAILED_PREDICATE)
              .build();

      AsyncDataStream.unorderedWaitWithRetry(
              files.rebalance(),
              new AsyncDeleteFiles(name(), tableLoader(), deleteWorkerPoolSize),
              10000,
              TimeUnit.MILLISECONDS,
              deleteWorkerPoolSize,
              retryStrategy)
          .name(DELETE_FILES_TASK_NAME)
          .uid(uidPrefix() + "-delete-files")
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(parallelism());
    }

    private static SerializableTable allFiles(SerializableTable table) {
      return (SerializableTable)
          SerializableTable.copyOf(
              MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ALL_FILES));
    }

    private static class FirstStringColumn implements FlatMapFunction<RowData, String> {
      @Override
      public void flatMap(RowData value, Collector<String> out) {
        if (value != null && value.getString(0) != null) {
          out.collect(value.getString(0).toString());
        }
      }
    }
  }
}
