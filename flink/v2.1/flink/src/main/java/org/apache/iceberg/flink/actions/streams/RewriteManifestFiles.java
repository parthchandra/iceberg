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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.actions.operators.AllManifests;
import org.apache.iceberg.flink.actions.operators.ErrorAggregator;
import org.apache.iceberg.flink.actions.operators.FilterSplitsByFileName;
import org.apache.iceberg.flink.actions.operators.IncompatibleChangeBlocker;
import org.apache.iceberg.flink.actions.operators.ManifestUpdater;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.flink.actions.operators.SimpleOperators;
import org.apache.iceberg.flink.actions.operators.TablePlanner;
import org.apache.iceberg.flink.actions.operators.TableReader;
import org.apache.iceberg.flink.actions.operators.WriteManifests;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Creates the manifest file rewriter data stream. Which runs a single iteration of the task for
 * every input event.
 *
 * <p>The input is a {@link DataStream} with {@link SerializableTable} events and every event should
 * be immediately followed by a {@link org.apache.flink.streaming.api.watermark.Watermark} with the
 * same timestamp as the event.
 *
 * <p>The output is a {@link DataStream} with the result of the run (success or failure) followed by
 * the {@link org.apache.flink.streaming.api.watermark.Watermark}.
 */
public class RewriteManifestFiles {
  static final String SPEC_CHANGE_BLOCKER_TASK_NAME = "RMF Spec change blocker";
  static final String OLD_MANIFESTS_TASK_NAME = "Old manifest list";
  static final String PLANNER_TASK_NAME = "Planner";
  static final String READER_TASK_NAME = "Entries reader";
  static final String FIRST_WRITE_TASK_NAME = "Write First Stage";
  static final String LAST_WRITE_TASK_NAME = "Write Last Stage";
  static final String COMMIT_TASK_NAME = "RMF Manifest updater";

  private RewriteManifestFiles() {
    // Do not instantiate directly
  }

  /** Creates the builder for creating a stream which rewrites manifest files for the table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<Builder> {
    private Long targetManifestSizeBytes = null;
    private int planningWorkerPoolSize = 10;
    private boolean multiStage = false;
    private boolean failOnSchemaChange = false;

    /**
     * Sets the target manifest file size if it is different from the table default defined by
     * {@link TableProperties#MANIFEST_TARGET_SIZE_BYTES}.
     *
     * @param newTargetManifestSizeBytes target file size
     * @return for chained calls
     */
    public Builder targetManifestSizeBytes(long newTargetManifestSizeBytes) {
      this.targetManifestSizeBytes = newTargetManifestSizeBytes;
      return this;
    }

    /**
     * The worker pool size used for planning the scan of the {@link MetadataTableType#ENTRIES}
     * table. This scan is used for determining the manifest files used by the table.
     *
     * @param newPlanningWorkerPoolSize for scanning
     * @return for chained calls
     */
    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    /**
     * Parallel writers for the new {@link ManifestFile}s can speed up the task for big tables, but
     * the last manifest files could be undersized. Enabling multiStage writers will read these
     * partial files and writes them out again with a single writer. The resulting file layout is
     * optimal, but some IO is wasted on rewrites.
     *
     * @param newMultiStage for scanning
     * @return for chained calls
     */
    public Builder multiStage(boolean newMultiStage) {
      this.multiStage = newMultiStage;
      return this;
    }

    /**
     * If there is a spec change on the table, then the state of the manifest rewrite task becomes
     * invalid. If failOnSpecChange is set to <code>true</code>, then the job will stop with {@link
     * org.apache.flink.runtime.execution.SuppressRestartsException} to prevent job restarts. If
     * failOnSpecChange is set to <code>false</code>, then the job will continue to run but the
     * manifest rewrite job will stop working.
     *
     * @param newFailOnSpecChange to stop the job on table spec change
     * @return for chained calls
     */
    public Builder failOnSpecChange(boolean newFailOnSpecChange) {
      this.failOnSchemaChange = newFailOnSpecChange;
      return this;
    }

    @Override
    public DataStream<RunResponse> buildInternal(DataStream<SerializableTable> trigger) {
      Table table = tableLoader().loadTable();
      if (targetManifestSizeBytes == null) {
        targetManifestSizeBytes =
            PropertyUtil.propertyAsLong(
                table.properties(),
                TableProperties.MANIFEST_TARGET_SIZE_BYTES,
                TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
      }

      // default the output location to the metadata location
      TableOperations ops = ((HasTableOperations) table).operations();
      Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
      String outputLocation = metadataFilePath.getParent().toString();

      // use the current table format version for new manifests
      int formatVersion = ops.current().formatVersion();

      SerializableTable serializedTable = (SerializableTable) SerializableTable.copyOf(table);
      SerializableTable entriesTable = entries(serializedTable);

      // Create start trigger event
      KeyedStream<Tuple2<Long, Long>, Long> withTime = extractTimes(trigger, "-rm-trigger");

      // Enforce parallelism 1 and prevent spec changes
      SingleOutputStreamOperator<SerializableTable> checked = enforceSpec(trigger, serializedTable);

      // Collect the original manifests
      SingleOutputStreamOperator<ManifestFile> oldManifests =
          checked
              .process(new AllManifests(name()))
              .name(OLD_MANIFESTS_TASK_NAME)
              .uid(uidPrefix() + "-old-manifest-list")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      KeyedStream<Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>, Long>
          keyedOldManifests =
              withEventTime(oldManifests, ManifestUpdater.ManifestSource.OLD, "old-manifest-list")
                  .keyBy(value -> value.f0);

      // Generate the iceberg splits for reading the entries
      SingleOutputStreamOperator<IcebergSourceSplit> splits =
          planManifestEntryTableRead(checked, entriesTable);

      KeyedStream<Tuple2<Long, IcebergSourceSplit>, Long> keyedSplits =
          SimpleOperators.keyByEventTime(splits, uidPrefix(), "splits", slotSharingGroup());

      // Filter splits to only read required manifest files
      DataStream<IcebergSourceSplit> filteredSplits =
          keyedOldManifests
              .connect(keyedSplits)
              .process(new FilterSplitsByFileName())
              .name("Filtered splits")
              .uid(uidPrefix() + "-split-filter")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      // Read entries from the manifest files
      SingleOutputStreamOperator<RowData> manifestEntries =
          filteredSplits
              .rebalance()
              .process(new TableReader(name(), entriesTable))
              .name(READER_TASK_NAME)
              .uid(uidPrefix() + "-entries-reader")
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      KeyedStream<Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>, Long> writeResult;
      DataStream<Tuple2<Long, Exception>> writeError;
      if (multiStage) {
        WriteManifests firstWriter = writer(serializedTable, outputLocation, formatVersion, false);

        // Write the manifest files which reach the file size limit
        SingleOutputStreamOperator<ManifestFile> fromFirstStage =
            writeFirstStage(manifestEntries, firstWriter);

        WriteManifests lastWriter = writer(serializedTable, outputLocation, formatVersion, true);

        // Write the remaining manifest entries not written previously
        SingleOutputStreamOperator<ManifestFile> fromLastStage =
            fromFirstStage
                .getSideOutput(WriteManifests.REMAINING)
                .transform(LAST_WRITE_TASK_NAME, TypeInformation.of(ManifestFile.class), lastWriter)
                .uid(uidPrefix() + "-" + LAST_WRITE_TASK_NAME)
                .slotSharingGroup(slotSharingGroup())
                .startNewChain()
                .forceNonParallel();

        writeResult =
            withEventTime(
                    fromFirstStage, ManifestUpdater.ManifestSource.NEW, "new-manifest-list-first")
                .union(
                    withEventTime(
                        fromLastStage,
                        ManifestUpdater.ManifestSource.NEW,
                        "new-manifest-list-last"))
                .keyBy(value -> value.f0);

        writeError =
            prepareError(fromFirstStage, "rm-first-stage-error")
                .union(prepareError(fromLastStage, "rm-last-stage-error"));
      } else {
        WriteManifests firstWriter = writer(serializedTable, outputLocation, formatVersion, true);

        SingleOutputStreamOperator<ManifestFile> fromFirstStage =
            writeFirstStage(manifestEntries, firstWriter);

        writeResult =
            withEventTime(
                    fromFirstStage, ManifestUpdater.ManifestSource.NEW, "new-manifest-list-first")
                .keyBy(value -> value.f0);

        writeError = prepareError(fromFirstStage, "rm-first-stage-error");
      }

      DataStream<Tuple2<Long, Exception>> error =
          prepareError(oldManifests, "rm-old-manifests-error")
              .union(
                  prepareError(splits, "rm-splits-error"),
                  prepareError(manifestEntries, "rm-manifest-entries-error"),
                  prepareError(checked, "rm-incompatible-error"),
                  writeError);

      SingleOutputStreamOperator<Tuple2<Long, Boolean>> update =
          writeResult
              .union(keyedOldManifests)
              .keyBy(value -> value.f0)
              .connect(error.keyBy(value -> value.f0))
              .process(new ManifestUpdater(name(), tableLoader()))
              .name(COMMIT_TASK_NAME)
              .uid(uidPrefix() + "-manifest-updater")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      return withTime
          .connect(error.union(prepareError(update, "rm-update-error")).keyBy(value -> value.f0))
          .process(new ErrorAggregator(id()))
          .name("RM Result")
          .uid(uidPrefix() + "-rm-result-aggregator")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private WriteManifests writer(
        SerializableTable serializedTable,
        String outputLocation,
        int formatVersion,
        boolean flushPartial) {
      return new WriteManifests(
          name(),
          serializedTable,
          outputLocation,
          formatVersion,
          targetManifestSizeBytes,
          null,
          flushPartial);
    }

    private static SerializableTable entries(SerializableTable table) {
      return (SerializableTable)
          SerializableTable.copyOf(
              MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ENTRIES));
    }

    private DataStream<Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>> withEventTime(
        DataStream<ManifestFile> source, ManifestUpdater.ManifestSource type, String name) {
      return source
          .process(new ManifestEventTimeDecorator(type))
          .name("Time decorator")
          .uid(uidPrefix() + "time-decorator-for-" + name)
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(source.getParallelism());
    }

    private SingleOutputStreamOperator<ManifestFile> writeFirstStage(
        SingleOutputStreamOperator<RowData> entries, WriteManifests writer) {
      return entries
          .transform(FIRST_WRITE_TASK_NAME, TypeInformation.of(ManifestFile.class), writer)
          .uid(uidPrefix() + "-" + FIRST_WRITE_TASK_NAME)
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(parallelism());
    }

    private SingleOutputStreamOperator<SerializableTable> enforceSpec(
        DataStream<SerializableTable> trigger, SerializableTable table) {
      DataStream<SerializableTable> nonParallel =
          trigger.getParallelism() == 1
              ? trigger
              : trigger
                  .rebalance()
                  .map(a -> a)
                  .name("Non parallel")
                  .uid(uidPrefix() + "-non-parallel")
                  .slotSharingGroup(slotSharingGroup())
                  .forceNonParallel();

      // Prevent incompatible changes
      return DataStreamUtils.reinterpretAsKeyedStream(nonParallel, unused -> true)
          .process(new IncompatibleChangeBlocker(name(), table, false, true, failOnSchemaChange))
          .name(SPEC_CHANGE_BLOCKER_TASK_NAME)
          .uid(uidPrefix() + "-spec-change-blocker")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private SingleOutputStreamOperator<IcebergSourceSplit> planManifestEntryTableRead(
        DataStream<SerializableTable> trigger, SerializableTable entriesTable) {
      return trigger
          .map(Builder::entries)
          .name("To entries table")
          .uid(uidPrefix() + "-to-entries")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel()
          .process(new TablePlanner(name(), entriesTable, planningWorkerPoolSize))
          .name(PLANNER_TASK_NAME)
          .uid(uidPrefix() + "-entries-planner")
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }

    private static class ManifestEventTimeDecorator
        extends ProcessFunction<
            ManifestFile, Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>> {
      private final ManifestUpdater.ManifestSource type;

      private ManifestEventTimeDecorator(ManifestUpdater.ManifestSource type) {
        this.type = type;
      }

      @Override
      public void processElement(
          ManifestFile value,
          ProcessFunction<ManifestFile, Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>>
                  .Context
              ctx,
          Collector<Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>> out) {
        out.collect(Tuple3.of(ctx.timestamp(), type, value));
      }
    }
  }
}
