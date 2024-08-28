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

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the new {@link ManifestFile}s based on the incoming manifest entries represented by the
 * {@link RowData} with the help of the {@link StreamingManifestWriter}s. There is a separate writer
 * for every event time and {@link ManifestContent}. The pending entries are store in the state and
 * removed when the writer flushes a new {@link ManifestFile}.
 */
public class WriteManifests extends AbstractStreamOperator<ManifestFile>
    implements OneInputStreamOperator<RowData, ManifestFile> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteManifests.class);

  public static final OutputTag<RowData> REMAINING =
      new OutputTag<RowData>("remaining-manifests") {};

  // TODO: Check if we would like to expose this from ManifestEntry
  static final String DATA_FILE = "data_file";
  static final String STATUS = "status";
  static final String SNAPSHOT_ID = "snapshot_id";
  static final String SEQUENCE_NUMBER = "sequence_number";
  static final String FILE_SEQUENCE_NUMBER = "file_sequence_number";

  private final String name;
  private final boolean flushPartial;
  private final RowType entriesTableType;
  private final FileIO io;
  private final StreamingManifestWriter.Builder writerBuilder;
  private final Map<String, Integer> positions;
  private final int dataFileFieldNum;

  private transient Counter errorCounter;
  private transient Counter removedPartialManifestNumCounter;
  private transient Counter removedPartialManifestSizeCounter;
  private transient ListState<Tuple2<Long, RowData>> pendingEntriesState;
  private transient NavigableMap<Tuple2<Long, ManifestContent>, StreamingManifestWriter> writers;

  public WriteManifests(
      String name,
      SerializableTable table,
      String outputLocation,
      int formatVersion,
      long targetManifestSizeBytes,
      Integer rowsDivisor,
      boolean flushPartial) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(table, "Table should no be null");
    Preconditions.checkNotNull(outputLocation, "The output location should no be null");

    this.name = name;
    this.flushPartial = flushPartial;

    Table entriesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ENTRIES);

    this.entriesTableType = FlinkSchemaUtil.convert(entriesTable.schema());
    RowType fileType =
        (RowType) entriesTableType.getTypeAt(entriesTableType.getFieldIndex(DATA_FILE));

    this.io = table.io();
    this.positions =
        ImmutableMap.of(
            DATA_FILE,
            entriesTableType.getFieldIndex(DATA_FILE),
            STATUS,
            entriesTableType.getFieldIndex(STATUS),
            SNAPSHOT_ID,
            entriesTableType.getFieldIndex(SNAPSHOT_ID),
            SEQUENCE_NUMBER,
            entriesTableType.getFieldIndex(SEQUENCE_NUMBER),
            FILE_SEQUENCE_NUMBER,
            entriesTableType.getFieldIndex(FILE_SEQUENCE_NUMBER),
            // This one is one level deeper, but key does not clash
            DataFile.CONTENT.name(),
            fileType.getFieldIndex(DataFile.CONTENT.name()));
    this.dataFileFieldNum = fileType.getFieldCount();

    this.writerBuilder =
        new StreamingManifestWriter.Builder(
            table,
            outputLocation,
            formatVersion,
            entriesTableType,
            positions,
            targetManifestSizeBytes,
            rowsDivisor);

    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) throws Exception {
    if (element.isRecord()) {
      RowData data = element.getValue();
      long timestamp = element.getTimestamp();

      if (!isLive(data)) {
        // If not live, then skip
        return;
      }

      try {
        Tuple2<Long, ManifestContent> key = content(timestamp, data);
        StreamingManifestWriter writer = writerFor(key);
        ManifestFile newManifest = writer.write(data);
        if (newManifest != null) {
          output.collect(new StreamRecord<>(newManifest, timestamp));
        }
      } catch (Exception e) {
        LOG.info("Exception processing element {} at {}", data, element.getTimestamp(), e);
        output.collect(ErrorAggregator.ERROR_STREAM, new StreamRecord<>(e, timestamp));
        errorCounter.inc();
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    NavigableMap<Tuple2<Long, ManifestContent>, StreamingManifestWriter> writersToClose =
        Maps.newTreeMap(writers)
            .headMap(Tuple2.of(mark.getTimestamp(), ManifestContent.DELETES), true);

    writersToClose.forEach(
        (key, value) -> {
          try {
            closeWriterAndCleanup(key.f0, value);
          } catch (Exception ex) {
            LOG.info("Exception processing watermark {}", mark, ex);
            output.collect(
                ErrorAggregator.ERROR_STREAM, new StreamRecord<>(ex, mark.getTimestamp()));
            errorCounter.inc();
          }
        });

    pendingEntriesState.clear();
    writersToClose.keySet().forEach(writers::remove);

    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    pendingEntriesState.clear();
    for (Map.Entry<Tuple2<Long, ManifestContent>, StreamingManifestWriter> entry :
        writers.entrySet()) {
      pendingEntriesState.addAll(
          entry.getValue().pending().stream()
              .map(row -> Tuple2.of(entry.getKey().f0, row))
              .collect(Collectors.toList()));
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.errorCounter =
        getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);

    this.removedPartialManifestNumCounter =
        getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_PARTIAL_MANIFEST_NUM);
    this.removedPartialManifestSizeCounter =
        getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_PARTIAL_MANIFEST_SIZE);

    this.writers =
        Maps.newTreeMap(
            (o1, o2) -> {
              int time = o1.f0.compareTo(o2.f0);
              return time != 0 ? time : o1.f1.id() - o2.f1.id();
            });

    this.pendingEntriesState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "writeManifestPendingEntries-" + flushPartial,
                    new TupleTypeInfo<>(Types.LONG, InternalTypeInfo.of(entriesTableType))));

    if (context.isRestored()) {
      // Rewrite the pending data
      pendingEntriesState
          .get()
          .forEach(entry -> writerFor(content(entry.f0, entry.f1)).write(entry.f1));
    }
  }

  private StreamingManifestWriter writerFor(Tuple2<Long, ManifestContent> key) {
    return writers.computeIfAbsent(key, newKey -> writerBuilder.build(newKey.f1));
  }

  private Tuple2<Long, ManifestContent> content(long timestamp, RowData data) {
    return Tuple2.of(
        timestamp,
        data.getRow(positions.get(DATA_FILE), dataFileFieldNum)
                    .getInt(positions.get(DataFile.CONTENT.name()))
                < 1
            ? ManifestContent.DATA
            : ManifestContent.DELETES);
  }

  /** See {@link org.apache.iceberg.ManifestEntry#isLive()} */
  private boolean isLive(RowData data) {
    return data.getInt(positions.get(STATUS)) < 2;
  }

  private void closeWriterAndCleanup(long timestamp, StreamingManifestWriter writer)
      throws IOException {
    if (writer != null) {
      if (flushPartial) {
        ManifestFile lastManifest = writer.close();
        output.collect(new StreamRecord<>(lastManifest, timestamp));
      } else {
        writer
            .pending()
            .forEach(
                entry ->
                    output.collect(WriteManifests.REMAINING, new StreamRecord<>(entry, timestamp)));
        ManifestFile lastManifest = writer.close();
        removedPartialManifestNumCounter.inc();
        removedPartialManifestSizeCounter.inc(lastManifest.length());
        io.deleteFile(lastManifest.path());
      }
    }
  }
}
