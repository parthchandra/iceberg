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
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.actions.operators.ManifestWriterFactory.StreamingRollingManifestWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

interface StreamingManifestWriter {
  ManifestFile close() throws IOException;

  ManifestFile write(RowData rowData);

  List<RowData> pending();

  class Builder implements Serializable {
    private final FileIO io;
    private final PartitionSpec spec;

    private final long targetManifestSizeBytes;
    private final Integer rowsDivisor;
    private final Map<String, Integer> positions;
    private final Types.StructType combinedFileType;
    private final Types.StructType fileType;
    private final RowType rowType;
    private final int formatVersion;
    private final String outputLocation;

    Builder(
        SerializableTable table,
        String outputLocation,
        int formatVersion,
        RowType entriesTableType,
        Map<String, Integer> positions,
        long targetManifestSizeBytes,
        Integer rowsDivisor) {
      this.io = table.io();
      this.spec = table.spec();
      this.outputLocation = outputLocation;
      this.formatVersion = formatVersion;
      this.positions = positions;
      this.targetManifestSizeBytes = targetManifestSizeBytes;
      this.rowsDivisor = rowsDivisor;

      this.combinedFileType = DataFile.getType(Partitioning.partitionType(table));
      this.fileType = DataFile.getType(table.spec().partitionType());
      this.rowType = (RowType) entriesTableType.getTypeAt(positions.get(WriteManifests.DATA_FILE));
    }

    StreamingManifestWriter build(ManifestContent content) {
      ManifestWriterFactory factory =
          new ManifestWriterFactory(
              io, spec, formatVersion, outputLocation, targetManifestSizeBytes, rowsDivisor);
      switch (content) {
        case DATA:
          return new DataManifestWriter(
              factory.newRollingManifestWriter(),
              new FlinkDataFile(combinedFileType, fileType, rowType),
              positions,
              rowType.getFieldCount());
        case DELETES:
          return new DeleteManifestWriter(
              factory.newRollingDeleteManifestWriter(),
              new FlinkDeleteFile(combinedFileType, fileType, rowType),
              positions,
              rowType.getFieldCount());
        default:
          throw new IllegalArgumentException("Unknown content type: " + content);
      }
    }
  }

  abstract class ManifestWriterBase implements StreamingManifestWriter {
    private final Map<String, Integer> positions;
    private final int dataFileFieldCount;
    private final List<RowData> pending;

    ManifestWriterBase(Map<String, Integer> positions, int dataFileFieldCount) {
      this.positions = positions;
      this.dataFileFieldCount = dataFileFieldCount;
      this.pending = Lists.newArrayList();
    }

    abstract ManifestFile closeInternalWriter() throws IOException;

    abstract ManifestFile writeToInternalWriter(RowData data);

    Map<String, Integer> positions() {
      return positions;
    }

    int dataFileFieldCount() {
      return dataFileFieldCount;
    }

    @Override
    public ManifestFile close() throws IOException {
      return closeInternalWriter();
    }

    @Override
    public ManifestFile write(RowData rowData) {
      ManifestFile newManifest = writeToInternalWriter(rowData);
      if (newManifest != null) {
        pending.clear();
      }

      pending.add(rowData);
      return newManifest;
    }

    @Override
    public List<RowData> pending() {
      return pending;
    }
  }

  class DataManifestWriter extends ManifestWriterBase {
    private final StreamingRollingManifestWriter<DataFile> rollingWriter;
    private final FlinkContentFile<DataFile> wrapper;

    DataManifestWriter(
        StreamingRollingManifestWriter<DataFile> rollingWriter,
        FlinkContentFile<DataFile> wrapper,
        Map<String, Integer> positions,
        int dataFileFieldCount) {
      super(positions, dataFileFieldCount);
      this.rollingWriter = rollingWriter;
      this.wrapper = wrapper;
    }

    @Override
    public ManifestFile closeInternalWriter() throws IOException {
      return rollingWriter.closeWithResult();
    }

    @Override
    public ManifestFile writeToInternalWriter(RowData rowData) {
      return rollingWriter.existingWithResult(
          wrapper.wrap(
              rowData.getRow(positions().get(WriteManifests.DATA_FILE), dataFileFieldCount())),
          rowData.getLong(positions().get(WriteManifests.SNAPSHOT_ID)),
          rowData.getLong(positions().get(WriteManifests.SEQUENCE_NUMBER)),
          rowData.getLong(positions().get(WriteManifests.FILE_SEQUENCE_NUMBER)));
    }
  }

  class DeleteManifestWriter extends ManifestWriterBase {
    private final StreamingRollingManifestWriter<DeleteFile> rollingWriter;
    private final FlinkContentFile<DeleteFile> wrapper;

    DeleteManifestWriter(
        StreamingRollingManifestWriter<DeleteFile> rollingWriter,
        FlinkContentFile<DeleteFile> wrapper,
        Map<String, Integer> positions,
        int dataFileFieldCount) {
      super(positions, dataFileFieldCount);
      this.rollingWriter = rollingWriter;
      this.wrapper = wrapper;
    }

    @Override
    public ManifestFile closeInternalWriter() throws IOException {
      return rollingWriter.closeWithResult();
    }

    @Override
    public ManifestFile writeToInternalWriter(RowData rowData) {
      return rollingWriter.existingWithResult(
          wrapper.wrap(
              rowData.getRow(positions().get(WriteManifests.DATA_FILE), dataFileFieldCount())),
          rowData.getLong(positions().get(WriteManifests.SNAPSHOT_ID)),
          rowData.getLong(positions().get(WriteManifests.SEQUENCE_NUMBER)),
          rowData.getLong(positions().get(WriteManifests.FILE_SEQUENCE_NUMBER)));
    }
  }
}
