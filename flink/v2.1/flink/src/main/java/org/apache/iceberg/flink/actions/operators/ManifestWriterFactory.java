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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;

/**
 * Factory to creating manifest writers. @TODO: Duplicated from
 * RewriteManifestsSparkAction$ManifestWriterFactory, should be cleaned up.
 */
class ManifestWriterFactory implements Serializable {
  private final int formatVersion;
  private final String outputLocation;
  private final long maxManifestSizeBytes;
  private final Integer rowsDivisor;
  private final FileIO io;
  private final PartitionSpec spec;

  ManifestWriterFactory(
      FileIO io,
      PartitionSpec spec,
      int formatVersion,
      String outputLocation,
      long maxManifestSizeBytes,
      Integer rowsDivisor) {
    this.formatVersion = formatVersion;
    this.outputLocation = outputLocation;
    this.maxManifestSizeBytes = maxManifestSizeBytes;
    this.rowsDivisor = rowsDivisor;
    this.io = io;
    this.spec = spec;
  }

  StreamingRollingManifestWriter<DataFile> newRollingManifestWriter() {
    return new StreamingRollingManifestWriter<>(
        this::newManifestWriter, maxManifestSizeBytes, rowsDivisor);
  }

  private ManifestWriter<DataFile> newManifestWriter() {
    return ManifestFiles.write(formatVersion, spec, newOutputFile(), null);
  }

  StreamingRollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
    return new StreamingRollingManifestWriter<>(
        this::newDeleteManifestWriter, maxManifestSizeBytes, rowsDivisor);
  }

  private ManifestWriter<DeleteFile> newDeleteManifestWriter() {
    return ManifestFiles.writeDeleteManifest(formatVersion, spec, newOutputFile(), null);
  }

  private OutputFile newOutputFile() {
    String fileName = FileFormat.AVRO.addExtension("optimized-m-" + UUID.randomUUID());
    return io.newOutputFile(new Path(outputLocation, fileName).toString());
  }

  static class StreamingRollingManifestWriter<F extends ContentFile<F>>
      extends RollingManifestWriter<F> {
    private final AtomicReference<ManifestFile> lastManifestFile = new AtomicReference<>();

    StreamingRollingManifestWriter(
        Supplier<ManifestWriter<F>> manifestWriterSupplier,
        long targetFileSizeInBytes,
        Integer rowsDivisor) {
      super(manifestWriterSupplier, targetFileSizeInBytes, rowsDivisor);
    }

    @Override
    protected void onNewManifestFile(ManifestFile newManifestFile) {
      super.onNewManifestFile(newManifestFile);
      if (lastManifestFile.get() != null) {
        throw new IllegalArgumentException(
            "Multiple new manifests in a single write is not supported");
      }

      lastManifestFile.set(newManifestFile);
    }

    public ManifestFile existingWithResult(
        F existingFile, long fileSnapshotId, long dataSequenceNumber, Long fileSequenceNumber) {
      lastManifestFile.set(null);
      super.existing(existingFile, fileSnapshotId, dataSequenceNumber, fileSequenceNumber);
      return lastManifestFile.get();
    }

    public ManifestFile closeWithResult() throws IOException {
      lastManifestFile.set(null);
      super.close();
      return lastManifestFile.get();
    }
  }
}
