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
package org.apache.iceberg.spark.data.vectorized;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.comet.CometRuntimeException;
import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.IcebergCometNativeBatchReader;
import org.apache.comet.parquet.NativeBatchReader;
import org.apache.comet.parquet.NativeColumnReader;
import org.apache.comet.vector.CometSelectionVector;
import org.apache.comet.vector.CometVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.parquet.NativeReadConf;
import org.apache.iceberg.parquet.NativeVectorizedReader;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.CometDeleteColumnReader.DeleteColumnReader;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NativeVectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's
 * vectorized read path. The {@link ColumnarBatch} returned is created by passing in the Arrow
 * vectors populated via delegated read calls to {@link CometNativeColumnReader
 * NativeColumnReader(s)}.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
class CometNativeColumnarBatchReader implements NativeVectorizedReader<ColumnarBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(CometNativeColumnarBatchReader.class);

  private final BaseCometColumnReader<?>[] readers;
  private final boolean hasIsDeletedColumn;

  // The delegated BatchReader on the Comet side does the real work of loading a batch of rows
  // The Comet NativeBatchReader contains an array of NativeColumnReader. There is no need to
  // explicitly call
  // NativeColumnReader.readBatch; instead, NativeBatchReader.nextBatch will be called, which
  // underneath calls
  // NativeColumnReader.readBatch. The only exception is DeleteColumnReader, because at the time of
  // calling NativeBatchReader.nextBatch, the isDeleted value is not yet available, so
  // DeleteColumnReader.readBatch must be called explicitly later, after the isDeleted value is
  // available.
  private IcebergCometNativeBatchReader delegate = null;
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;
  private NativeReadConf<?> conf = null;
  private final Schema schema;

  CometNativeColumnarBatchReader(List<VectorizedReader<?>> readers, Schema schema) {
    this.schema = schema;
    this.readers =
        readers.stream()
            .map(BaseCometColumnReader.class::cast)
            .toArray(BaseCometColumnReader[]::new);
    this.hasIsDeletedColumn =
        readers.stream().anyMatch(reader -> reader instanceof CometDeleteColumnReader);
  }

  @Override
  public void init(NativeReadConf<?> readConf, long start, long length) {
    LOG.info(
        "COMET_NATIVE: Initializing CometNativeColumnarBatchReader for file: {}, start: {}, length: {}",
        readConf.file().location(),
        start,
        length);
    this.conf = readConf;
    this.delegate = new IcebergCometNativeBatchReader(SparkSchemaUtil.convert(schema));
    // Initialize the native batch reader with parameters from NativeReadConf
    try {
      // Get Configuration from HadoopInputFile
      Configuration hadoopConf;

      if (readConf.file() instanceof HadoopInputFile) {
        HadoopInputFile hadoopInputFile = (HadoopInputFile) readConf.file();
        hadoopConf = hadoopInputFile.getConf();
      } else {
        // Use default Hadoop configuration if file is not a HadoopInputFile
        hadoopConf = new Configuration();
      }

      // Get ParquetMetadata from the file reader (we need to open it to get metadata)
      ParquetMetadata metadata;
      metadata = readConf.reader().getFooter();

      // Create FileInfo from NativeReadConf
      NativeBatchReader.FileInfo fileInfo =
          new NativeBatchReader.FileInfo(
              start, length, readConf.file().location(), readConf.file().getLength());

      // Convert ParquetMetadata to JSON
      byte[] metadataBytes = new ParquetMetadataSerializer().serialize(metadata);

      // Iceberg predicate are already applied in the NativeVectorizedParquetReade to prune out row
      // groups that can be skipped
      byte[] nativeFilter = null;

      // Get Spark schema from the vectorized model
      // The schema is already set in the delegate during construction
      StructType sparkSchema = delegate.getSparkSchema();

      // Construct preInitializedReaders array from the delegates already assigned in the readers
      // array
      // If a reader does not have a delegate assigned, the corresponding entry should be null
      AbstractColumnReader[] preInitializedReaders = new AbstractColumnReader[readers.length];
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] != null) {
          Object readerDelegate = readers[i].delegate();
          if (readerDelegate != null && readerDelegate instanceof AbstractColumnReader) {
            preInitializedReaders[i] = (AbstractColumnReader) readerDelegate;
          } else {
            preInitializedReaders[i] = null;
          }
        } else {
          preInitializedReaders[i] = null;
        }
      }

      // Initialize the native reader
      delegate.init(
          hadoopConf,
          fileInfo,
          metadataBytes,
          nativeFilter,
          readConf.batchSize(),
          sparkSchema,
          true, // caseSensitive - from ReadConf
          true, // useFieldId - Iceberg uses field IDs
          false, // ignoreMissingIds
          false, // useLegacyDateTimestamp
          null, // partitionSchema - no partitions for now
          null, // partitionValues - no partitions for now
          preInitializedReaders, // preInitializedReaders
          Collections.emptyMap() // metrics
          );

      // Match up Iceberg readers with Comet delegate column readers
      // matchReadersWithDelegateColumnReaders();

    } catch (Throwable e) {
      throw new RuntimeIOException(
          new IOException("Failed to initialize IcebergCometNativeBatchReader", e));
    }
  }

  @SuppressWarnings("unchecked")
  private void matchReadersWithDelegateColumnReaders() {
    if (delegate == null) {
      throw new IllegalStateException("Delegate must be initialized before matching readers");
    }

    AbstractColumnReader[] delegateColumnReaders = delegate.getColumnReaders();

    if (delegateColumnReaders == null) {
      throw new IllegalStateException("Delegate column readers are null");
    }

    if (readers.length != delegateColumnReaders.length) {
      throw new IllegalStateException(
          String.format(
              "Mismatch between number of readers (%d) and delegate column readers (%d)",
              readers.length, delegateColumnReaders.length));
    }

    // Match each reader with its corresponding delegate column reader
    for (int i = 0; i < readers.length; i++) {
      if (readers[i] != null && delegateColumnReaders[i] != null) {
        ((BaseCometColumnReader) readers[i]).setDelegate(delegateColumnReaders[i]);
      }
    }
  }

  @Override
  public void reset() {
    this.delegate = null;
    this.conf = null;
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData) {
    throw new UnsupportedOperationException(
        "Comet native vectorized reader does not support setRowGroupInfo");
  }

  public void setDeleteFilter(DeleteFilter<InternalRow> deleteFilter) {
    this.deletes = deleteFilter;
  }

  @Override
  public final ColumnarBatch read(ColumnarBatch reuse, int numRowsToRead) {
    ColumnarBatch columnarBatch = new ColumnBatchLoader(numRowsToRead).loadDataToColumnBatch();
    rowStartPosInBatch += numRowsToRead;
    return columnarBatch;
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (BaseCometColumnReader<?> reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }

  @Override
  public void close() {
    for (BaseCometColumnReader<?> reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Encode a file path for use in a URI while preserving the URI structure (scheme, authority,
   * path). This method properly encodes special characters like spaces, colons, plus signs, etc. in
   * the path component while keeping the URI scheme and authority intact.
   *
   * @param filePath The file path to encode (may include URI scheme like file://, s3://, etc.)
   * @return The encoded file path
   */
  private static String encodeFilePath(String filePath) {
    try {
      // Try to parse as URI to extract components
      URI uri = new URI(filePath);

      // If no scheme, it's a plain file path - return as is since FileInfo will add file://
      if (uri.getScheme() == null) {
        return filePath;
      }

      // Has a scheme - encode only the path component
      String scheme = uri.getScheme();
      String authority = uri.getAuthority();
      String path = uri.getPath();
      String query = uri.getQuery();
      String fragment = uri.getFragment();

      // Encode the path by splitting into segments and encoding each one
      String encodedPath = path != null ? encodePathSegments(path) : "";

      // Reconstruct the URI
      StringBuilder result = new StringBuilder();
      result.append(scheme).append("://");
      if (authority != null) {
        result.append(authority);
      }
      result.append(encodedPath);
      if (query != null) {
        result.append("?").append(query);
      }
      if (fragment != null) {
        result.append("#").append(fragment);
      }

      return result.toString();
    } catch (URISyntaxException e) {
      // If parsing fails, try manual parsing
      int schemeIndex = filePath.indexOf("://");
      if (schemeIndex > 0) {
        String scheme = filePath.substring(0, schemeIndex);
        String rest = filePath.substring(schemeIndex + 3);

        // Split into authority and path
        int pathStart = rest.indexOf('/');
        if (pathStart >= 0) {
          String authority = rest.substring(0, pathStart);
          String path = rest.substring(pathStart);
          String encodedPath = encodePathSegments(path);
          return scheme + "://" + authority + encodedPath;
        } else {
          // No path, just authority
          return filePath;
        }
      }
      // No scheme at all - return as is
      return filePath;
    }
  }

  /**
   * Encode individual path segments in a URI path, preserving the '/' separators. This encodes
   * special characters like spaces (as %20), plus signs, colons, equals signs, etc.
   *
   * @param path The path to encode (should start with /)
   * @return The encoded path with '/' separators preserved
   */
  private static String encodePathSegments(String path) {
    if (path == null || path.isEmpty()) {
      return path;
    }

    // Split by / to preserve path structure
    String[] segments = path.split("/", -1);
    StringBuilder encoded = new StringBuilder();

    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        encoded.append('/');
      }
      String segment = segments[i];
      if (!segment.isEmpty()) {
        // URLEncoder encodes space as +, but URIs use %20
        // It also encodes / which we don't want since we're encoding segments
        encoded.append(URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20"));
      }
    }

    return encoded.toString();
  }

  private class ColumnBatchLoader {
    private final int batchSize;

    ColumnBatchLoader(int numRowsToRead) {
      Preconditions.checkArgument(
          numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
      this.batchSize = numRowsToRead;
    }

    ColumnarBatch loadDataToColumnBatch() {
      ColumnVector[] vectors = readDataToColumnVectors();
      int numLiveRows = batchSize;

      if (hasIsDeletedColumn) {
        boolean[] isDeleted = buildIsDeleted(vectors);
        readDeletedColumn(vectors, isDeleted);
        throw new CometRuntimeException("Comet native reader does not support deleted columns");
      } else {
        Pair<int[], Integer> pair = buildRowIdMapping(vectors);
        if (pair != null) {
          int[] rowIdMapping = pair.first();
          if (pair.second() != null) {
            numLiveRows = pair.second();
            for (int i = 0; i < vectors.length; i++) {
              if (vectors[i] instanceof CometVector) {
                vectors[i] =
                    new CometSelectionVector((CometVector) vectors[i], rowIdMapping, numLiveRows);
              } else {
                throw new CometRuntimeException(
                    "Unsupported column vector type: " + vectors[i].getClass());
              }
            }
          }
        }
      }

      if (deletes != null && deletes.hasEqDeletes()) {
        vectors = ColumnarBatchUtil.removeExtraColumns(deletes, vectors);
      }

      ColumnarBatch batch = new ColumnarBatch(vectors);
      batch.setNumRows(numLiveRows);
      return batch;
    }

    private boolean[] buildIsDeleted(ColumnVector[] vectors) {
      return ColumnarBatchUtil.buildIsDeleted(vectors, deletes, rowStartPosInBatch, batchSize);
    }

    private Pair<int[], Integer> buildRowIdMapping(ColumnVector[] vectors) {
      return ColumnarBatchUtil.buildRowIdMapping(vectors, deletes, rowStartPosInBatch, batchSize);
    }

    ColumnVector[] readDataToColumnVectors() {
      LOG.info(
          "COMET_NATIVE: Reading data to column vectors, batch size: {}, num readers: {}",
          batchSize,
          readers.length);
      ColumnVector[] columnVectors = new ColumnVector[readers.length];
      // Fetch rows for all readers in the delegate
      try {
        delegate.nextBatch();
        // Comet's NativeBatchReader reinitializes the column readers after every batch is read so
        // we need to do this matching for every batch.
        matchReadersWithDelegateColumnReaders();
      } catch (IOException e) {
        throw new CometRuntimeException("Failed to get next batch from native read", e);
      }
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] instanceof CometConstantStructColumnReader) {
          // Special handling for struct constant column readers since they don't use native readers
          CometConstantStructColumnReader structReader =
              (CometConstantStructColumnReader) readers[i];
          columnVectors[i] = structReader.getConstantVector(batchSize);
        } else {
          Object delegateReader = readers[i].delegate();
          if (delegateReader instanceof NativeColumnReader) {
            NativeColumnReader nativeReader = (NativeColumnReader) delegateReader;
            nativeReader.readBatch(batchSize);
            columnVectors[i] = nativeReader.currentBatch();
          } else {
            AbstractColumnReader columnReader = (AbstractColumnReader) delegateReader;
            columnReader.readBatch(batchSize);
            columnVectors[i] = columnReader.currentBatch();
          }
        }
      }
      return columnVectors;
    }

    void readDeletedColumn(ColumnVector[] columnVectors, boolean[] isDeleted) {
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] instanceof CometDeleteColumnReader) {
          CometDeleteColumnReader<AbstractColumnReader> deleteColumnReader =
              new CometDeleteColumnReader<>(isDeleted);
          deleteColumnReader.setBatchSize(batchSize);
          DeleteColumnReader deleted = (DeleteColumnReader) deleteColumnReader.delegate();
          deleted.readBatch(batchSize);
          columnVectors[i] = deleted.currentBatch();
        }
      }
    }
  }
}
