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

import java.util.List;
import java.util.Map;
import org.apache.comet.CometRuntimeException;
import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.IcebergCometNativeBatchReader;
import org.apache.comet.parquet.NativeColumnReader;
import org.apache.comet.vector.CometSelectionVector;
import org.apache.comet.vector.CometVector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized
 * read path using Comet's native batch reader. The {@link ColumnarBatch} returned is created by
 * passing in the Arrow vectors populated via delegated read calls to {@link CometNativeColumnReader}s.
 *
 * <p>Unlike {@link CometColumnarBatchReader}, this reader uses {@link IcebergCometNativeBatchReader}
 * which reads data directly from native code without requiring per-row-group initialization.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
class CometNativeColumnarBatchReader implements VectorizedReader<ColumnarBatch> {

  private final CometNativeColumnReader[] readers;
  private final boolean hasIsDeletedColumn;

  // The delegated NativeBatchReader on the Comet side does the real work of loading a batch of rows
  // directly from native code. Unlike the regular CometBatchReader, the NativeBatchReader handles
  // all column reading through native code and doesn't require per-row-group initialization.
  private final IcebergCometNativeBatchReader delegate;
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;

  CometNativeColumnarBatchReader(
      List<VectorizedReader<?>> readers,
      Schema schema,
      IcebergCometNativeBatchReader nativeBatchReader) {
    this.readers =
        readers.stream()
            .map(CometNativeColumnReader.class::cast)
            .toArray(CometNativeColumnReader[]::new);
    this.hasIsDeletedColumn =
        readers.stream().anyMatch(reader -> reader instanceof CometDeleteColumnReader);
    this.delegate = nativeBatchReader;
  }

  /**
   * Initialize the native batch reader with all required parameters. This must be called before
   * reading any data.
   *
   * @param conf Hadoop configuration
   * @param inputSplit the partitioned file to read
   * @param parquetMetadataJson ParquetMetadata as JSON string
   * @param nativeFilter optional native filter as byte array
   * @param capacity batch capacity
   * @param dataSchema Spark data schema
   * @param isCaseSensitive whether column resolution is case-sensitive
   * @param useFieldId whether to use field IDs for column resolution
   * @param ignoreMissingIds whether to ignore missing field IDs
   * @param useLegacyDateTimestamp whether to use legacy date/timestamp handling
   * @param partitionSchema Spark partition schema
   * @param partitionValues partition values
   * @param metrics SQL metrics map
   */
  public void initNativeBatchReader(
      org.apache.hadoop.conf.Configuration conf,
      org.apache.spark.sql.execution.datasources.PartitionedFile inputSplit,
      String parquetMetadataJson,
      byte[] nativeFilter,
      int capacity,
      org.apache.spark.sql.types.StructType dataSchema,
      boolean isCaseSensitive,
      boolean useFieldId,
      boolean ignoreMissingIds,
      boolean useLegacyDateTimestamp,
      org.apache.spark.sql.types.StructType partitionSchema,
      InternalRow partitionValues,
      java.util.Map<String, org.apache.spark.sql.execution.metric.SQLMetric> metrics)
      throws Throwable {
    delegate.init(
        conf,
        inputSplit,
        parquetMetadataJson,
        nativeFilter,
        capacity,
        dataSchema,
        isCaseSensitive,
        useFieldId,
        ignoreMissingIds,
        useLegacyDateTimestamp,
        partitionSchema,
        partitionValues,
        metrics);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData) {
    // For native readers, row group info is handled by the NativeBatchReader
    // We just need to track the row offset for delete filtering
    this.rowStartPosInBatch =
        pageStore
            .getRowIndexOffset()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "PageReadStore does not contain row index offset"));
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
    for (CometNativeColumnReader reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }

  @Override
  public void close() {
    for (CometNativeColumnReader reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
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
      ColumnVector[] columnVectors = new ColumnVector[readers.length];

      // Read data for each column using the native reader
      for (int i = 0; i < readers.length; i++) {
        columnVectors[i] = readers[i].read(null, batchSize);
      }

      return columnVectors;
    }

    void readDeletedColumn(ColumnVector[] columnVectors, boolean[] isDeleted) {
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] instanceof CometDeleteColumnReader) {
          CometDeleteColumnReader deleteColumnReader = new CometDeleteColumnReader<>(isDeleted);
          deleteColumnReader.setBatchSize(batchSize);
          deleteColumnReader.delegate().readBatch(batchSize);
          columnVectors[i] = deleteColumnReader.delegate().currentBatch();
        }
      }
    }
  }
}