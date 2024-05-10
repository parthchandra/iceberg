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
package org.apache.iceberg.spark.data.vectorized.boson;

import com.apple.boson.parquet.AbstractColumnReader;
import com.apple.boson.parquet.BatchReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.spark.BosonReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.BaseColumnBatchLoader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetMetricsCallback;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized
 * read path. The {@link ColumnarBatch} returned is created by passing in the Arrow vectors
 * populated via delegated read calls to {@linkplain BosonColumnReader VectorReader(s)}.
 */
public class BosonColumnarBatchReader implements VectorizedReader<ColumnarBatch> {

  private final BosonColumnReader[] readers;
  private final boolean hasIsDeletedColumn;
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;
  private final BatchReader delegate;
  private final BosonReadOptions bosonReadOptions;

  public BosonColumnarBatchReader(
      List<VectorizedReader<?>> readers,
      Schema schema,
      BosonReadOptions bosonReadOptions,
      ParquetMetricsCallback metricsCallback) {
    this.readers =
        readers.stream().map(BosonColumnReader.class::cast).toArray(BosonColumnReader[]::new);
    this.hasIsDeletedColumn =
        readers.stream().anyMatch(reader -> reader instanceof BosonDeleteColumnReader);
    this.bosonReadOptions = bosonReadOptions;

    AbstractColumnReader[] abstractColumnReaders = new AbstractColumnReader[readers.size()];
    delegate = new BatchReader(abstractColumnReaders);
    delegate.setMetricsCallback(metricsCallback);
    delegate.setSparkSchema(SparkSchemaUtil.convert(schema));
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    for (int i = 0; i < readers.length; i++) {
      try {
        if (!(readers[i] instanceof BosonConstantColumnReader)
            && !(readers[i] instanceof BosonPositionColumnReader)
            && !(readers[i] instanceof BosonDeleteColumnReader)) {
          readers[i].reset();
          readers[i].setPageReader(pageStore.getPageReader(readers[i].getDescriptor()));
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to setRowGroupInfo for Boson vectorization", e);
      }
    }

    for (int i = 0; i < readers.length; i++) {
      delegate.getColumnReaders()[i] = this.readers[i].getDelegate();
    }

    this.rowStartPosInBatch = rowPosition;
  }

  public void setDeleteFilter(DeleteFilter<InternalRow> deleteFilter) {
    this.deletes = deleteFilter;
  }

  @Override
  public final ColumnarBatch read(ColumnarBatch reuse, int numRowsToRead) {
    ColumnarBatch columnarBatch = new BosonColumnBatchLoader(numRowsToRead).loadDataToColumnBatch();
    rowStartPosInBatch += numRowsToRead;
    return columnarBatch;
  }

  private class BosonColumnBatchLoader extends BaseColumnBatchLoader {
    BosonColumnBatchLoader(int numRowsToRead) {
      super(numRowsToRead, hasIsDeletedColumn, deletes, rowStartPosInBatch);
    }

    @Override
    protected ColumnVector[] readDataToColumnVectors() {
      ColumnVector[] columnVectors = new ColumnVector[readers.length];
      // Fetch rows for all readers in the delegate
      delegate.nextBatch(numRowsToRead);
      for (int i = 0; i < readers.length; i++) {
        BosonIcebergVector bv = readers[i].getVector();
        bv.setDelegate(readers[i].getDelegate().currentBatch());
        bv.setRowIdMapping(rowIdMapping);
        columnVectors[i] = bv;
      }

      return columnVectors;
    }

    @Override
    protected void readDeletedColumnIfNecessary(ColumnVector[] columnVectors) {
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] instanceof BosonDeleteColumnReader) {
          BosonDeleteColumnReader deleteColumnReader =
              new BosonDeleteColumnReader<>(isDeleted, bosonReadOptions);
          deleteColumnReader.setBatchSize(numRowsToRead);
          deleteColumnReader.read(null, numRowsToRead);
          columnVectors[i] = deleteColumnReader.getVector();
        }
      }
    }
  }

  @Override
  public void close() {
    for (BosonColumnReader reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (BosonColumnReader reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }
}
