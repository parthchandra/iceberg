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

import com.apple.boson.parquet.BosonIcebergColumnReader;
import com.apple.boson.parquet.BosonIcebergDeleteColumnReader;
import com.apple.boson.vector.BosonIcebergVector;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.spark.data.vectorized.BaseColumnBatchLoader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized
 * read path. The {@link ColumnarBatch} returned is created by passing in the Arrow vectors
 * populated via delegated read calls to {@linkplain BosonIcebergColumnReader VectorReader(s)}.
 */
public class BosonColumnarBatchReader extends BosonBaseBatchReader<ColumnarBatch> {

  private final boolean hasIsDeletedColumn;
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;

  public BosonColumnarBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
    this.hasIsDeletedColumn =
        readers.stream().anyMatch(reader -> reader instanceof BosonIcebergDeleteColumnReader);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    super.setRowGroupInfo(pageStore, metaData, rowPosition);
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

      for (int i = 0; i < readers.length; i++) {
        readers[i].read(null, numRowsToRead);
        BosonIcebergVector bv = readers[i].getVector();
        bv.setRowIdMapping(rowIdMapping);
        columnVectors[i] = bv;
      }

      return columnVectors;
    }

    @Override
    protected void readDeletedColumnIfNecessary(ColumnVector[] columnVectors) {
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] instanceof BosonIcebergDeleteColumnReader) {
          BosonIcebergDeleteColumnReader deleteColumnReader =
              new BosonIcebergDeleteColumnReader<>(isDeleted);
          deleteColumnReader.setBatchSize(numRowsToRead);
          deleteColumnReader.read(null, numRowsToRead);
          columnVectors[i] = deleteColumnReader.getVector();
        }
      }
    }
  }
}
