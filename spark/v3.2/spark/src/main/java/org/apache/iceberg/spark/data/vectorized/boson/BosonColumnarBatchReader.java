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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized read path. The
 * {@link ColumnarBatch} returned is created by passing in the Arrow vectors populated via delegated read calls to
 * {@linkplain BosonIcebergColumnReader VectorReader(s)}.
 */
public class BosonColumnarBatchReader extends BosonBaseBatchReader<ColumnarBatch> {
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;

  public BosonColumnarBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
  }

  @Override
  public void setRowGroupInfo(PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData,
                              long rowPosition) {
    super.setRowGroupInfo(pageStore, metaData, rowPosition);
    this.rowStartPosInBatch = rowPosition;
  }

  public void setDeleteFilter(DeleteFilter<InternalRow> deleteFilter) {
    this.deletes = deleteFilter;
  }

  @Override
  public final ColumnarBatch read(ColumnarBatch reuse, int numRowsToRead) {
    BosonColumnBatchLoader batchLoader =
        new BosonColumnBatchLoader(getReaders(), numRowsToRead, deletes, rowStartPosInBatch);
    rowStartPosInBatch += numRowsToRead;
    return batchLoader.getColumnarBatch();
  }
}
