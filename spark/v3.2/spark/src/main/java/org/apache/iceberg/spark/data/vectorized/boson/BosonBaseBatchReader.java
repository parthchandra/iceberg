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
import com.apple.boson.parquet.BosonIcebergConstantColumnReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * A base BatchReader class that contains common functionality and calls Boson ColumnReader
 */
public abstract class BosonBaseBatchReader<T> implements VectorizedReader<T> {

  private final BosonIcebergColumnReader[] readers;

  protected BosonBaseBatchReader(List<VectorizedReader<?>> readers) {
    this.readers = readers.stream()
      .map(BosonIcebergColumnReader.class::cast)
      .toArray(BosonIcebergColumnReader[]::new);
  }

  public BosonIcebergColumnReader[] getReaders() {
    return readers;
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    for (int i = 0; i < readers.length; i++) {
      if (readers[i] != null) {
        try {
          if (!(readers[i] instanceof BosonIcebergConstantColumnReader)) {
            readers[i].reset();
            readers[i].setPageReader(pageStore.getPageReader(readers[i].getDescriptor()));
          }
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to setRowGroupInfo for Boson vectorization", e);
        }
      }
    }
  }

  @Override
  public void close() {
    for (BosonIcebergColumnReader reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (BosonIcebergColumnReader reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }
}
