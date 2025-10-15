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
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.schema.MessageType;

public class CometNativeVectorizedParquetReader<T> extends CloseableGroup
    implements CloseableIterable<T> {

  private final Function<MessageType, VectorizedReader<?>> batchReaderFunc;
  private final boolean reuseContainers;
  private final int batchSize;
  private final MessageType fileSchema;

  public CometNativeVectorizedParquetReader(
      Function<MessageType, VectorizedReader<?>> batchReaderFunc,
      MessageType fileSchema,
      boolean reuseContainers,
      int batchSize) {
    this.batchReaderFunc = batchReaderFunc;
    this.fileSchema = fileSchema;
    this.reuseContainers = reuseContainers;
    this.batchSize = batchSize;
  }

  @Override
  public CloseableIterator<T> iterator() {
    VectorizedReader<T> model = (VectorizedReader<T>) batchReaderFunc.apply(fileSchema);
    model.setBatchSize(batchSize);
    FileIterator<T> iter = new FileIterator<>(model, reuseContainers, batchSize);
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private final VectorizedReader<T> model;
    private final boolean reuseContainers;
    private final int batchSize;
    private T last = null;
    private boolean hasMoreData = true;

    FileIterator(VectorizedReader<T> model, boolean reuseContainers, int batchSize) {
      this.model = model;
      this.reuseContainers = reuseContainers;
      this.batchSize = batchSize;
    }

    @Override
    public boolean hasNext() {
      return hasMoreData;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (reuseContainers) {
        this.last = model.read(last, batchSize);
      } else {
        this.last = model.read(null, batchSize);
      }

      // For native reader, we need a mechanism to detect end of data
      // This is a simplified implementation - actual implementation would need
      // proper row count tracking from the native batch reader
      if (last == null) {
        hasMoreData = false;
      }

      return last;
    }

    @Override
    public void close() throws IOException {
      model.close();
    }
  }
}
