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
package org.apache.iceberg.spark.parquet;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.CometIOException;
import org.apache.iceberg.parquet.NativeReadConf;
import org.apache.iceberg.parquet.NativeVectorizedReader;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CometNativeVectorizedParquetReader<T> extends CloseableGroup
    implements CloseableIterable<T> {

  private final InputFile input;
  private final ParquetReadOptions options;
  private final Schema expectedSchema;
  private final Function<MessageType, VectorizedReader<?>> batchReaderFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final int batchSize;
  private final NameMapping nameMapping;
  private final Map<String, String> properties;
  private final Long start;
  private final Long length;
  private final ByteBuffer fileEncryptionKey;
  private final ByteBuffer fileAADPrefix;

  public CometNativeVectorizedParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      int maxRecordsPerBatch,
      Map<String, String> properties,
      Long start,
      Long length,
      ByteBuffer fileEncryptionKey,
      ByteBuffer fileAADPrefix) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.batchReaderFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.batchSize = maxRecordsPerBatch;
    this.nameMapping = nameMapping;
    this.properties = properties;
    this.start = start;
    this.length = length;
    this.fileEncryptionKey = fileEncryptionKey;
    this.fileAADPrefix = fileAADPrefix;
  }

  @Override
  public CloseableIterator<T> iterator() {
    FileIterator<T> iter =
        new FileIterator<>(
            input,
            expectedSchema,
            options,
            batchReaderFunc,
            filter,
            reuseContainers,
            caseSensitive,
            batchSize,
            nameMapping,
            properties,
            start,
            length,
            fileEncryptionKey,
            fileAADPrefix);
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private static final Logger LOG = LoggerFactory.getLogger(FileIterator.class);

    private final NativeReadConf<T> readConf;
    private final NativeVectorizedReader<T> model;
    private final int batchSize;
    private T last = null;
    private final long totalValues;
    private long valuesRead = 0;
    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private final boolean[] shouldSkip;
    private final List<BlockMetaData> rowGroups;

    FileIterator(
        InputFile input,
        Schema expectedSchema,
        ParquetReadOptions options,
        Function<MessageType, VectorizedReader<?>> batchReaderFunc,
        Expression filter,
        boolean reuseContainers,
        boolean caseSensitive,
        int batchSize,
        NameMapping nameMapping,
        Map<String, String> properties,
        Long start,
        Long length,
        ByteBuffer fileEncryptionKey,
        ByteBuffer fileAADPrefix) {
      this.batchSize = batchSize;

      // Create NativeReadConf with all the configuration including native-specific parameters
      readConf =
          new NativeReadConf<>(
              input,
              options,
              expectedSchema,
              filter,
              null, // readerFunc - not used for vectorized
              batchReaderFunc,
              nameMapping,
              reuseContainers,
              caseSensitive,
              batchSize,
              properties,
              start,
              length,
              fileEncryptionKey,
              fileAADPrefix);

      this.shouldSkip = readConf.shouldSkip();
      this.totalValues = readConf.totalValues();
      this.rowGroups = readConf.getRowGroups();

      // Get the model from NativeReadConf
      VectorizedReader<T> vectorizedModel = readConf.vectorizedModel();
      if (vectorizedModel instanceof NativeVectorizedReader) {
        model = (NativeVectorizedReader<T>) vectorizedModel;
        model.setBatchSize(this.batchSize);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported model type: " + vectorizedModel.getClass());
      }
    }

    @Override
    public boolean hasNext() {
      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      LOG.info(
          "COMET_NATIVE: next() called - valuesRead: {}, totalValues: {}, nextRowGroupStart: {}, nextRowGroup: {}",
          valuesRead,
          totalValues,
          nextRowGroupStart,
          nextRowGroup);
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      int numValuesToRead = (int) Math.min(nextRowGroupStart - valuesRead, batchSize);
      this.last = model.read(null, numValuesToRead);
      valuesRead += numValuesToRead;

      return last;
    }

    private void advance() {
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        try {
          model.reset(); // sets the delegate to null
        } catch (Exception e) {
          throw CometIOException.fromException("Failed to skip row group", e);
        }
      }
      try {
        BlockMetaData rowGroup = rowGroups.get(nextRowGroup);
        model.init(
            readConf,
            rowGroup.getStartingPos(),
            rowGroup.getCompressedSize()); // creates and initializes a new delegate
        nextRowGroupStart += rowGroups.get(nextRowGroup).getRowCount();
      } catch (Exception e) {
        throw CometIOException.fromException("Failed to read row group", e);
      }
      nextRowGroup += 1;
    }

    @Override
    public void close() {
      model.close();
    }
  }
}
