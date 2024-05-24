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
package org.apache.iceberg.parquet;

import com.apple.boson.parquet.BosonInputFile;
import com.apple.boson.parquet.FileReader;
import com.apple.boson.parquet.ReadOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetMetricsCallback;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

public class VectorizedParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, VectorizedReader<?>> batchReaderFunc;
  private final Expression filter;
  private boolean reuseContainers;
  private final boolean caseSensitive;
  private final int batchSize;
  private final NameMapping nameMapping;
  private final boolean useBoson;

  public VectorizedParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      int maxRecordsPerBatch,
      boolean useBoson) {
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
    this.useBoson = useBoson;
  }

  private ReadConf conf = null;

  private ReadConf init() {
    if (conf == null) {
      ReadConf readConf =
          new ReadConf(
              input,
              options,
              expectedSchema,
              filter,
              null,
              batchReaderFunc,
              nameMapping,
              reuseContainers,
              caseSensitive,
              batchSize);
      this.conf = readConf.copy();
      return readConf;
    }
    return conf;
  }

  @Override
  public CloseableIterator<T> iterator() {
    FileIterator<T> iter = new FileIterator<>(init(), options, useBoson);
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private final ReadConf readConf;
    private final ParquetFileReader reader;
    private final FileReader bosonReader;
    private final boolean[] shouldSkip;
    private final VectorizedReader<T> model;
    private final long totalValues;
    private final int batchSize;
    private final List<Map<ColumnPath, ColumnChunkMetaData>> columnChunkMetadata;
    private final boolean reuseContainers;
    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;
    private final long[] rowGroupsStartRowPos;

    FileIterator(ReadConf conf, ParquetReadOptions options, boolean useBoson) {
      this.readConf = conf;
      this.reader = conf.reader();
      if (useBoson) {
        this.bosonReader =
            newBosonReader(
                options, reader.getFooter(), conf.file(), conf.projection(), conf.rowGroups());
      } else {
        this.bosonReader = null;
      }
      this.shouldSkip = conf.shouldSkip();
      this.totalValues = conf.totalValues();
      this.reuseContainers = conf.reuseContainers();
      this.model = conf.vectorizedModel();
      this.batchSize = conf.batchSize();
      this.model.setBatchSize(this.batchSize);
      this.columnChunkMetadata = conf.columnChunkMetadataForRowGroups();
      this.rowGroupsStartRowPos = conf.startRowPositions();
    }

    private FileReader newBosonReader(
        ParquetReadOptions options,
        ParquetMetadata footer,
        InputFile file,
        MessageType projection,
        List<BlockMetaData> rowGroups) {
      try {
        ReadOptions bosonOptions;
        org.apache.parquet.io.InputFile parquetFile;
        if (file instanceof HadoopInputFile) {
          // Use BosonInputFile which contains extra optimizations
          HadoopInputFile hInputFile = (HadoopInputFile) file;
          org.apache.hadoop.conf.Configuration hConfig = hInputFile.getConf();
          parquetFile = BosonInputFile.fromPath(hInputFile.getPath(), hConfig);
          bosonOptions = ReadOptions.builder(hConfig).build();
        } else {
          parquetFile = ParquetIO.file(file);
          bosonOptions = ReadOptions.builder().build();
        }
        FileReader fileReader =
            new FileReader(parquetFile, footer, options, bosonOptions, rowGroups);
        fileReader.setRequestedSchema(projection.getColumns());
        return fileReader;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to open Parquet file: " + file.location(), e);
      }
    }

    @Override
    public boolean hasNext() {
      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      // batchSize is an integer, so casting to integer is safe
      int numValuesToRead = (int) Math.min(nextRowGroupStart - valuesRead, batchSize);
      if (reuseContainers) {
        this.last = model.read(last, numValuesToRead);
      } else {
        this.last = model.read(null, numValuesToRead);
      }
      valuesRead += numValuesToRead;

      return last;
    }

    private void advance() {
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        if (bosonReader != null) {
          bosonReader.skipNextRowGroup();
        } else {
          reader.skipNextRowGroup();
        }
      }
      PageReadStore pages;
      try {
        long startNs = System.nanoTime();
        if (bosonReader != null) {
          pages = bosonReader.readNextRowGroup();
        } else {
          pages = reader.readNextRowGroup();
        }
        ParquetMetricsCallback metricsCallback = readConf.getOptions().getMetricsCallback();
        if (metricsCallback != null) {
          metricsCallback.setValueLong("ParquetLoadRowGroupTime", System.nanoTime() - startNs);
          metricsCallback.setValueLong("ParquetRowGroups", 1);
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      long rowPosition = rowGroupsStartRowPos[nextRowGroup];
      model.setRowGroupInfo(pages, columnChunkMetadata.get(nextRowGroup), rowPosition);
      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;
    }

    @Override
    public void close() throws IOException {
      model.close();
      if (reader != null) {
        reader.close();
      }
      if (bosonReader != null) {
        bosonReader.close();
      }
    }
  }
}
