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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;

/**
 * Extended configuration for native Parquet readers that need additional parameters beyond what
 * ReadConf provides.
 *
 * <p>This class stores native-specific parameters like file encryption keys, start/length offsets,
 * and properties that are needed for native reader initialization.
 *
 * @param <T> type of value to read
 */
public class NativeReadConf<T> extends ReadConf<T> {

  private final Map<String, String> properties;
  private final Long start;
  private final Long length;
  private final ByteBuffer fileEncryptionKey;
  private final ByteBuffer fileAADPrefix;

  public NativeReadConf(
      InputFile file,
      ParquetReadOptions options,
      Schema expectedSchema,
      Expression filter,
      Function<org.apache.parquet.schema.MessageType, ParquetValueReader<?>> readerFunc,
      Function<org.apache.parquet.schema.MessageType, VectorizedReader<?>> batchedReaderFunc,
      NameMapping nameMapping,
      boolean reuseContainers,
      boolean caseSensitive,
      Integer batchSize,
      Map<String, String> properties,
      Long start,
      Long length,
      ByteBuffer fileEncryptionKey,
      ByteBuffer fileAADPrefix) {
    super(
        file,
        options,
        expectedSchema,
        filter,
        readerFunc,
        batchedReaderFunc,
        nameMapping,
        reuseContainers,
        caseSensitive,
        batchSize);
    this.properties = properties;
    this.start = start;
    this.length = length;
    this.fileEncryptionKey = fileEncryptionKey;
    this.fileAADPrefix = fileAADPrefix;
  }

  /** Returns the properties map for native reader configuration. */
  public Map<String, String> properties() {
    return properties;
  }

  /** Returns the start offset in the file, or null if reading from the beginning. */
  public Long start() {
    return start;
  }

  /** Returns the length to read from the file, or null if reading to the end. */
  public Long length() {
    return length;
  }

  /** Returns the file encryption key, or null if the file is not encrypted. */
  public ByteBuffer fileEncryptionKey() {
    return fileEncryptionKey;
  }

  /** Returns the file AAD prefix for encryption, or null if not used. */
  public ByteBuffer fileAADPrefix() {
    return fileAADPrefix;
  }


  @Override
  public ParquetFileReader reader() {
    return super.reader();
  }

  @Override
  public InputFile file() {
    return super.file();
  }

  @Override
  public Integer batchSize() {
    return super.batchSize();
  }
}