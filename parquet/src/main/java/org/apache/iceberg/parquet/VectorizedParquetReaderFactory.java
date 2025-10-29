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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.schema.MessageType;

/**
 * Factory interface for creating vectorized Parquet readers. Implementations can be registered via
 * Java's ServiceLoader mechanism to provide custom reader implementations (e.g., for Comet
 * integration).
 *
 * <p>Factories are discovered and loaded at runtime. When multiple factories are available, they
 * are checked in priority order (highest first). The first factory that returns a non-null reader
 * from {@link #createReader} will be used.
 *
 * <p>To register a factory implementation, create a file at: {@code
 * META-INF/services/org.apache.iceberg.parquet.VectorizedParquetReaderFactory} containing the fully
 * qualified class name of your implementation.
 */
public interface VectorizedParquetReaderFactory {

  /**
   * Creates a vectorized reader for the given parameters.
   *
   * @param input the input file to read
   * @param expectedSchema the expected Iceberg schema
   * @param options Parquet read options
   * @param readerFunc function to create the vectorized reader from MessageType
   * @param nameMapping optional name mapping for schema evolution
   * @param filter optional filter expression
   * @param reuseContainers whether to reuse container objects
   * @param caseSensitive whether column name matching is case-sensitive
   * @param maxRecordsPerBatch maximum number of records per batch
   * @param properties additional properties (may contain reader-specific configuration)
   * @param start optional start position in the file
   * @param length optional length to read from the file
   * @param fileEncryptionKey optional encryption key
   * @param fileAADPrefix optional AAD prefix for encryption
   * @param <T> the type of records returned by the reader
   * @return a CloseableIterable reader, or null if this factory cannot handle the request
   */
  <T> CloseableIterable<T> createReader(
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
      ByteBuffer fileAADPrefix);

  /**
   * Returns the priority of this factory. Higher priority factories are checked first.
   *
   * <p>Default priority is 0. Implementations should return a higher value to take precedence over
   * the default reader.
   *
   * @return the priority (higher values are checked first)
   */
  default int priority() {
    return 0;
  }

  /**
   * Returns true if this factory can handle the request based on the provided properties.
   *
   * <p>This method is called before {@link #createReader} to quickly determine if a factory is
   * applicable. Implementations should check for specific configuration flags or properties that
   * enable their reader.
   *
   * @param properties the properties map that may contain reader-specific configuration
   * @return true if this factory can handle the request
   */
  boolean canHandle(Map<String, String> properties);
}
