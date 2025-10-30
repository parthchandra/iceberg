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
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.VectorizedParquetReaderFactory;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory implementation for creating Comet-based vectorized Parquet readers. This factory is
 * registered via Java's ServiceLoader mechanism and will be used when Comet is enabled.
 */
public class CometVectorizedParquetReaderFactory implements VectorizedParquetReaderFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(CometVectorizedParquetReaderFactory.class);
  private static final String COMET_ENABLED_PROPERTY = "spark.comet.enabled";

  private static boolean isCometAvailable() {
    try {
      Class.forName("org.apache.comet.parquet.FileReader");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Creates a Comet vectorized reader if Comet is available and enabled.
   *
   * @return a CometVectorizedParquetReader if Comet is enabled, null otherwise
   */
  @Override
  public <T> CloseableIterable<T> createReader(
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

    // Only create reader if Comet is available
    if (!isCometAvailable()) {
      LOG.debug("Comet is not available in the classpath");
      return null;
    }

    LOG.info("Creating Comet vectorized Parquet reader for: {}", input.location());
    return new CometVectorizedParquetReader<>(
        input,
        expectedSchema,
        options,
        readerFunc,
        nameMapping,
        filter,
        reuseContainers,
        caseSensitive,
        maxRecordsPerBatch,
        properties,
        start,
        length,
        fileEncryptionKey,
        fileAADPrefix);
  }

  /**
   * Returns priority of 100 to ensure this factory is checked before the default reader.
   *
   * @return priority value (higher values are checked first)
   */
  @Override
  public int priority() {
    return 100;
  }

  /**
   * Checks if this factory can handle the request by verifying if Comet is enabled in the
   * properties.
   *
   * @param properties the properties map that may contain comet configuration
   * @return true if Comet is enabled in the properties
   */
  @Override
  public boolean canHandle(Map<String, String> properties) {
    if (properties == null) {
      return false;
    }

    // Check if comet is explicitly enabled in the properties
    String cometEnabled = properties.get(COMET_ENABLED_PROPERTY);
    return "true".equalsIgnoreCase(cometEnabled);
  }
}
