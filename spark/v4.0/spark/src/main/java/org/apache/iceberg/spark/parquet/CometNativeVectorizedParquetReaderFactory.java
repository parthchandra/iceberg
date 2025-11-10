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

public class CometNativeVectorizedParquetReaderFactory implements VectorizedParquetReaderFactory {
  @Override
  public String name() {
    return "comet-native";
  }

  @Override
  public <T> CloseableIterable<T> createReader(
      InputFile file,
      Schema schema,
      ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> batchedReaderFunc,
      NameMapping mapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      int maxRecordsPerBatch,
      Map<String, String> properties,
      Long start,
      Long length,
      ByteBuffer fileEncryptionKey,
      ByteBuffer fileAADPrefix) {

    return new CometNativeVectorizedParquetReader<>(
        file,
        schema,
        options,
        batchedReaderFunc,
        mapping,
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
}
