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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;

public class BosonVectorizedSparkParquetReaders {

  private BosonVectorizedSparkParquetReaders() {}

  public static BosonColumnarBatchReader buildReader(
      Schema expectedSchema, MessageType fileSchema) {
    return buildReader(expectedSchema, fileSchema, Maps.newHashMap());
  }

  public static BosonColumnarBatchReader buildReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return (BosonColumnarBatchReader)
        TypeWithSchemaVisitor.visit(
            expectedSchema.asStruct(),
            fileSchema,
            new BosonVectorizedReaderBuilder(
                expectedSchema, fileSchema, idToConstant, BosonColumnarBatchReader::new));
  }

  public static BosonColumnarBatchReader buildReader(
      Schema expectedSchema,
      MessageType fileSchema,
      Map<Integer, ?> idToConstant,
      DeleteFilter<InternalRow> deleteFilter) {
    return (BosonColumnarBatchReader)
        TypeWithSchemaVisitor.visit(
            expectedSchema.asStruct(),
            fileSchema,
            new ReaderBuilder(
                expectedSchema,
                fileSchema,
                idToConstant,
                BosonColumnarBatchReader::new,
                deleteFilter));
  }

  private static class ReaderBuilder extends BosonVectorizedReaderBuilder {
    private final DeleteFilter<InternalRow> deleteFilter;

    ReaderBuilder(
        Schema expectedSchema,
        MessageType parquetSchema,
        Map<Integer, ?> idToConstant,
        Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
        DeleteFilter<InternalRow> deleteFilter) {
      super(expectedSchema, parquetSchema, idToConstant, readerFactory);
      this.deleteFilter = deleteFilter;
    }

    @Override
    protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
      VectorizedReader<?> reader = super.vectorizedReader(reorderedFields);
      if (deleteFilter != null) {
        ((BosonColumnarBatchReader) reader).setDeleteFilter(deleteFilter);
      }
      return reader;
    }
  }
}
