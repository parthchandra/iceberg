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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.comet.parquet.NativeColumnReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

/**
 * A builder for creating {@link VectorizedReader} instances that use Comet's {@link
 * NativeColumnReader}.
 *
 * <p>This builder supports all types similar to {@link
 * org.apache.iceberg.spark.data.SparkParquetReaders.ReadBuilder} but returns {@link
 * CometNativeColumnReader} instances instead.
 *
 * <p>The builder accepts a factory function to create {@link NativeColumnReader} instances, since
 * they require a native batch handle that is only available at runtime.
 */
class CometNativeVectorizedReaderBuilder extends TypeWithSchemaVisitor<VectorizedReader<?>> {

  private final MessageType parquetSchema;
  private final Schema icebergSchema;
  private final Map<Integer, ?> idToConstant;
  private final Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory;
  private final DeleteFilter<InternalRow> deleteFilter;
  private final NativeColumnReaderFactory nativeReaderFactory;

  /**
   * Functional interface for creating {@link NativeColumnReader} instances.
   *
   * <p>Since {@link NativeColumnReader} requires a native batch handle and column index that are
   * only available at runtime, this factory allows deferred creation of native readers.
   */
  @FunctionalInterface
  public interface NativeColumnReaderFactory {
    /**
     * Creates a {@link NativeColumnReader} for the given column.
     *
     * @param columnIndex the index of the column in the batch
     * @param sparkType the Spark data type for this column
     * @param parquetType the Parquet field type
     * @param descriptor the Parquet column descriptor
     * @return a new {@link NativeColumnReader} instance
     */
    NativeColumnReader create(
        int columnIndex, DataType sparkType, Type parquetType, ColumnDescriptor descriptor);
  }

  CometNativeVectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      DeleteFilter<InternalRow> deleteFilter,
      NativeColumnReaderFactory nativeReaderFactory) {
    this.parquetSchema = parquetSchema;
    this.icebergSchema = expectedSchema;
    this.idToConstant = idToConstant;
    this.readerFactory = readerFactory;
    this.deleteFilter = deleteFilter;
    this.nativeReaderFactory = nativeReaderFactory;
  }

  @Override
  public VectorizedReader<?> message(
      Types.StructType expected, MessageType message, List<VectorizedReader<?>> fieldReaders) {
    return struct(expected, message.asGroupType(), fieldReaders);
  }

  @Override
  public VectorizedReader<?> struct(
      Types.StructType expected, GroupType groupType, List<VectorizedReader<?>> fieldReaders) {
    if (expected == null) {
      return readerFactory.apply(ImmutableList.of());
    }

    GroupType struct = groupType;
    Map<Integer, VectorizedReader<?>> readersById = Maps.newHashMap();
    List<Type> fields = struct.getFields();

    IntStream.range(0, fields.size())
        .filter(pos -> fields.get(pos).getId() != null)
        .forEach(pos -> readersById.put(fields.get(pos).getId().intValue(), fieldReaders.get(pos)));

    List<Types.NestedField> icebergFields =
        expected != null ? expected.fields() : ImmutableList.of();

    List<VectorizedReader<?>> reorderedFields =
        Lists.newArrayListWithExpectedSize(icebergFields.size());

    int columnIndex = 0;
    for (Types.NestedField field : icebergFields) {
      int id = field.fieldId();
      VectorizedReader<?> reader = readersById.get(id);

      if (idToConstant.containsKey(id)) {
        CometConstantColumnReader constantReader =
            new CometConstantColumnReader<>(idToConstant.get(id), field);
        reorderedFields.add(constantReader);
      } else if (id == MetadataColumns.ROW_POSITION.fieldId()) {
        reorderedFields.add(new CometPositionColumnReader(field));
      } else if (id == MetadataColumns.IS_DELETED.fieldId()) {
        CometColumnReader deleteReader = new CometDeleteColumnReader<>(field);
        reorderedFields.add(deleteReader);
      } else if (reader != null) {
        reorderedFields.add(reader);
      } else if (field.initialDefault() != null) {
        CometColumnReader constantReader =
            new CometConstantColumnReader<>(field.initialDefault(), field);
        reorderedFields.add(constantReader);
      } else if (field.isOptional()) {
        CometColumnReader constantReader = new CometConstantColumnReader<>(null, field);
        reorderedFields.add(constantReader);
      } else {
        throw new IllegalArgumentException(
            String.format("Missing required field: %s", field.name()));
      }
      columnIndex++;
    }

    return vectorizedReader(reorderedFields);
  }

  protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
    VectorizedReader<?> reader = readerFactory.apply(reorderedFields);
    if (deleteFilter != null && reader instanceof CometColumnarBatchReader) {
      ((CometColumnarBatchReader) reader).setDeleteFilter(deleteFilter);
    }
    return reader;
  }

  @Override
  public VectorizedReader<?> list(
      Types.ListType expectedList, GroupType array, VectorizedReader<?> elementReader) {
    // For native vectorized reads, nested types (lists/arrays) are read as complete structures
    // at the native level. The native batch reader handles the full Arrow array structure.
    // We create a native column reader for the entire list column.
    if (array.getId() == null) {
      return null;
    }

    int fieldId = array.getId().intValue();
    Types.NestedField icebergField = icebergSchema.findField(fieldId);
    if (icebergField == null) {
      return null;
    }

    DataType sparkType = SparkSchemaUtil.convert(icebergField.type());
    ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());

    // For lists with primitive elements, we can create a native reader
    // For complex nested elements, return null to fall back
    if (elementReader == null) {
      return null;
    }

    // Create a native column reader for the list column
    NativeColumnReader nativeReader =
        nativeReaderFactory.create(fieldId, sparkType, array, desc);

    return new CometNativeColumnReader(nativeReader);
  }

  @Override
  public VectorizedReader<?> map(
      Types.MapType expectedMap,
      GroupType map,
      VectorizedReader<?> keyReader,
      VectorizedReader<?> valueReader) {
    // For native vectorized reads, nested types (maps) are read as complete structures
    // at the native level. The native batch reader handles the full Arrow map structure.
    // We create a native column reader for the entire map column.
    if (map.getId() == null) {
      return null;
    }

    int fieldId = map.getId().intValue();
    Types.NestedField icebergField = icebergSchema.findField(fieldId);
    if (icebergField == null) {
      return null;
    }

    DataType sparkType = SparkSchemaUtil.convert(icebergField.type());
    ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());

    // For maps with primitive keys/values, we can create a native reader
    // For complex nested keys/values, return null to fall back
    if (keyReader == null || valueReader == null) {
      return null;
    }

    // Create a native column reader for the map column
    NativeColumnReader nativeReader =
        nativeReaderFactory.create(fieldId, sparkType, map, desc);

    return new CometNativeColumnReader(nativeReader);
  }

  @Override
  public VectorizedReader<?> primitive(
      org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {

    if (primitive.getId() == null) {
      return null;
    }

    int parquetFieldId = primitive.getId().intValue();
    ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());

    // Nested types not yet supported for vectorized reads
    if (desc.getMaxRepetitionLevel() > 0) {
      return null;
    }

    Types.NestedField icebergField = icebergSchema.findField(parquetFieldId);
    if (icebergField == null) {
      return null;
    }

    DataType sparkType = SparkSchemaUtil.convert(icebergField.type());

    // Get the Parquet field type from the schema
    Type parquetFieldType = findFieldType(parquetSchema, primitive);

    // Create NativeColumnReader using the factory
    // Note: columnIndex will need to be determined by the caller based on field order
    // For now, we use the field ID as a placeholder - this should be updated by the caller
    NativeColumnReader nativeReader =
        nativeReaderFactory.create(parquetFieldId, sparkType, parquetFieldType, desc);

    return new CometNativeColumnReader(nativeReader);
  }

  /**
   * Finds the field type in the Parquet schema for the given primitive type.
   *
   * @param schema the Parquet message type
   * @param primitive the primitive type to find
   * @return the field type, or the primitive itself if not found
   */
  private Type findFieldType(MessageType schema, PrimitiveType primitive) {
    String[] path = currentPath();
    if (path == null || path.length == 0) {
      return primitive;
    }

    try {
      Type current = schema;
      for (String fieldName : path) {
        if (current instanceof GroupType) {
          current = ((GroupType) current).getType(fieldName);
        } else {
          return primitive;
        }
      }
      return current;
    } catch (Exception e) {
      return primitive;
    }
  }
}