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
import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.IcebergCometNativeBatchReader;
import org.apache.comet.parquet.NativeColumnReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * A builder that creates CometNativeColumnReader instances from a NativeBatchReader.
 *
 * <p>This builder visits the Iceberg schema and Parquet schema, extracting NativeColumnReader
 * instances from the provided IcebergCometNativeBatchReader and wrapping them in
 * CometNativeColumnReader instances that implement VectorizedReader<ColumnVector>.
 *
 * <p>This builder supports all types including complex types (structs, lists, maps) at the
 * top-level, as NativeColumnReader can return complex Arrow vectors.
 */
class CometNativeVectorizedReaderBuilder extends TypeWithSchemaVisitor<VectorizedReader<?>> {

  private final MessageType parquetSchema;
  private final Schema icebergSchema;
  private final Map<Integer, ?> idToConstant;
  private final Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory;
  private final DeleteFilter<InternalRow> deleteFilter;
  private final IcebergCometNativeBatchReader nativeBatchReader;
  private final AbstractColumnReader[] nativeColumnReaders;

  CometNativeVectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      DeleteFilter<InternalRow> deleteFilter,
      IcebergCometNativeBatchReader nativeBatchReader) {
    this.parquetSchema = parquetSchema;
    this.icebergSchema = expectedSchema;
    this.idToConstant = idToConstant;
    this.readerFactory = readerFactory;
    this.deleteFilter = deleteFilter;
    this.nativeBatchReader = nativeBatchReader;
    this.nativeColumnReaders = nativeBatchReader.getColumnReaders();
  }

  /**
   * Helper method to create a reader for a top-level field by its Parquet field ID.
   * This works for both primitive and complex types.
   */
  private VectorizedReader<?> readerForFieldId(int parquetFieldId) {
    Types.NestedField icebergField = icebergSchema.findField(parquetFieldId);
    if (icebergField == null) {
      return null;
    }

    // Find the index of this field in the schema
    List<Types.NestedField> topLevelFields = icebergSchema.asStruct().fields();
    int columnIndex = -1;
    for (int i = 0; i < topLevelFields.size(); i++) {
      if (topLevelFields.get(i).fieldId() == parquetFieldId) {
        columnIndex = i;
        break;
      }
    }

    if (columnIndex == -1 || columnIndex >= nativeColumnReaders.length) {
      return null;
    }

    AbstractColumnReader abstractReader = nativeColumnReaders[columnIndex];

    // Ensure we have a NativeColumnReader
    Preconditions.checkState(
        abstractReader instanceof NativeColumnReader,
        "Expected NativeColumnReader but got: %s",
        abstractReader.getClass().getName());

    NativeColumnReader nativeReader = (NativeColumnReader) abstractReader;

    // Wrap it in a CometNativeColumnReader
    return new CometNativeColumnReader(nativeReader);
  }

  @Override
  public VectorizedReader<?> message(
      Types.StructType expected, MessageType message, List<VectorizedReader<?>> fieldReaders) {
    GroupType groupType = message.asGroupType();
    Map<Integer, VectorizedReader<?>> readersById = Maps.newHashMap();
    List<Type> fields = groupType.getFields();

    IntStream.range(0, fields.size())
        .filter(pos -> fields.get(pos).getId() != null)
        .forEach(pos -> readersById.put(fields.get(pos).getId().intValue(), fieldReaders.get(pos)));

    List<Types.NestedField> icebergFields =
        expected != null ? expected.fields() : ImmutableList.of();

    List<VectorizedReader<?>> reorderedFields =
        Lists.newArrayListWithExpectedSize(icebergFields.size());

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
    }
    return vectorizedReader(reorderedFields);
  }

  protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
    VectorizedReader<?> reader = readerFactory.apply(reorderedFields);
    if (deleteFilter != null) {
      ((CometColumnarBatchReader) reader).setDeleteFilter(deleteFilter);
    }
    return reader;
  }

  @Override
  public VectorizedReader<?> struct(
      Types.StructType expected, GroupType groupType, List<VectorizedReader<?>> fieldReaders) {
    // Check if this is a top-level struct field
    if (groupType.getId() != null) {
      int fieldId = groupType.getId().intValue();
      Types.NestedField icebergField = icebergSchema.findField(fieldId);
      if (icebergField != null) {
        // Check if this field is a top-level field
        List<Types.NestedField> topLevelFields = icebergSchema.asStruct().fields();
        for (Types.NestedField topField : topLevelFields) {
          if (topField.fieldId() == fieldId) {
            // This is a top-level struct field - return a reader for it
            return readerForFieldId(fieldId);
          }
        }
      }
    }

    // For nested structs, we don't support them yet
    if (expected != null) {
      throw new UnsupportedOperationException(
          "Nested struct fields are not supported for native vectorized reads");
    }
    return null;
  }

  @Override
  public VectorizedReader<?> list(
      Types.ListType expectedList, GroupType array, VectorizedReader<?> elementReader) {
    // Check if this is a top-level list field
    if (array.getId() != null) {
      int fieldId = array.getId().intValue();
      Types.NestedField icebergField = icebergSchema.findField(fieldId);
      if (icebergField != null) {
        // Check if this field is a top-level field
        List<Types.NestedField> topLevelFields = icebergSchema.asStruct().fields();
        for (Types.NestedField topField : topLevelFields) {
          if (topField.fieldId() == fieldId) {
            // This is a top-level list field - return a reader for it
            return readerForFieldId(fieldId);
          }
        }
      }
    }
    // For nested lists, return null (not supported for vectorized reads)
    return null;
  }

  @Override
  public VectorizedReader<?> map(
      Types.MapType expectedMap,
      GroupType map,
      VectorizedReader<?> keyReader,
      VectorizedReader<?> valueReader) {
    // Check if this is a top-level map field
    if (map.getId() != null) {
      int fieldId = map.getId().intValue();
      Types.NestedField icebergField = icebergSchema.findField(fieldId);
      if (icebergField != null) {
        // Check if this field is a top-level field
        List<Types.NestedField> topLevelFields = icebergSchema.asStruct().fields();
        for (Types.NestedField topField : topLevelFields) {
          if (topField.fieldId() == fieldId) {
            // This is a top-level map field - return a reader for it
            return readerForFieldId(fieldId);
          }
        }
      }
    }
    // For nested maps, return null (not supported for vectorized reads)
    return null;
  }

  @Override
  public VectorizedReader<?> primitive(
      org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {

    if (primitive.getId() == null) {
      return null;
    }
    int parquetFieldId = primitive.getId().intValue();

    // Use the helper method to get the reader for this field
    return readerForFieldId(parquetFieldId);
  }
}