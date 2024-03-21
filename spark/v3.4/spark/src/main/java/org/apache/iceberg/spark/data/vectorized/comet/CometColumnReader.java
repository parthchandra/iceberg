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
package org.apache.iceberg.spark.data.vectorized.comet;

import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.ColumnReader;
import org.apache.comet.parquet.TypeUtil;
import org.apache.comet.parquet.Utils;
import org.apache.comet.vector.CometVector;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.CometReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

/**
 * A Parquet column reader backed by a Comet {@link ColumnReader}. This class should be used
 * together with {@link CometIcebergVector}.
 *
 * <p>Example:
 *
 * <pre>
 *   CometColumnReader reader = ...
 *   reader.setBatchSize(batchSize);
 *
 *   while (hasMoreRowsToRead) {
 *     if (endOfRowGroup) {
 *       reader.reset();
 *       PageReader pageReader = ...
 *       reader.setPageReader(pageReader);
 *     }
 *
 *     int numRows = ...
 *     CometIcebergVector vector = reader.read(null, numRows);
 *
 *     // consume the vector
 *   }
 *
 *   reader.close();
 * </pre>
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class CometColumnReader implements VectorizedReader<CometIcebergVector> {
  public static final int DEFAULT_BATCH_SIZE = 5000;

  private final DataType sparkType;
  protected AbstractColumnReader delegate;
  private final CometIcebergVector vector;
  private final ColumnDescriptor descriptor;
  protected boolean initialized = false;
  protected int batchSize = DEFAULT_BATCH_SIZE;
  private final CometReadOptions cometReadOptions;

  public CometColumnReader(
      DataType sparkType, ColumnDescriptor descriptor, CometReadOptions cometReadOptions) {
    this.sparkType = sparkType;
    this.descriptor = descriptor;
    this.cometReadOptions = cometReadOptions;
    this.vector = new CometIcebergVector(sparkType, cometReadOptions.getUseDecimal128());
  }

  public CometColumnReader(Types.NestedField field, CometReadOptions cometReadOptions) {
    DataType dataType = SparkSchemaUtil.convert(field.type());
    StructField structField = new StructField(field.name(), dataType, false, Metadata.empty());
    this.sparkType = dataType;
    this.descriptor = TypeUtil.convertToParquet(structField);
    this.cometReadOptions = cometReadOptions;
    this.vector = new CometIcebergVector(sparkType, cometReadOptions.getUseDecimal128());
  }

  public AbstractColumnReader getDelegate() {
    return delegate;
  }

  /**
   * This method is to initialized/reset the ColumnReader. This needs to be called for each row
   * group after readNextRowGroup, so a new dictionary encoding can be set for each of the new row
   * groups.
   */
  public void reset() {
    if (delegate != null) {
      delegate.close();
    }

    delegate =
        Utils.getColumnReader(
            sparkType,
            descriptor,
            batchSize,
            cometReadOptions.getUseDecimal128(),
            cometReadOptions.getUseLazyMaterialization());
    initialized = true;
  }

  @Override
  public CometIcebergVector read(CometIcebergVector reuse, int numRows) {
    delegate.readBatch(numRows);
    CometVector bv = delegate.currentBatch();
    vector.setDelegate(bv);
    return reuse;
  }

  public ColumnDescriptor getDescriptor() {
    return descriptor;
  }

  public CometIcebergVector getVector() {
    return vector;
  }

  /** Returns the Spark data type for this column. */
  public DataType getSparkType() {
    return sparkType;
  }

  /**
   * Set the page reader to be 'pageReader'.
   *
   * <p>NOTE: this should be called before reading a new Parquet column chunk.
   */
  public void setPageReader(PageReader pageReader) throws IOException {
    reset();
    if (!initialized) {
      throw new IllegalStateException("Invalid state: 'reset' should be called first");
    }
    ((ColumnReader) delegate).setPageReader(pageReader);
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }

  @Override
  public void setBatchSize(int batchSize) {
    Preconditions.checkArgument(
        !initialized, "'reset' shouldn't be called  before 'setBatchSize' is called");
    this.batchSize = batchSize;
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageReadStore, Map<ColumnPath, ColumnChunkMetaData> map, long l) {
    throw new UnsupportedOperationException("Not supported");
  }
}
