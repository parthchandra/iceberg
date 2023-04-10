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

import com.apple.boson.vector.BosonVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A Spark ColumnVector implementation backed by Arrow arrays. This is used by Iceberg's
 * vectorization through Boson
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class BosonIcebergVector extends BosonVector {

  // the rowId mapping to skip deleted rows for all column vectors inside a batch
  // Here is an example:
  // [0,1,2,3,4,5,6,7] -- Original status of the row id mapping array
  // Position delete 2, 6
  // [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6]
  // Equality delete 1 <= x <= 3
  // [0,4,5,7,-,-,-,-] -- After applying equality deletes [Set Num records to 4]
  protected int[] rowIdMapping;

  /** The delegate Boson vector containing the actual data. */
  private BosonVector delegate;

  public BosonIcebergVector(DataType type) {
    // FIXME: infer the following boolean flag (from 'spark.boson.use.decimal128') once native
    //  execution is enabled for Iceberg
    super(type, false);
  }

  public void setDelegate(BosonVector delegate) {
    this.delegate = delegate;
  }

  public void setRowIdMapping(int[] rowIdMapping) {
    this.rowIdMapping = rowIdMapping;
  }

  @Override
  public ValueVector getValueVector() {
    return delegate.getValueVector();
  }

  @Override
  public void setNumNulls(int numNulls) {
    delegate.setNumNulls(numNulls);
  }

  @Override
  public void setNumValues(int numValues) {
    delegate.setNumValues(numValues);
  }

  @Override
  public boolean hasNull() {
    return delegate.hasNull();
  }

  @Override
  public int numNulls() {
    return delegate.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.isNullAt(newRowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getBoolean(newRowId);
  }

  @Override
  public byte getByte(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getByte(newRowId);
  }

  @Override
  public short getShort(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getShort(newRowId);
  }

  @Override
  public int getInt(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getInt(newRowId);
  }

  @Override
  public long getLong(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getLong(newRowId);
  }

  @Override
  public float getFloat(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getFloat(newRowId);
  }

  @Override
  public double getDouble(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getDouble(newRowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    int newRowId = rowId;
    if (isNullAt(newRowId)) {
      return null;
    }
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getDecimal(newRowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    int newRowId = rowId;
    if (isNullAt(newRowId)) {
      return null;
    }
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getUTF8String(newRowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    int newRowId = rowId;
    if (isNullAt(newRowId)) {
      return null;
    }
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return delegate.getBinary(newRowId);
  }
}
