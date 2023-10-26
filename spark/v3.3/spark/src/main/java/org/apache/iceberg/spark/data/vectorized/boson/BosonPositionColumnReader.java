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

import com.apple.boson.parquet.MetadataColumnReader;
import com.apple.boson.parquet.Native;
import org.apache.iceberg.spark.BosonReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.spark.sql.types.DataTypes;

public class BosonPositionColumnReader extends BosonColumnReader {
  public BosonPositionColumnReader(Types.NestedField field, BosonReadOptions bosonReadOptions) {
    super(field, bosonReadOptions);
    delegate = new PositionColumnReader(getDescriptor(), bosonReadOptions);
  }

  @Override
  public void setBatchSize(int batchSize) {
    delegate.setBatchSize(batchSize);
    this.batchSize = batchSize;
    initialized = true;
  }

  private static class PositionColumnReader extends MetadataColumnReader {
    /** The current position value of the column that are used to initialize this column reader. */
    private long position;

    PositionColumnReader(ColumnDescriptor descriptor, BosonReadOptions bosonReadOptions) {
      this(descriptor, 0L, bosonReadOptions);
    }

    PositionColumnReader(
        ColumnDescriptor descriptor, long position, BosonReadOptions bosonReadOptions) {
      super(DataTypes.LongType, descriptor, bosonReadOptions.getUseDecimal128());
      this.position = position;
    }

    @Override
    public void readBatch(int total) {
      Native.resetBatch(nativeHandle);
      Native.setPosition(nativeHandle, position, total);
      position += total;

      super.readBatch(total);
    }
  }
}
