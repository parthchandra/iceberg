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

import com.apple.boson.parquet.ConstantColumnReader;
import com.apple.boson.parquet.MetadataColumnReader;
import com.apple.boson.parquet.Native;
import com.apple.boson.parquet.TypeUtil;
import org.apache.iceberg.spark.BosonReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

public class BosonDeleteColumnReader<T> extends BosonColumnReader {
  public BosonDeleteColumnReader(Types.NestedField field, BosonReadOptions bosonReadOptions) {
    super(field, bosonReadOptions);
    delegate =
        new ConstantColumnReader(
            getSparkType(), getDescriptor(), false, bosonReadOptions.getUseDecimal128());
  }

  public BosonDeleteColumnReader(boolean[] isDeleted, BosonReadOptions bosonReadOptions) {
    super(
        DataTypes.BooleanType,
        TypeUtil.convertToParquet(
            new StructField("deleted", DataTypes.BooleanType, false, Metadata.empty())),
        bosonReadOptions);
    delegate = new DeleteColumnReader(isDeleted, bosonReadOptions);
  }

  @Override
  public void setBatchSize(int batchSize) {
    delegate.setBatchSize(batchSize);
    this.batchSize = batchSize;
    initialized = true;
  }

  private static class DeleteColumnReader extends MetadataColumnReader {
    private boolean[] isDeleted;

    DeleteColumnReader(boolean[] isDeleted, BosonReadOptions bosonReadOptions) {
      super(
          DataTypes.BooleanType,
          TypeUtil.convertToParquet(
              new StructField("deleted", DataTypes.BooleanType, false, Metadata.empty())),
          bosonReadOptions.getUseDecimal128());
      this.isDeleted = isDeleted;
    }

    @Override
    public void readBatch(int total) {
      Native.resetBatch(nativeHandle);
      Native.setIsDeleted(nativeHandle, isDeleted);

      super.readBatch(total);
    }
  }
}
