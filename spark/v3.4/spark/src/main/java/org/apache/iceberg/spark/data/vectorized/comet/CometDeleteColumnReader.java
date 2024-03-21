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

import org.apache.comet.parquet.ConstantColumnReader;
import org.apache.comet.parquet.MetadataColumnReader;
import org.apache.comet.parquet.Native;
import org.apache.comet.parquet.TypeUtil;
import org.apache.iceberg.spark.CometReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

public class CometDeleteColumnReader<T> extends CometColumnReader {
  public CometDeleteColumnReader(Types.NestedField field, CometReadOptions cometReadOptions) {
    super(field, cometReadOptions);
    delegate =
        new ConstantColumnReader(
            getSparkType(), getDescriptor(), false, cometReadOptions.getUseDecimal128());
  }

  public CometDeleteColumnReader(boolean[] isDeleted, CometReadOptions cometReadOptions) {
    super(
        DataTypes.BooleanType,
        TypeUtil.convertToParquet(
            new StructField("deleted", DataTypes.BooleanType, false, Metadata.empty())),
        cometReadOptions);
    delegate = new DeleteColumnReader(isDeleted, cometReadOptions);
  }

  @Override
  public void setBatchSize(int batchSize) {
    delegate.setBatchSize(batchSize);
    this.batchSize = batchSize;
    initialized = true;
  }

  private static class DeleteColumnReader extends MetadataColumnReader {
    private boolean[] isDeleted;

    DeleteColumnReader(boolean[] isDeleted, CometReadOptions cometReadOptions) {
      super(
          DataTypes.BooleanType,
          TypeUtil.convertToParquet(
              new StructField("deleted", DataTypes.BooleanType, false, Metadata.empty())),
          cometReadOptions.getUseDecimal128());
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
