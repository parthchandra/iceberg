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

import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A column reader for constant struct values. This reader uses {@link ConstantColumnVector} to
 * handle struct values including InternalRow, which Comet's constant reader doesn't support.
 */
class CometConstantStructColumnReader extends BaseCometColumnReader<ConstantColumnVector> {

  private final Object constantValue;
  private final Types.NestedField field;

  CometConstantStructColumnReader(Object value, Types.NestedField field) {
    this.constantValue = value;
    this.field = field;
  }

  @Override
  public void setBatchSize(int batchSize) {
    // No initialization needed for constant columns
  }

  @Override
  public ColumnVector read(ColumnVector reuse, int numRowsToRead) {
    return getConstantVector(numRowsToRead);
  }

  /**
   * Returns a ConstantColumnVector for the specified batch size.
   *
   * @param batchSize the number of rows in the batch
   * @return a ConstantColumnVector containing the constant value
   */
  ConstantColumnVector getConstantVector(int batchSize) {
    return new ConstantColumnVector(field.type(), batchSize, constantValue);
  }

  @Override
  public void close() {
    // No resources to close for constant column vectors
  }
}
