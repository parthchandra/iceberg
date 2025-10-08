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

import org.apache.comet.parquet.NativeColumnReader;
import org.apache.comet.vector.CometDecodedVector;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A wrapper around Comet's NativeColumnReader that implements VectorizedReader<ColumnVector>.
 *
 * <p>This reader delegates column reading operations to the underlying NativeColumnReader, which
 * reads Parquet data directly into Arrow vectors via native code for improved performance.
 */
class CometNativeColumnReader implements VectorizedReader<ColumnVector> {
  private final NativeColumnReader delegate;
  private int batchSize;

  CometNativeColumnReader(NativeColumnReader delegate) {
    this.delegate = delegate;
  }

  public NativeColumnReader delegate() {
    return delegate;
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  public int batchSize() {
    return batchSize;
  }

  @Override
  public ColumnVector read(ColumnVector reuse, int numRowsToRead) {
    // Read the batch using the native reader
    delegate.readBatch(numRowsToRead);

    // Get the current batch as a CometDecodedVector
    CometDecodedVector cometVector = delegate.loadVector();

    // Return the vector as a ColumnVector
    // CometDecodedVector extends ColumnVector in Spark
    return cometVector;
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }
}