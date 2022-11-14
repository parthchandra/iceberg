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

import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;

class ColumnBatchLoader extends BaseColumnBatchLoader {
  private final VectorizedArrowReader[] readers;
  private final VectorHolder[] vectorHolders;

  ColumnBatchLoader(VectorizedArrowReader[] readers, VectorHolder[] vectorHolders,
                    int numRowsToRead, DeleteFilter<InternalRow> deletes, long rowStartPosInBatch) {
    super(deletes, rowStartPosInBatch);
    this.readers = readers;
    this.vectorHolders = vectorHolders;
    initRowIdMapping(numRowsToRead);
    loadDataToColumnBatch(numRowsToRead);
  }

  public ColumnVector[] readDataToColumnVectors(int numRowsToRead) {
    ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];

    for (int i = 0; i < readers.length; i += 1) {
      vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
      int numRowsInVector = vectorHolders[i].numValues();
      Preconditions.checkState(
              numRowsInVector == numRowsToRead,
              "Number of rows in the vector %s didn't match expected %s ", numRowsInVector,
              numRowsToRead);

      arrowColumnVectors[i] = hasDeletes() ?
              ColumnVectorWithFilter.forHolder(vectorHolders[i], getRowIdMapping(), getNumRows()) :
              IcebergArrowColumnVector.forHolder(vectorHolders[i], numRowsInVector);
    }
    return arrowColumnVectors;
  }
}
