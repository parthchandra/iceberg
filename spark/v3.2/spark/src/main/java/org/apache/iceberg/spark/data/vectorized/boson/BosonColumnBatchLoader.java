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

import com.apple.boson.parquet.BosonIcebergColumnReader;
import com.apple.boson.vector.BosonIcebergVector;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.spark.data.vectorized.BaseColumnBatchLoader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;

class BosonColumnBatchLoader extends BaseColumnBatchLoader {
  private final BosonIcebergColumnReader[] readers;

  BosonColumnBatchLoader(BosonIcebergColumnReader[] readers, int numRowsToRead,
                         DeleteFilter<InternalRow> deletes, long rowStartPosInBatch) {
    super(deletes, rowStartPosInBatch);
    this.readers = readers;
    initRowIdMapping(numRowsToRead);
    loadDataToColumnBatch(numRowsToRead);
  }

  public ColumnVector[] readDataToColumnVectors(int numRowsToRead) {
    ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];
    for (int i = 0; i < readers.length; i++) {
      readers[i].read(null, numRowsToRead);
      BosonIcebergVector bv = readers[i].getVector();
      bv.setRowIdMapping(getRowIdMapping());
      arrowColumnVectors[i] = bv;
    }
    return arrowColumnVectors;
  }
}
