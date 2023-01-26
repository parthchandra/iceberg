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
package org.apache.iceberg.spark.data.parquet.vectorized;

import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.vectorized.boson.BosonVectorizedSparkParquetReaders;
import org.junit.Ignore;
import org.junit.Test;

public class TestBosonParquetVectorizedReads extends TestParquetVectorizedReads {

  @Override
  void createBatchedReaderFunc(
      Parquet.ReadBuilder readBuilder, Schema schema, boolean setAndCheckArrowValidityBuffer) {
    readBuilder.createBatchedReaderFunc(
        type -> BosonVectorizedSparkParquetReaders.buildReader(schema, type));
  }

  @Override
  boolean checkArrowValidityBuffer(boolean arrowValidityBuffer) {
    return false;
  }

  @Override
  String containsMessage() {
    return "";
  }

  @Test
  @Override
  @Ignore // Ignored since this Boson doesn't support type promotion yet
  public void testReadsForTypePromotedColumns() throws Exception {}

  @Test
  @Override
  @Ignore // Ignored since this Boson doesn't support this yet
  public void testSupportedReadsForParquetV2() throws Exception {}
}
