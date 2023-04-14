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
import org.apache.iceberg.types.Types;

public class BosonConstantColumnReader<T> extends BosonColumnReader {
  private final T value;

  public BosonConstantColumnReader(T value, Types.NestedField field) {
    super(field);
    this.value = value;
    // FIXME: infer the following boolean flag ((from 'spark.boson.use.decimal128') once native
    // execution is enabled for Iceberg
    delegate = new ConstantColumnReader(getSparkType(), getDescriptor(), value, false);
  }

  @Override
  public void setBatchSize(int batchSize) {
    delegate.setBatchSize(batchSize);
    this.batchSize = batchSize;
    initialized = true;
  }
}
