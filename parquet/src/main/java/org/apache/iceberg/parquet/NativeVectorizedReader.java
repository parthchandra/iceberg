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
package org.apache.iceberg.parquet;

/**
 * Interface for native vectorized readers that need initialization with NativeReadConf.
 *
 * <p>This interface allows readers to be initialized with configuration after construction, which
 * is useful for native readers that need access to file metadata and other configuration details
 * including native-specific parameters like encryption keys.
 */
public interface NativeVectorizedReader<T> extends VectorizedReader<T> {

  /**
   * Initialize the reader with native configuration.
   *
   * @param conf the native read configuration containing file metadata, schema, and native-specific
   *     settings
   * @param start the start offset in this read conf to initialize the reader
   * @param length the length of the data to read from this read conf
   */
  void init(NativeReadConf<?> conf, long start, long length);

  /** Reset this vectorized reader. To reuse, the init method has to be called again. */
  void reset();
}
