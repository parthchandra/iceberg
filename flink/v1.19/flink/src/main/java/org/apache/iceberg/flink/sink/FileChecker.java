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
package org.apache.iceberg.flink.sink;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileChecker implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(FileChecker.class);

  private final FileIO io;
  private final FileFormat format;
  private final Schema dataSchema;
  private final Schema posDeleteSchema;
  private final Schema eqDeleteSchema;

  FileChecker(
      FileIO io,
      FileFormat format,
      Schema dataSchema,
      Set<Integer> equalityFieldIds,
      boolean upsert) {
    this.io = io;
    this.format = format;
    this.dataSchema = dataSchema;
    this.posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      this.eqDeleteSchema = null;
    } else if (upsert) {
      this.eqDeleteSchema = TypeUtil.select(dataSchema, Sets.newHashSet(equalityFieldIds));
    } else {
      this.eqDeleteSchema = dataSchema;
    }
  }

  void check(WriteResult result, IcebergStreamWriterMetrics metrics) {
    Arrays.stream(result.dataFiles()).forEach(f -> check(f, metrics));
    Arrays.stream(result.deleteFiles()).forEach(f -> check(f, metrics));
  }

  private void check(ContentFile<?> contentFile, IcebergStreamWriterMetrics metrics) {
    Schema schema = null;
    switch (contentFile.content()) {
      case DATA:
        schema = dataSchema;
        break;
      case POSITION_DELETES:
        schema = posDeleteSchema;
        break;
      case EQUALITY_DELETES:
        schema = eqDeleteSchema;
        break;
    }

    InputFile file = io.newInputFile(contentFile.path().toString());
    CloseableIterable<Object> iterable = null;
    switch (format) {
      case AVRO:
        iterable = Avro.read(file).project(schema).build();
        break;
      case ORC:
        Schema finalSchema = schema;
        iterable =
            ORC.read(file)
                .project(schema)
                .createReaderFunc(
                    fileSchema -> GenericOrcReader.buildReader(finalSchema, fileSchema))
                .build();
        break;
      case PARQUET:
        iterable = Parquet.read(file).project(schema).callInit().build();
        break;
      case METADATA:
        throw new IllegalArgumentException("METADATA file content could not be checked");
    }

    try (CloseableIterable<Object> toClose = iterable) {
      LOG.trace("Checking file: {}", contentFile.path());
      for (@SuppressWarnings("unused") Object o : toClose) {
        // just read through the files
      }
      metrics.increaseFilesChecked();
    } catch (Exception e) {
      metrics.increaseFailedFileChecks();
      LOG.warn("Error checking file: {}", contentFile.path(), e);
      throw new RuntimeException("Error checking file", e);
    }
  }
}
