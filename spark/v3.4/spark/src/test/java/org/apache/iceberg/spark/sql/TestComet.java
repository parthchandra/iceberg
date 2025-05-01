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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.comet.CometConf;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.ParquetReaderType;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.SparkException;
import org.junit.Test;

public class TestComet extends SparkTestBaseWithCatalog {

  @Test
  public void testSchemaEvolution() {
    String table = tableName("test");
    sql("CREATE TABLE %s (id Int) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1)", table);
    boolean[] cometEnabled = new boolean[] {false, true};
    ParquetReaderType[] readerTypes =
        new ParquetReaderType[] {ParquetReaderType.ICEBERG, ParquetReaderType.COMET};
    for (boolean enabled : cometEnabled) {
      for (ParquetReaderType readerType : readerTypes) {
        spark.conf().set(CometConf.COMET_ENABLED().key(), enabled);
        spark.conf().set(SparkSQLProperties.PARQUET_READER_TYPE.toString(), readerType.name());
        sql("alter table %s alter column id type bigint", table);
        if (enabled && readerType == ParquetReaderType.COMET) {
          assertThatThrownBy(() -> sql("SELECT * FROM %s", table))
              .isInstanceOf(SparkException.class)
              .hasMessageContaining("column: [id], physicalType: INT32, logicalType: bigint");
        } else {
          List<Object[]> results = sql("SELECT * FROM %s", table);
          List<Object[]> expected = ImmutableList.of(row(1L));
          assertEquals("Should return correct values", expected, results);
        }
      }
    }

    sql("DROP TABLE IF EXISTS %s", table);
  }
}
