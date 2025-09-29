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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.comet.CometConf;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.ParquetReaderType;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestComet extends TestBaseWithCatalog {

  @TestTemplate
  public void testRequireExplicitCometConfiguration() {
    String tableName = tableName("test");
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    Table table = validationCatalog.loadTable(TableIdentifier.of("default", "test"));
    SparkReadConf readConf;

    // Test 1: Default configuration should not enable Comet (ParquetReaderType defaults to ICEBERG)
    readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    assertThat(SparkUtil.cometEnabled(spark, readConf))
        .as("Comet should not be enabled by default")
        .isFalse();
    assertThat(readConf.parquetReaderType())
        .as("Default ParquetReaderType should be ICEBERG")
        .isEqualTo(ParquetReaderType.ICEBERG);

    // Test 2: COMET_ENABLED=true alone is not sufficient (ParquetReaderType must also be COMET)
    spark.conf().set(CometConf.COMET_ENABLED().key(), "true");
    readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    assertThat(SparkUtil.cometEnabled(spark, readConf))
        .as("Comet should not be enabled with only COMET_ENABLED=true")
        .isFalse();
    assertThat(readConf.parquetReaderType())
        .as("ParquetReaderType should remain ICEBERG when not explicitly set")
        .isEqualTo(ParquetReaderType.ICEBERG);

    // Test 3: ParquetReaderType.COMET alone is not sufficient (COMET_ENABLED must also be true)
    spark.conf().set(CometConf.COMET_ENABLED().key(), "false");
    spark.conf().set(SparkSQLProperties.PARQUET_READER_TYPE, ParquetReaderType.COMET.name());
    readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    assertThat(SparkUtil.cometEnabled(spark, readConf))
        .as("Comet should not be enabled with only ParquetReaderType=COMET")
        .isFalse();
    assertThat(readConf.parquetReaderType())
        .as("ParquetReaderType should be COMET when explicitly set")
        .isEqualTo(ParquetReaderType.COMET);

    // Test 4: Both COMET_ENABLED=true AND ParquetReaderType=COMET are required
    spark.conf().set(CometConf.COMET_ENABLED().key(), "true");
    spark.conf().set(SparkSQLProperties.PARQUET_READER_TYPE, ParquetReaderType.COMET.name());
    readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    assertThat(SparkUtil.cometEnabled(spark, readConf))
        .as("Comet should be enabled when both COMET_ENABLED=true and ParquetReaderType=COMET")
        .isTrue();
    assertThat(readConf.parquetReaderType())
        .as("ParquetReaderType should be COMET")
        .isEqualTo(ParquetReaderType.COMET);

    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
