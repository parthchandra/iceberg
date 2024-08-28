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
package org.apache.iceberg.flink.actions.streams;

import static org.apache.iceberg.flink.actions.ActionTestUtils.closeJobClient;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.TableChange;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestE2e extends StreamTestBase {
  @TempDir private File checkpointDir;
  private StreamExecutionEnvironment env;

  @BeforeEach
  public void beforeEach() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  @Test
  void testE2e() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    DataStream<TableChange> changes =
        TableMonitor.builder(env, tableLoader)
            .monitorFrequency(Duration.ofSeconds(1))
            .maxReadBack(10)
            .build();

    TableMaintenance.builder(changes, tableLoader)
        .uidPrefix("E2eTestUID")
        .rateLimit(Duration.ofMinutes(10))
        .concurrentCheckDelay(Duration.ofSeconds(10))
        .clearRunLocks(false)
        .add(
            ExpireSnapshots.builder()
                .scheduleOnCommit(10)
                .minAge(Duration.ofMinutes(10))
                .retainLast(5)
                .parallelism(8))
        .add(
            RewriteDataFiles.builder()
                .scheduleOnFileNumber(3)
                .scheduleOnFileSize(10L * 1024L * 1024L)
                .schedulerOnDeleteFileNumber(5)
                .minInputFiles(2)
                .parallelism(8))
        .add(RewriteManifestFiles.builder().scheduleOnCommit(5).parallelism(1))
        .add(
            DeleteOrphanFiles.builder(env)
                .scheduleOnTime(Duration.ofDays(1))
                .minAge(Duration.ofMinutes(10)))
        .build();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      assertThat(jobClient).isNotNull();
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testE2eSimple() throws Exception {
    sql.exec(
        "CREATE TABLE %s (data varchar, part varchar, id int) PARTITIONED BY (part)", TABLE_NAME);
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    RowType rowType = FlinkSchemaUtil.convert(tableLoader.loadTable().schema());
    env.enableCheckpointing(30000);
    env.getCheckpointConfig().setCheckpointStorage("file://" + checkpointDir.getPath());

    DataStream<String> source =
        env.addSource(new DataGeneratorSource<>(RandomGenerator.stringGenerator(8), 1, null))
            .returns(String.class)
            .uid("datagen-source-uid")
            .name("datagen-source");

    FlinkSink.forRowData(source.process(new RowDataConverter(rowType)))
        .tableLoader(tableLoader)
        .uidPrefix("iceberg-sink")
        .append();

    TableMaintenance.builder(env, tableLoader)
        .uidPrefix("E2eTestUID")
        .rateLimit(Duration.ofMinutes(10))
        .parallelism(2)
        .add(ExpireSnapshots.builder().scheduleOnCommit(10))
        .add(RewriteDataFiles.builder().scheduleOnFileNumber(3))
        .add(RewriteManifestFiles.builder().scheduleOnCommit(5))
        .add(DeleteOrphanFiles.builder(env).scheduleOnTime(Duration.ofDays(1)))
        .build();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      assertThat(jobClient).isNotNull();
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testE2eSimple1Par() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    TableMaintenance.builder(env, tableLoader)
        .uidPrefix("E2eTestUID")
        .rateLimit(Duration.ofMinutes(10))
        .parallelism(1)
        .add(ExpireSnapshots.builder().scheduleOnCommit(10))
        .add(RewriteDataFiles.builder().scheduleOnFileNumber(3))
        .add(RewriteManifestFiles.builder().scheduleOnCommit(5))
        .add(DeleteOrphanFiles.builder(env).scheduleOnTime(Duration.ofDays(1)))
        .build();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      assertThat(jobClient).isNotNull();
    } finally {
      closeJobClient(jobClient);
    }
  }

  /**
   * Converter class for converting Strings to Iceberg RowData objects which are expected by the
   * Iceberg FlinkSink.
   */
  private static class RowDataConverter extends ProcessFunction<String, RowData>
      implements ResultTypeQueryable<RowData> {

    private final RowType rowType;
    private final Random random = new Random(System.currentTimeMillis());

    RowDataConverter(RowType rowType) {
      this.rowType = rowType;
    }

    @Override
    public void processElement(String record, Context ctx, Collector<RowData> out) {
      out.collect(
          GenericRowData.of(
              StringData.fromString(record),
              StringData.fromString(String.valueOf(record.charAt(3))),
              random.nextInt(10)));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
      return InternalTypeInfo.of(rowType);
    }
  }

  private static AvroToRowDataConverters.AvroToRowDataConverter createRowDataConverter(
      RowType rowType) {
    AvroToRowDataConverters.AvroToRowDataConverter[] fieldConverters =
        rowType.getFields().stream()
            .map(RowType.RowField::getType)
            .map(TestE2e::createAvroToRowDataConverter)
            .collect(Collectors.toList())
            .toArray(new AvroToRowDataConverters.AvroToRowDataConverter[] {});
    String[] fields = rowType.getFieldNames().toArray(new String[] {});
    int arity = rowType.getFieldCount();
    return (avroObject) -> {
      GenericRecord record = (GenericRecord) avroObject;
      GenericRowData row = new GenericRowData(arity);

      for (int i = 0; i < arity; ++i) {
        row.setField(i, fieldConverters[i].convert(record.get(fields[i])));
      }

      return row;
    };
  }

  private static AvroToRowDataConverters.AvroToRowDataConverter createAvroToRowDataConverter(
      LogicalType type) {
    if (type.getTypeRoot() == LogicalTypeRoot.ROW) {
      return createRowDataConverter((RowType) type);
    } else {
      try {
        AvroToRowDataConverters.AvroToRowDataConverter converter =
            createConverterOld != null
                ? (AvroToRowDataConverters.AvroToRowDataConverter)
                    createConverterOld.invoke(null, type)
                : (AvroToRowDataConverters.AvroToRowDataConverter)
                    createConverterNew.invoke(null, type, false);
        return avroObject -> avroObject == null ? null : converter.convert(avroObject);
      } catch (Exception var2) {
        throw new RuntimeException(var2);
      }
    }
  }

  private static Method createConverterOld = null;
  private static Method createConverterNew = null;

  static {
    try {
      createConverterOld =
          AvroToRowDataConverters.class.getDeclaredMethod("createConverter", LogicalType.class);
      createConverterOld.setAccessible(true);
    } catch (NoSuchMethodException var1) {
      try {
        createConverterNew =
            AvroToRowDataConverters.class.getDeclaredMethod(
                "createConverter", LogicalType.class, boolean.class);
        createConverterNew.setAccessible(true);
      } catch (NoSuchMethodException var2) {
        throw new RuntimeException(var1);
      }
    }
  }
}
