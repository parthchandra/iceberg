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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestIcebergSinkWriter extends TestFlinkIcebergSinkBase {

  @BeforeEach
  public void before() throws IOException {
    this.table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned());

    this.tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @Test
  void testCheckFilesEnabled() throws Exception {
    testCheckFiles(true);
  }

  @Test
  void testCheckFilesDisabled() throws Exception {
    testCheckFiles(false);
  }

  void testCheckFiles(boolean enabled) throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .uid("mySourceId");

    IcebergSink.Builder builder =
        IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
            .writeParallelism(1)
            .checkFiles(enabled);

    IcebergSink sink = builder.build();
    WriterInitContext context = Mockito.mock(WriterInitContext.class);
    Mockito.when(context.getTaskInfo()).thenReturn(Mockito.mock(TaskInfo.class));
    Mockito.when(context.metricGroup())
        .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());

    IcebergSinkWriter writer = (IcebergSinkWriter) sink.createWriter(context);

    writer.write(GenericRowData.of(1, StringData.fromString("test")), null);
    assertThat(writer.getMetrics().getSucceededFileChecks().getCount()).isEqualTo(0);

    writer.prepareCommit();
    if (enabled) {
      assertThat(writer.getMetrics().getSucceededFileChecks().getCount()).isEqualTo(1);
    } else {
      assertThat(writer.getMetrics().getSucceededFileChecks().getCount()).isEqualTo(0);
    }
  }
}
