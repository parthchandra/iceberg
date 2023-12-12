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

import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.junit.Assert;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runners.Parameterized;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkIcebergSinkV2WithFileChecker extends TestFlinkIcebergSinkV2 {
  @Parameterized.Parameters(
      name = "FileFormat = {0}, Parallelism = {1}, Partitioned={2}, WriteDistributionMode ={3}")
  public static Object[][] parameters() {
    return TestFlinkIcebergSinkV2.parameters();
  }

  protected void testChangeLogs(
      List<String> equalityFieldColumns,
      KeySelector<Row, Object> keySelector,
      boolean insertAsUpsert,
      List<List<Row>> elementsPerCheckpoint,
      List<List<Record>> expectedRecordsPerCheckpoint,
      String branch)
      throws Exception {
    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(elementsPerCheckpoint), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(tableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .equalityFieldColumns(equalityFieldColumns)
        .upsert(insertAsUpsert)
        .toBranch(branch)
        .checkFiles(true)
        .append();

    // Execute the program.
    env.execute("Test Iceberg Change-Log DataStream.");

    table.refresh();
    List<Snapshot> snapshots = findValidSnapshots();
    int expectedSnapshotNum = expectedRecordsPerCheckpoint.size();
    Assert.assertEquals(
        "Should have the expected snapshot number", expectedSnapshotNum, snapshots.size());

    for (int i = 0; i < expectedSnapshotNum; i++) {
      long snapshotId = snapshots.get(i).snapshotId();
      List<Record> expectedRecords = expectedRecordsPerCheckpoint.get(i);
      Assert.assertEquals(
          "Should have the expected records for the checkpoint#" + i,
          expectedRowSet(expectedRecords.toArray(new Record[0])),
          actualRowSet(snapshotId, "*"));
    }
  }
}
