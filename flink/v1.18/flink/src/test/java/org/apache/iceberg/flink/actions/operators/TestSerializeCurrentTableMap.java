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
package org.apache.iceberg.flink.actions.operators;

import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;

class TestSerializeCurrentTableMap extends OperatorTestBase {
  static final long EVENT_TIME = 10L;

  @Test
  void testRefresh() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<SerializableTable> actual;
    long snapshotId = table.currentSnapshot().snapshotId();
    try (OneInputStreamOperatorTestHarness<Long, SerializableTable> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new StreamMap<>(new SerializeCurrentTableMap<>(tableLoader)))) {
      testHarness.open();

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processElement(EVENT_TIME, EVENT_TIME);
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).currentSnapshot().snapshotId()).isEqualTo(snapshotId);

      // Check if the result is not changed if there is no new data
      testHarness.processElement(EVENT_TIME, EVENT_TIME);
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(2);
      assertThat(actual.get(1).currentSnapshot().snapshotId()).isEqualTo(snapshotId);

      // Insert some more data, and check if the result has changed
      sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);
      table.refresh();
      snapshotId = table.currentSnapshot().snapshotId();
      assertThat(testHarness.extractOutputValues()).hasSize(2);

      testHarness.processElement(EVENT_TIME, EVENT_TIME);
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(3);
      assertThat(actual.get(2).currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    }
  }
}
