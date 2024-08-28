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

import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.DUMMY_NAME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestIncompatibleChangeBlocker extends OperatorTestBase {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBlockSchemaChange(boolean failOnChange) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    try (KeyedOneInputStreamOperatorTestHarness<Boolean, SerializableTable, SerializableTable>
        testHarness =
            harness(
                new IncompatibleChangeBlocker(
                    DUMMY_NAME,
                    (SerializableTable) SerializableTable.copyOf(table),
                    true,
                    false,
                    failOnChange))) {

      testHarness.open();
      // Should not fail on the same table
      testHarness.processElement(
          (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Should not fail on spec change
      table.updateSpec().addField("data").commit();
      table.refresh();

      testHarness.processElement(
          (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(2);

      // Should fail on schema change
      table.updateSchema().addColumn("newColumn", Types.IntegerType.get()).commit();

      table.refresh();

      if (failOnChange) {
        assertThatThrownBy(
                () ->
                    testHarness.processElement(
                        (SerializableTable) SerializableTable.copyOf(table),
                        System.currentTimeMillis()))
            .isInstanceOf(SuppressRestartsException.class)
            .hasMessage(
                "Unrecoverable failure. This suppresses job restarts. Please check the stack trace for the root cause.");
      } else {
        testHarness.processElement(
            (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis());
        assertThat(testHarness.extractOutputValues()).hasSize(2);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBlockSpecChange(boolean failOnChange) throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    try (KeyedOneInputStreamOperatorTestHarness<Boolean, SerializableTable, SerializableTable>
        testHarness =
            harness(
                new IncompatibleChangeBlocker(
                    DUMMY_NAME,
                    (SerializableTable) SerializableTable.copyOf(table),
                    false,
                    true,
                    failOnChange))) {
      testHarness.open();
      // Should not fail on the same table
      testHarness.processElement(
          (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Should not fail on schema change
      table.updateSchema().addColumn("newColumn", Types.IntegerType.get()).commit();
      table.refresh();

      testHarness.processElement(
          (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(2);

      // We should fail on spec change
      table.updateSpec().addField("data").commit();
      table.refresh();

      if (failOnChange) {
        assertThatThrownBy(
                () ->
                    testHarness.processElement(
                        (SerializableTable) SerializableTable.copyOf(table),
                        System.currentTimeMillis()))
            .isInstanceOf(SuppressRestartsException.class)
            .hasMessage(
                "Unrecoverable failure. This suppresses job restarts. Please check the stack trace for the root cause.");
      } else {
        testHarness.processElement(
            (SerializableTable) SerializableTable.copyOf(table), System.currentTimeMillis());
        assertThat(testHarness.extractOutputValues()).hasSize(2);
      }
    }
  }

  @Test
  void testBlockSpecChangeOnRestoreState() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    OperatorSubtaskState state;
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, SerializableTable, SerializableTable>
        testHarness =
            harness(
                new IncompatibleChangeBlocker(DUMMY_NAME, serializableTable, false, true, true))) {
      testHarness.open();
      state = testHarness.snapshot(1, System.currentTimeMillis());
    }

    // Restart with the same table should be successful
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, SerializableTable, SerializableTable>
        testHarness =
            harness(
                new IncompatibleChangeBlocker(DUMMY_NAME, serializableTable, false, true, true))) {
      testHarness.initializeState(state);
      testHarness.open();

      // Should not fail on the same table
      testHarness.processElement(serializableTable, System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      state = testHarness.snapshot(2, System.currentTimeMillis());
    }

    // Restart with a changed table should fail
    table.updateSpec().addField("data").commit();
    table.refresh();

    SerializableTable finalSerializableTable = (SerializableTable) SerializableTable.copyOf(table);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, SerializableTable, SerializableTable>
        testHarness =
            harness(
                new IncompatibleChangeBlocker(
                    DUMMY_NAME, finalSerializableTable, false, true, true))) {
      testHarness.initializeState(state);
      testHarness.open();

      // Should not fail on the same table
      assertThatThrownBy(
              () -> testHarness.processElement(finalSerializableTable, System.currentTimeMillis()))
          .isInstanceOf(SuppressRestartsException.class)
          .hasMessage(
              "Unrecoverable failure. This suppresses job restarts. Please check the stack trace for the root cause.");
    }
  }

  private KeyedOneInputStreamOperatorTestHarness<Boolean, SerializableTable, SerializableTable>
      harness(IncompatibleChangeBlocker blocker) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(blocker),
        unused -> true,
        org.apache.flink.api.common.typeinfo.Types.BOOLEAN);
  }
}
