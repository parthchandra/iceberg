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
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.jupiter.api.extension.RegisterExtension;

class ScheduledBuilderTestBase extends StreamTestBase {
  @RegisterExtension ScheduledInfraExtension infra = new ScheduledInfraExtension();

  /**
   * Expects a table with at least 2 snapshots. Creates a {@link SerializableTable} from the last
   * but one snapshot, then invalidates it. After this feeds the {@link ManualSource} the invalid
   * table, and expects to have an error.
   *
   * @param env the Flink environment used to start the job
   * @param triggerSource used to trigger the maintenance task run
   * @param collectingSink used for getting the task results
   * @param table used for testing
   */
  void runAndWaitForFailure(
      StreamExecutionEnvironment env,
      ManualSource<SerializableTable> triggerSource,
      CollectingSink<RunResponse> collectingSink,
      Table table)
      throws Exception {
    table.refresh();
    Preconditions.checkArgument(
        table instanceof HasTableOperations, "Table needs to implement HasTableOperations");
    List<TableMetadata.MetadataLogEntry> history =
        ((HasTableOperations) table).operations().current().previousFiles();
    Preconditions.checkArgument(history.size() > 1, "Needs at least 2 history items");
    StaticTableOperations operations =
        new StaticTableOperations(history.get(history.size() - 1).file(), table.io());
    SerializableTable oldTable =
        (SerializableTable) SerializableTable.copyOf(new BaseTable(operations, table.name()));
    Preconditions.checkArgument(
        oldTable.currentSnapshot().snapshotId() != table.currentSnapshot().snapshotId(),
        "Error calculating old snapshot");

    // expire old snapshot
    table.expireSnapshots().expireSnapshotId(oldTable.currentSnapshot().snapshotId()).commit();

    table.refresh();
    Preconditions.checkArgument(
        table.snapshot(oldTable.currentSnapshot().snapshotId()) == null,
        "Error removing old snapshot");

    runAndWaitForResult(env, triggerSource, collectingSink, null, oldTable, false);
  }

  void runAndWaitForSuccess(
      StreamExecutionEnvironment env,
      ManualSource<SerializableTable> triggerSource,
      CollectingSink<RunResponse> collectingSink,
      Table table)
      throws Exception {
    table.refresh();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    runAndWaitForResult(env, triggerSource, collectingSink, null, serializableTable, true);
    table.refresh();
  }

  Configuration runAndWaitForSavepoint(
      StreamExecutionEnvironment env,
      ManualSource<SerializableTable> triggerSource,
      CollectingSink<RunResponse> collectingSink,
      File savepointDir,
      Table table)
      throws Exception {
    table.refresh();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    Configuration configuration =
        runAndWaitForResult(
            env, triggerSource, collectingSink, savepointDir, serializableTable, true);

    table.refresh();
    return configuration;
  }

  Configuration runAndWaitForResult(
      StreamExecutionEnvironment env,
      ManualSource<SerializableTable> triggerSource,
      CollectingSink<RunResponse> collectingSink,
      File savepointDir,
      SerializableTable table,
      boolean expectedResult)
      throws Exception {
    JobClient jobClient = null;
    Configuration configuration;
    try {
      jobClient = env.executeAsync();

      // Do a single task run
      triggerSource.sendRecord(table, EVENT_TIME);

      RunResponse result = collectingSink.poll(Duration.ofSeconds(5));

      assertThat(result.success()).isEqualTo(expectedResult);
    } finally {
      configuration = closeJobClient(jobClient, savepointDir);
    }

    return configuration;
  }

  void runAndWaitForJobFailure(
      StreamExecutionEnvironment env, ManualSource<SerializableTable> triggerSource, Table table)
      throws Exception {
    table.refresh();
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Do a single task run
      triggerSource.sendRecord(serializableTable, EVENT_TIME);

      JobClient finalJobClient = jobClient;
      assertThatThrownBy(() -> finalJobClient.getJobExecutionResult().get())
          .isInstanceOf(ExecutionException.class)
          .hasMessageContaining("Job execution failed");
    } finally {
      closeJobClient(jobClient);
    }
  }
}
