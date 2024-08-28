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
package org.apache.iceberg.flink.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.awaitility.Awaitility;

public class ActionTestUtils {
  public static final Schema SCHEMA =
      new Schema(
          optional(1, "id", org.apache.iceberg.types.Types.IntegerType.get()),
          optional(2, "data", org.apache.iceberg.types.Types.StringType.get()),
          optional(3, "spec", org.apache.iceberg.types.Types.StringType.get()));

  private ActionTestUtils() {
    // Do not instantiate
  }

  public static RowData row(int id, String data, String spec) {
    return GenericRowData.of(id, StringData.fromString(data), StringData.fromString(spec));
  }

  public static Record record(int id, String data, String spec) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("spec", spec);
    return record;
  }

  /**
   * Close the {@link JobClient} and wait for the job closure. If the savepointDir is specified, it
   * stops the job with a savepoint.
   *
   * @param jobClient the job to close
   * @param savepointDir the savepointDir to store the last savepoint. If <code>null</code> then
   *     stop without a savepoint.
   * @return configuration for restarting the job from the savepoint
   */
  public static Configuration closeJobClient(JobClient jobClient, File savepointDir) {
    Configuration conf = new Configuration();
    if (jobClient != null) {
      if (savepointDir != null) {
        // Stop with savepoint
        jobClient.stopWithSavepoint(false, savepointDir.getPath(), SavepointFormatType.CANONICAL);
        // Wait until the savepoint is created and the job has been stopped
        Awaitility.await().until(() -> savepointDir.listFiles(File::isDirectory).length == 1);
        conf.set(
            SavepointConfigOptions.SAVEPOINT_PATH,
            savepointDir.listFiles(File::isDirectory)[0].getAbsolutePath());
      } else {
        jobClient.cancel();
      }

      // Wait until the job has been stopped
      Awaitility.await().until(() -> jobClient.getJobStatus().get().isTerminalState());
      return conf;
    }

    return null;
  }

  /**
   * Close the {@link JobClient} and wait for the job closure.
   *
   * @param jobClient the job to close
   */
  public static void closeJobClient(JobClient jobClient) {
    closeJobClient(jobClient, null);
  }
}
