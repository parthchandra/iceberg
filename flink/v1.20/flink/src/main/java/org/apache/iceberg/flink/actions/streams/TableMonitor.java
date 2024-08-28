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

import java.time.Duration;
import java.util.UUID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.TableChange;
import org.apache.iceberg.flink.actions.operators.TableChangeSource;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class TableMonitor {
  private TableMonitor() {
    // Do not instantiate directly
  }

  /**
   * Creates the default monitor source for the table changes.
   *
   * @param env used to register the monitor source
   * @param tableLoader used for accessing the table
   * @return builder for the monitor stream
   */
  public static Builder builder(StreamExecutionEnvironment env, TableLoader tableLoader) {
    Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
    Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

    return new Builder(env, tableLoader);
  }

  public static class Builder {
    private final StreamExecutionEnvironment env;
    private final TableLoader tableLoader;
    private Duration monitorFrequency = Duration.ofMinutes(1);
    private String name = null;
    private long maxReadBack = 100L;
    private String uidPrefix = "TableMonitor-" + UUID.randomUUID();
    private String slotSharingGroup = StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;

    private Builder(StreamExecutionEnvironment env, TableLoader tableLoader) {
      this.env = env;
      this.tableLoader = tableLoader;
    }

    /**
     * The frequency for the scheduler to check the table for new commits.
     *
     * @param newMonitorFrequency to schedule the checks
     * @return for chained calls
     */
    public Builder monitorFrequency(Duration newMonitorFrequency) {
      this.monitorFrequency = newMonitorFrequency;
      return this;
    }

    /**
     * The maximum number of snapshots to read back to collect the recent changes. Useful for
     * previously not maintained tables with too many checkpoints.
     *
     * @param newMaxReadBack the number of checkpoints to read
     * @return for chained calls
     */
    public Builder maxReadBack(long newMaxReadBack) {
      this.maxReadBack = newMaxReadBack;
      return this;
    }

    /**
     * The prefix used for the generated {@link org.apache.flink.api.dag.Transformation}'s uid.
     *
     * @param newUidPrefix for the transformations
     * @return for chained calls
     */
    public Builder uidPrefix(String newUidPrefix) {
      this.uidPrefix = newUidPrefix;
      return this;
    }

    /**
     * The {@link SingleOutputStreamOperator#slotSharingGroup(String)} for all the operators of the
     * generated stream. Could be used to separate the resources used by this task.
     *
     * @param newSlotSharingGroup to be used for the operators
     * @return for chained calls
     */
    public Builder slotSharingGroup(String newSlotSharingGroup) {
      this.slotSharingGroup = newSlotSharingGroup;
      return this;
    }

    /**
     * The name of the source generated.
     *
     * @param newName the source name
     * @return for chained calls
     */
    public Builder name(String newName) {
      this.name = newName;
      return this;
    }

    public DataStream<TableChange> build() {
      if (name == null) {
        tableLoader.open();
        name = "Monitor for " + tableLoader.loadTable().name();
      }

      return env.fromSource(
              new TableChangeSource(
                  tableLoader,
                  RateLimiterStrategy.perSecond(1.0 / monitorFrequency.getSeconds()),
                  maxReadBack),
              WatermarkStrategy.noWatermarks(),
              name)
          .uid(uidPrefix)
          .slotSharingGroup(slotSharingGroup)
          .forceNonParallel();
    }
  }
}
