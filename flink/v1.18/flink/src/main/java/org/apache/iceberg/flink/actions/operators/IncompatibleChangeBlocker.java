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

import java.util.Locale;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a way to block incompatible changes for the table, like schema changes which could cause
 * issues with compaction.
 */
public class IncompatibleChangeBlocker
    extends KeyedProcessFunction<Boolean, SerializableTable, SerializableTable> {
  private static final Logger LOG = LoggerFactory.getLogger(IncompatibleChangeBlocker.class);

  private final String name;
  private final int schemaId;
  private final int specId;
  private final boolean checkSchemaChange;
  private final boolean checkSpecChange;
  private final boolean failOnError;

  private final String tableName;
  private transient Counter errorCounter;
  private transient Counter incompatibleSchemaChangeCounter;
  private transient Counter incompatibleSpecChangeCounter;
  private transient ValueState<Integer> schemaIdState;
  private transient ValueState<Integer> specIdState;

  public IncompatibleChangeBlocker(
      String name,
      SerializableTable original,
      boolean checkSchemaChange,
      boolean checkSpecChange,
      boolean failOnError) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(original, "Table should no be null");

    this.name = name;
    this.checkSchemaChange = checkSchemaChange;
    this.checkSpecChange = checkSpecChange;
    this.failOnError = failOnError;
    this.schemaId = original.schema().schemaId();
    this.specId = original.spec().specId();
    this.tableName = original.name();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);
    this.incompatibleSchemaChangeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.INCOMPATIBLE_SCHEMA_CHANGE);
    this.incompatibleSpecChangeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.INCOMPATIBLE_SPEC_CHANGE);

    this.schemaIdState =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("incompatibleChangeBlockerSchemaId", Types.INT));
    this.specIdState =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>("incompatibleChangeBlockerSpecIdState", Types.INT));
  }

  @Override
  public void processElement(SerializableTable table, Context ctx, Collector<SerializableTable> out)
      throws Exception {
    if (checkSchemaChange) {
      long currentSchemaId = table.schema().schemaId();

      if (schemaIdState.value() == null) {
        schemaIdState.update(schemaId);
      }

      if (schemaIdState.value() != currentSchemaId) {
        incompatibleSchemaChangeCounter.inc();
        errorCounter.inc();
        Exception error =
            new RuntimeException(
                String.format(
                    Locale.ROOT,
                    "Incompatible schema change for %s between schema: %d and %d at %d",
                    tableName,
                    schemaIdState.value(),
                    currentSchemaId,
                    ctx.timestamp()));
        if (failOnError) {
          throw new SuppressRestartsException(error);
        } else {
          ctx.output(ErrorAggregator.ERROR_STREAM, error);
          return;
        }
      }
    }

    if (checkSpecChange) {
      long currentSpecId = table.spec().specId();

      if (specIdState.value() == null) {
        specIdState.update(specId);
      }

      if (specIdState.value() != currentSpecId) {
        LOG.warn(
            "Incompatible schema change for {} between schema: {} and {} at {}",
            tableName,
            specIdState.value(),
            currentSpecId,
            ctx.timestamp());

        incompatibleSpecChangeCounter.inc();
        errorCounter.inc();
        Exception error =
            new RuntimeException(
                String.format(
                    Locale.ROOT,
                    "Incompatible schema change for %s between schema: %d and %d at %d",
                    tableName,
                    specIdState.value(),
                    currentSpecId,
                    ctx.timestamp()));
        if (failOnError) {
          throw new SuppressRestartsException(error);
        } else {
          ctx.output(ErrorAggregator.ERROR_STREAM, error);
          return;
        }
      }
    }

    out.collect(table);
  }
}
