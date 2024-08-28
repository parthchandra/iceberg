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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.flink.actions.operators.WindowClosingWatermarkStrategy;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ScheduledInfraExtension implements BeforeEachCallback {
  public static final String IGNORE = "Ignore";

  private StreamExecutionEnvironment env;
  private ManualSource<SerializableTable> source;
  private DataStream<SerializableTable> triggerStream;
  private CollectingSink<RunResponse> sink;

  @Override
  public void beforeEach(ExtensionContext context) {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    source = source(env);
    triggerStream = triggerStream(source);
    sink = newSink();
  }

  public StreamExecutionEnvironment env() {
    return env;
  }

  public ManualSource<SerializableTable> source() {
    return source;
  }

  public DataStream<SerializableTable> triggerStream() {
    return triggerStream;
  }

  public CollectingSink<RunResponse> sink() {
    return sink;
  }

  public ManualSource<SerializableTable> source(StreamExecutionEnvironment otherEnv) {
    return new ManualSource<>(otherEnv, TypeInformation.of(SerializableTable.class));
  }

  public DataStream<SerializableTable> triggerStream(ManualSource<SerializableTable> otherSource) {
    return otherSource
        .getDataStream()
        .map(new SerializableTableRateLimiter())
        .name(IGNORE)
        .forceNonParallel()
        .assignTimestampsAndWatermarks(new WindowClosingWatermarkStrategy<>())
        .name(IGNORE)
        .forceNonParallel()
        .map(value -> value.f1)
        .name(IGNORE)
        .forceNonParallel();
  }

  public CollectingSink<RunResponse> newSink() {
    return new CollectingSink<>();
  }

  private static class SerializableTableRateLimiter
      implements MapFunction<SerializableTable, Tuple2<Long, SerializableTable>> {
    private final long lastTrigger = System.currentTimeMillis();

    @Override
    public Tuple2<Long, SerializableTable> map(SerializableTable value) throws Exception {
      long current = System.currentTimeMillis();
      if (current < lastTrigger + 1) {
        Thread.sleep(lastTrigger + 1 - current);
      }

      return Tuple2.of(System.currentTimeMillis(), value);
    }
  }
}
