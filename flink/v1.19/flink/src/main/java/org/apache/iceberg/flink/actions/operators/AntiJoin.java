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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Receives {@link Tuple2}s with the event time, and the file name from the table file listing, and
 * similar tuples from the file system file the listing.
 *
 * <p>Emits every file name where the file is present in file system, but not present in the table.
 *
 * <p>Windowing is used to emit the records.
 *
 * <ul>
 *   <li>The input records should have the same event time.
 *   <li>{@link org.apache.flink.streaming.api.watermark.Watermark} should be used to close the
 *       window.
 * </ul>
 */
public class AntiJoin
    extends KeyedCoProcessFunction<
        Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
  private transient ValueState<Boolean> foundInTable;
  private transient ValueState<Boolean> foundInFileSystem;

  @Override
  public void open(Configuration config) throws Exception {
    foundInTable =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("antiJoinFoundInTable", Types.BOOLEAN));
    foundInFileSystem =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("antiJoinFoundInFileSystem", Types.BOOLEAN));
  }

  @Override
  public void processElement1(
      Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, String>> out)
      throws Exception {
    ctx.timerService().registerEventTimeTimer(value.f0);
    foundInTable.update(true);
  }

  @Override
  public void processElement2(
      Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, String>> out)
      throws Exception {
    ctx.timerService().registerEventTimeTimer(value.f0);
    foundInFileSystem.update(true);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, String>> out)
      throws Exception {
    if (foundInFileSystem.value() != null && foundInTable.value() == null) {
      out.collect(ctx.getCurrentKey());
    }

    foundInTable.clear();
    foundInFileSystem.clear();
  }
}
