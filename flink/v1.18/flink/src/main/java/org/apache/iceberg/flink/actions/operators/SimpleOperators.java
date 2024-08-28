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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SimpleOperators {
  private SimpleOperators() {
    // Do not instantiate
  }

  public static <K> DataStream<Tuple2<Long, K>> extractEventTime(
      DataStream<K> source, String uidPrefix, String name, String slotSharingGroup) {
    return source
        .process(new EventTimeDecorator<>())
        .name("Time Decorator")
        .uid(uidPrefix + "-time-decorator-for" + name)
        .setParallelism(source.getParallelism())
        .slotSharingGroup(slotSharingGroup);
  }

  public static <K> KeyedStream<Tuple2<Long, K>, Long> keyByEventTime(
      DataStream<K> source, String uidPrefix, String name, String slotSharingGroup) {
    return extractEventTime(source, uidPrefix, name, slotSharingGroup).keyBy(value -> value.f0);
  }

  private static class EventTimeDecorator<T> extends ProcessFunction<T, Tuple2<Long, T>> {
    @Override
    public void processElement(T value, Context ctx, Collector<Tuple2<Long, T>> out) {
      out.collect(Tuple2.of(ctx.timestamp(), value));
    }
  }

  public static class FullRecordKeySelector<T>
      implements KeySelector<Tuple2<Long, T>, Tuple2<Long, T>> {
    @Override
    public Tuple2<Long, T> getKey(Tuple2<Long, T> value) {
      return value;
    }
  }
}
