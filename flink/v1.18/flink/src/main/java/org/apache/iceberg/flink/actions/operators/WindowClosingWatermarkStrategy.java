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

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;

/** {@link WatermarkStrategy} for emitting a watermark 1ms after each record. */
public class WindowClosingWatermarkStrategy<T> implements WatermarkStrategy<Tuple2<Long, T>> {
  @Override
  public WatermarkGenerator<Tuple2<Long, T>> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {
    return new WatermarkGenerator<Tuple2<Long, T>>() {
      @Override
      public void onEvent(Tuple2<Long, T> event, long eventTimestamp, WatermarkOutput output) {
        output.emitWatermark(new Watermark(event.f0));
      }

      @Override
      public void onPeriodicEmit(WatermarkOutput output) {
        // No periodic watermarks
      }
    };
  }

  @Override
  public TimestampAssigner<Tuple2<Long, T>> createTimestampAssigner(
      TimestampAssignerSupplier.Context context) {
    return (element, unused) -> element.f0;
  }
}
