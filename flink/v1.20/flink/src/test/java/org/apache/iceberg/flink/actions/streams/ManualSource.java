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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Source implementation for Flink sources which can be triggered manually. */
public class ManualSource<T> implements SourceFunction<T>, ResultTypeQueryable<T> {

  private static final long serialVersionUID = 1L;
  private static final List<BlockingQueue<Tuple2<?, Long>>> QUEUES =
      Collections.synchronizedList(Lists.newArrayListWithExpectedSize(1));
  private static final CountDownLatch AT_LEAST_ONE_STARTED = new CountDownLatch(1);
  private static int numSources = 0;
  private final TypeInformation<T> type;
  private final int index;
  private transient DataStream<T> stream;
  private final transient StreamExecutionEnvironment env;
  private final transient BlockingQueue<Tuple2<?, Long>> currentQueue;
  private volatile boolean isRunning = false;

  public ManualSource(StreamExecutionEnvironment env, TypeInformation<T> type) {
    this.type = type;
    this.env = env;
    this.index = numSources++;
    this.currentQueue = new LinkedBlockingQueue<>();
    QUEUES.add(this.currentQueue);
  }

  public void sendRecord(T event) {
    this.sendInternal(Tuple2.of(event, null));
  }

  public void sendRecord(T event, long ts) {
    this.sendInternal(Tuple2.of(event, ts));
  }

  public void sendWatermark(long timeStamp) {
    this.sendInternal(Tuple2.of(null, timeStamp));
  }

  public void markFinished() {
    this.sendWatermark(Long.MAX_VALUE);
    this.sendInternal(Tuple2.of(null, null));
  }

  private void sendInternal(Tuple2<?, Long> tuple) {
    try {
      AT_LEAST_ONE_STARTED.await(1L, TimeUnit.MINUTES);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }

    this.currentQueue.offer(tuple);
  }

  public void cancel() {
    this.isRunning = false;
  }

  public void run(SourceContext<T> ctx) throws Exception {
    AT_LEAST_ONE_STARTED.countDown();
    BlockingQueue<Tuple2<?, Long>> queue = QUEUES.get(this.index);
    this.isRunning = true;

    while (this.isRunning) {
      Tuple2<?, Long> tuple = queue.poll(10, TimeUnit.MILLISECONDS);
      if (tuple != null) {
        synchronized (ctx.getCheckpointLock()) {
          if (tuple.f0 != null) {
            if (tuple.f1 != null) {
              ctx.collectWithTimestamp((T) tuple.f0, tuple.f1);
            } else {
              ctx.collect((T) tuple.f0);
            }
          } else {
            if (tuple.f1 == null) {
              this.isRunning = false;
              break;
            }

            ctx.emitWatermark(new Watermark(tuple.f1));
          }
        }
      }
    }
  }

  public TypeInformation<T> getProducedType() {
    return this.type;
  }

  public DataStream<T> getDataStream() {
    if (this.stream == null) {
      this.stream = this.env.addSource(this);
    }

    return this.stream;
  }
}
