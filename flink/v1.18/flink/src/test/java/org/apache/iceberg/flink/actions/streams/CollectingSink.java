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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Sink for collecting output during testing. */
public class CollectingSink<T> implements Sink<T> {
  private static final long serialVersionUID = 1L;
  private static final List<BlockingQueue<Object>> QUEUES =
      Collections.synchronizedList(Lists.newArrayListWithExpectedSize(1));
  private static final AtomicInteger NUM_SINKS = new AtomicInteger(-1);
  private final int index;

  public CollectingSink() {
    this.index = NUM_SINKS.incrementAndGet();
    QUEUES.add(new LinkedBlockingQueue<>());
  }

  public List<T> getRemainingOutput() {
    return Lists.newArrayList((BlockingQueue<T>) QUEUES.get(this.index));
  }

  public boolean isEmpty() {
    return QUEUES.get(this.index).isEmpty();
  }

  public T poll() throws TimeoutException {
    return this.poll(Duration.ofSeconds(1500000L));
  }

  public T poll(Duration duration) throws TimeoutException {
    Object element;

    try {
      element = QUEUES.get(this.index).poll(duration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException var4) {
      throw new RuntimeException(var4);
    }

    if (element == null) {
      throw new TimeoutException();
    } else {
      return (T) element;
    }
  }

  @Override
  public SinkWriter<T> createWriter(InitContext context) {
    return new CollectingWriter<>(index);
  }

  private static class CollectingWriter<T> implements SinkWriter<T> {
    private final int index;

    CollectingWriter(int index) {
      this.index = index;
    }

    @Override
    public void write(T element, Context context) {
      QUEUES.get(index).add(element);
    }

    @Override
    public void flush(boolean endOfInput) {
      // Nothing to do here
    }

    @Override
    public void close() {
      // Nothing to do here
    }
  }
}
