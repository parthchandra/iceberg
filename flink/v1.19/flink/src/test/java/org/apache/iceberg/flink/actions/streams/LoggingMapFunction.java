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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMapFunction<I, O> implements MapFunction<I, O>, ResultTypeQueryable<O> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LoggingMapFunction.class);
  private static final List<BlockingQueue<Object>> QUEUES =
      Collections.synchronizedList(Lists.newArrayListWithExpectedSize(1));
  private static final AtomicInteger NUM_MAPS = new AtomicInteger(-1);
  private static final Map<Integer, List<Integer>> CALL_ORDER = Maps.newConcurrentMap();
  private final int index;
  private final Function<I, O> function;
  private final TypeInformation<O> type;

  public LoggingMapFunction(Function<I, O> function, TypeInformation<O> type) {
    this.index = NUM_MAPS.incrementAndGet();
    this.function = function;
    this.type = type;
    QUEUES.add(new LinkedBlockingQueue<>());
    CALL_ORDER.put(index, Collections.synchronizedList(Lists.newArrayList()));
  }

  public LoggingMapFunction(LoggingMapFunction<I, O> sharedWith) {
    this.index = NUM_MAPS.incrementAndGet();
    this.function = sharedWith.function;
    this.type = sharedWith.type;
    QUEUES.add(QUEUES.get(sharedWith.index));
    CALL_ORDER.put(index, CALL_ORDER.get(sharedWith.index));
  }

  public List<I> remainingOutput() {
    return Lists.newArrayList((BlockingQueue<I>) QUEUES.get(index));
  }

  public List<Integer> callOrder() {
    return CALL_ORDER.get(index);
  }

  public boolean isEmpty() {
    return QUEUES.get(index).isEmpty();
  }

  public I poll() throws TimeoutException {
    return this.poll(Duration.ofSeconds(1500000L));
  }

  public I poll(Duration duration) throws TimeoutException {
    Object element;

    try {
      element = QUEUES.get(this.index).poll(duration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException var4) {
      throw new RuntimeException(var4);
    }

    if (element == null) {
      throw new TimeoutException();
    } else {
      return (I) element;
    }
  }

  @Override
  public O map(I value) throws Exception {
    LOG.debug("Mapper received {} {}", value, index);
    CALL_ORDER.get(index).add(index);
    QUEUES.get(index).add(value);

    return function.apply(value);
  }

  @Override
  public TypeInformation<O> getProducedType() {
    return type;
  }
}
