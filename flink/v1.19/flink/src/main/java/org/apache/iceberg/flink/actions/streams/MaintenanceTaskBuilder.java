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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.operators.ErrorAggregator;
import org.apache.iceberg.flink.actions.operators.RunResponse;
import org.apache.iceberg.flink.actions.operators.SimpleOperators;
import org.apache.iceberg.flink.actions.operators.StreamScheduler;
import org.apache.iceberg.flink.actions.operators.StreamSchedulerTrigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public abstract class MaintenanceTaskBuilder<T extends MaintenanceTaskBuilder> {
  private int id;
  private String name;
  private TableLoader tableLoader;
  private boolean initialized;
  private String uidPrefix = null;
  private String slotSharingGroup = null;
  private Integer parallelism = null;
  private transient StreamSchedulerTrigger.Builder triggerBuilder =
      new StreamSchedulerTrigger.Builder();

  abstract DataStream<RunResponse> buildInternal(DataStream<SerializableTable> sourceStream);

  /**
   * After a given number of Iceberg table commits since the last run, starts the downstream job.
   *
   * @param commitNumber after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnCommit(int commitNumber) {
    triggerBuilder.commitNumber(commitNumber);
    return (T) this;
  }

  /**
   * After a given number of new data files since the last run, starts the downstream job.
   *
   * @param fileNumber after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnFileNumber(int fileNumber) {
    triggerBuilder.fileNumber(fileNumber);
    return (T) this;
  }

  /**
   * After a given aggregated data file size since the last run, starts the downstream job.
   *
   * @param fileSize after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnFileSize(long fileSize) {
    triggerBuilder.fileSize(fileSize);
    return (T) this;
  }

  /**
   * After a given number of new delete files since the last run, starts the downstream job.
   *
   * @param deleteFileNumber after the downstream job should be started
   * @return for chained calls
   */
  public T schedulerOnDeleteFileNumber(int deleteFileNumber) {
    triggerBuilder.fileNumber(deleteFileNumber);
    return (T) this;
  }

  /**
   * After a given time since the last run, starts the downstream job.
   *
   * @param time after the downstream job should be started
   * @return for chained calls
   */
  public T scheduleOnTime(Duration time) {
    triggerBuilder.timeout(time);
    return (T) this;
  }

  /**
   * The prefix used for the generated {@link org.apache.flink.api.dag.Transformation}'s uid.
   *
   * @param newUidPrefix for the transformations
   * @return for chained calls
   */
  public T uidPrefix(String newUidPrefix) {
    this.uidPrefix = newUidPrefix;
    return (T) this;
  }

  /**
   * The {@link SingleOutputStreamOperator#slotSharingGroup(String)} for all the operators of the
   * generated stream. Could be used to separate the resources used by this task.
   *
   * @param newSlotSharingGroup to be used for the operators
   * @return for chained calls
   */
  public T slotSharingGroup(String newSlotSharingGroup) {
    this.slotSharingGroup = newSlotSharingGroup;
    return (T) this;
  }

  /**
   * Sets the parallelism for the stream.
   *
   * @param newParallelism the required parallelism
   * @return for chained calls
   */
  public T parallelism(int newParallelism) {
    this.parallelism = newParallelism;
    return (T) this;
  }

  T init(
      int newId,
      String newName,
      TableLoader newTableLoader,
      String mainUidPrefix,
      String mainSlotSharingGroup,
      int mainParallelism) {
    Preconditions.checkNotNull(newName, "Name should not be null");
    Preconditions.checkNotNull(newTableLoader, "TableLoader should not be null");

    this.id = newId;
    this.name = newName;
    this.tableLoader = newTableLoader;

    if (uidPrefix == null) {
      uidPrefix = mainUidPrefix + "_" + name + "_" + id;
    }

    if (parallelism == null) {
      parallelism = mainParallelism;
    }

    if (slotSharingGroup == null) {
      slotSharingGroup = mainSlotSharingGroup;
    }

    this.initialized = true;
    return (T) this;
  }

  int id() {
    return id;
  }

  String name() {
    return name;
  }

  TableLoader tableLoader() {
    return tableLoader;
  }

  String uidPrefix() {
    return uidPrefix;
  }

  String slotSharingGroup() {
    return slotSharingGroup;
  }

  Integer parallelism() {
    return parallelism;
  }

  <K> StreamScheduler<K> scheduler() {
    return new StreamScheduler<>(triggerBuilder.build(), id, name);
  }

  DataStream<RunResponse> build(DataStream<SerializableTable> sourceStream) {
    Preconditions.checkArgument(
        parallelism == null || parallelism == -1 || parallelism > 0,
        "Parallelism should be left to default (-1/null) or greater than 0");
    Preconditions.checkArgument(initialized, "Builder should be initialized");

    tableLoader.open();

    return buildInternal(sourceStream);
  }

  <K> DataStream<Tuple2<Long, Exception>> prepareError(
      SingleOutputStreamOperator<K> stream, String operatorName) {
    return SimpleOperators.extractEventTime(
        stream.getSideOutput(ErrorAggregator.ERROR_STREAM),
        uidPrefix(),
        operatorName,
        slotSharingGroup());
  }

  <K> KeyedStream<Tuple2<Long, K>, Long> withEventTime(DataStream<K> stream, String operatorName) {
    return SimpleOperators.keyByEventTime(stream, uidPrefix(), operatorName, slotSharingGroup());
  }

  <K> KeyedStream<Tuple2<Long, Long>, Long> extractTimes(
      DataStream<K> source, String operatorName) {
    return source
        .process(new ProcessingTimeExtractor<>())
        .name("Time Extractor")
        .uid(uidPrefix() + "-time-extractor-for" + operatorName)
        .setParallelism(source.getParallelism())
        .slotSharingGroup(slotSharingGroup())
        .keyBy(value -> value.f0);
  }

  private static class ProcessingTimeExtractor<K> extends ProcessFunction<K, Tuple2<Long, Long>> {
    @Override
    public void processElement(
        K value,
        ProcessFunction<K, Tuple2<Long, Long>>.Context ctx,
        Collector<Tuple2<Long, Long>> out) {
      out.collect(Tuple2.of(ctx.timestamp(), ctx.timerService().currentProcessingTime()));
    }
  }
}
