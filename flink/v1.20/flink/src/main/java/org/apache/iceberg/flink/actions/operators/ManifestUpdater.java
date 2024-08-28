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

import java.util.Iterator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits the manifest changes using {@link RewriteManifests}. The input1 is a {@link Tuple2} of:
 *
 * <ul>
 *   <li>Event time
 *   <li>{@link ManifestSource} to differentiate old and new manifest files
 *   <li>New manifest file represented by {@link ManifestFile}
 * </ul>
 *
 * The input2 is the error stream of the previous steps to prevent committing wrong results. It's a
 * {@link Tuple2} of:
 *
 * <ul>
 *   <li>Event time
 *   <li>Exception
 * </ul>
 *
 * Keyed by the event time, and the result is a single <code>true</code> which is emitted after the
 * watermark which signals, that manifest compaction round has finished, or <code>false</code> if
 * this round of manifest update is failed for any reason.
 */
public class ManifestUpdater
    extends KeyedCoProcessFunction<
        Long,
        Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
        Tuple2<Long, Exception>,
        Tuple2<Long, Boolean>> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestUpdater.class);

  private final String name;
  private final TableLoader tableLoader;
  private transient Table table;
  private transient ListState<ManifestFile> newManifests;
  private transient ListState<ManifestFile> oldManifests;
  private transient ListState<Exception> exceptions;
  private transient Counter errorCounter;
  private transient Counter addedManifestFileNumCounter;
  private transient Counter addedManifestFileSizeCounter;
  private transient Counter removedManifestFileNumCounter;
  private transient Counter removedManifestFileSizeCounter;

  public ManifestUpdater(String name, TableLoader tableLoader) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.name = name;
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(Configuration config) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.MAINTENANCE_ERROR_METRIC);
    this.addedManifestFileNumCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.ADDED_MANIFEST_FILE_NUM_METRIC);
    this.addedManifestFileSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.ADDED_MANIFEST_FILE_SIZE_METRIC);
    this.removedManifestFileNumCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_MANIFEST_FILE_NUM_METRIC);
    this.removedManifestFileSizeCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.REMOVED_MANIFEST_FILE_SIZE_METRIC);

    this.newManifests =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "manifestUpdaterNewManifestFiles", TypeInformation.of(ManifestFile.class)));
    this.oldManifests =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "manifestUpdaterOldManifestFiles", TypeInformation.of(ManifestFile.class)));
    this.exceptions =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "manifestUpdaterExceptions", TypeInformation.of(Exception.class)));

    tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public void processElement1(
      Tuple3<Long, ManifestSource, ManifestFile> value,
      Context ctx,
      Collector<Tuple2<Long, Boolean>> out)
      throws Exception {
    ctx.timerService().registerEventTimeTimer(value.f0);
    if (ManifestSource.NEW.equals(value.f1)) {
      newManifests.add(value.f2);
    } else {
      oldManifests.add(value.f2);
    }
  }

  @Override
  public void processElement2(
      Tuple2<Long, Exception> value, Context ctx, Collector<Tuple2<Long, Boolean>> out)
      throws Exception {
    ctx.timerService().registerEventTimeTimer(value.f0);
    exceptions.add(value.f1);
  }

  @Override
  public void onTimer(long ts, OnTimerContext ctx, Collector<Tuple2<Long, Boolean>> out) {
    try {
      Iterator<Exception> collected = exceptions.get().iterator();
      if (!collected.hasNext()) {
        table.refresh();

        RewriteManifests rewriteManifests = table.rewriteManifests();
        oldManifests.get().forEach(rewriteManifests::deleteManifest);
        newManifests.get().forEach(rewriteManifests::addManifest);
        rewriteManifests.commit();
        updateMetrics();

        out.collect(Tuple2.of(ts, true));
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Successfully finished manifest update for table {} from {} to {} at {}",
              table,
              Lists.newArrayList(oldManifests.get()),
              Lists.newArrayList(newManifests.get()),
              ctx.timestamp());
        } else {
          LOG.info(
              "Successfully finished manifest update for table {} at {}", table, ctx.timestamp());
        }
      } else {
        out.collect(Tuple2.of(ts, true));

        if (LOG.isDebugEnabled()) {
          LOG.info(
              "Omitting commit {}/{} on failure for table {} at {}",
              Lists.newArrayList(oldManifests.get().iterator()),
              Lists.newArrayList(newManifests.get().iterator()),
              table,
              ctx.timestamp());
        } else {
          LOG.info("Omitting commit on failure for table {} at {}", table, ctx.timestamp());
        }
      }
    } catch (Exception e) {
      LOG.info("Exception updating manifests for {} at {}", table, ctx.timestamp(), e);
      try {
        newManifests.get().forEach(f -> table.io().deleteFile(f.path()));
      } catch (Exception ex) {
        LOG.info(
            "Exception removing non-committed manifest files for {} at {}",
            table,
            ctx.timestamp(),
            ex);
      }

      ctx.output(ErrorAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    } finally {
      oldManifests.clear();
      newManifests.clear();
      exceptions.clear();
    }
  }

  private void updateMetrics() throws Exception {
    for (ManifestFile newManifest : newManifests.get()) {
      addedManifestFileNumCounter.inc();
      addedManifestFileSizeCounter.inc(newManifest.length());
    }

    for (ManifestFile oldManifest : oldManifests.get()) {
      removedManifestFileNumCounter.inc();
      removedManifestFileSizeCounter.inc(oldManifest.length());
    }
  }

  public enum ManifestSource {
    OLD,
    NEW
  }
}
