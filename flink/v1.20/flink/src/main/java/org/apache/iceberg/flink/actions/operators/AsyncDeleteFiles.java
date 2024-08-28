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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delete the files using the {@link FileIO}. */
public class AsyncDeleteFiles extends RichAsyncFunction<String, Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDeleteFiles.class);

  public static final Predicate<Collection<Boolean>> FAILED_PREDICATE = new FailedPredicate();

  private final String name;
  private final FileIO io;
  private final int workerPoolSize;
  private final String tableName;

  private transient ExecutorService workerPool;
  private transient Counter errorCounter;
  private transient Counter successCounter;

  public AsyncDeleteFiles(String name, TableLoader tableLoader, int workerPoolSize) {
    Preconditions.checkNotNull(name, "Name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.name = name;
    tableLoader.open();
    Table table = tableLoader.loadTable();
    this.io = table.io();
    this.workerPoolSize = workerPoolSize;
    this.tableName = table.name();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.errorCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, name)
            .counter(MetricConstants.DELETE_FILE_ERROR_METRIC);
    this.successCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup("compactionTask", name)
            .counter(MetricConstants.DELETE_FILE_SUCCESS_METRIC);

    this.workerPool =
        ThreadPools.newWorkerPool(tableName + "-" + name + "-async-delete-files", workerPoolSize);
  }

  @Override
  public void asyncInvoke(String fileName, ResultFuture<Boolean> resultFuture) {
    workerPool.execute(
        () -> {
          try {
            LOG.info("Deleting file: {} with {}", fileName, name);
            io.deleteFile(fileName);
            resultFuture.complete(Collections.singletonList(true));
            successCounter.inc();
          } catch (Throwable e) {
            LOG.info("Failed to delete file {} with {}", fileName, name, e);
            resultFuture.complete(Collections.singletonList(false));
            errorCounter.inc();
          }
        });
  }

  private static class FailedPredicate implements Predicate<Collection<Boolean>>, Serializable {
    @Override
    public boolean test(Collection<Boolean> collection) {
      return collection.size() != 1 || !collection.iterator().next();
    }
  }
}
