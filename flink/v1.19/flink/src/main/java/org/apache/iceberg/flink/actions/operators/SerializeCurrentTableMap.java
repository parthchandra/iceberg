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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializeCurrentTableMap<T> extends RichMapFunction<T, SerializableTable> {
  private static final Logger LOG = LoggerFactory.getLogger(SerializeCurrentTableMap.class);

  private final TableLoader tableLoader;
  private transient Table table;

  public SerializeCurrentTableMap(TableLoader tableLoader) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.tableLoader = tableLoader;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public SerializableTable map(T value) throws Exception {
    table.refresh();
    LOG.info("Refreshed table {} to {}", table.name(), table.currentSnapshot().snapshotId());
    return (SerializableTable) SerializableTable.copyOf(table);
  }
}
