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

import java.util.Map;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg table {@link ManageSnapshots#createTag(String, long)}/{@link
 * ManageSnapshots#removeTag(String)} based lock implementation for {@link TriggerLock}.
 */
public class TagBasedLock implements TriggerLock {
  private static final Logger LOG = LoggerFactory.getLogger(TagBasedLock.class);
  static final String RUNNING_TAG = "__flink_maintenance_running";
  private static final int CHANGE_ATTEMPTS = 3;

  private final Table table;

  public TagBasedLock(TableLoader tableLoader) {
    tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  /**
   * The lock will be acquired by jobs in alphabetical order based on the jobId. A new empty commit
   * is added for a table without snapshots.
   *
   * @return <code>true</code> if the lock is acquired by this operator
   */
  @Override
  public boolean tryLock() {
    table.refresh();
    Map<String, SnapshotRef> refs = table.refs();
    if (refs.keySet().stream().anyMatch(key -> key.equals(RUNNING_TAG))) {
      LOG.info("Already running trigger detected: {}", refs.keySet());
      return false;
    }

    if (table.currentSnapshot() == null) {
      // Create an empty commit
      table.newFastAppend().commit();
      LOG.info("Empty table, new empty commit added for using tags");
    }

    try {
      Tasks.foreach(1)
          .retry(CHANGE_ATTEMPTS)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .run(
              unused -> {
                table.refresh();
                ManageSnapshots manage = table.manageSnapshots();
                manage.createTag(RUNNING_TAG, table.currentSnapshot().snapshotId());
                manage.commit();
                LOG.debug("Lock created");
              });
    } catch (Exception e) {
      LOG.info("Concurrent lock created. Stop concurrent maintenance jobs", e);
      return false;
    }

    return true;
  }

  @Override
  public void unlock() {
    table.refresh();

    if (table.refs().get(RUNNING_TAG) != null) {
      Tasks.foreach(1)
          .retry(CHANGE_ATTEMPTS)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .run(
              unused -> {
                table.refresh();
                ManageSnapshots manage = table.manageSnapshots();
                manage.removeTag(RUNNING_TAG);
                manage.commit();
              });
      LOG.debug("Lock removed");
    } else {
      LOG.warn("Missing lock, can not remove. Found {}", table.refs().keySet());
    }
  }
}
