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

package org.apache.iceberg.spark.source;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock manager that allows to lock tables within a single JVM.
 *
 * This class is helpful to prevent retries when one Spark driver executes multiple jobs on the same table.
 */
public class LockManager {
  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);
  private static final Map<String, Lock> LOCKS = new ConcurrentHashMap<>();

  private LockManager() {}

  public static void lock(Table table) {
    LOG.info("Trying to lock {}", table);
    Lock tableLock = LOCKS.computeIfAbsent(table.location(), location -> new ReentrantLock());
    tableLock.lock();
    LOG.info("Locked {}", table);
  }

  public static void unlock(Table table) {
    LOG.info("Unlocking {}", table);
    Lock tableLock = LOCKS.get(table.location());
    if (tableLock != null) {
      tableLock.unlock();
      LOG.info("Unlocked {}", table);
    }
  }
}
