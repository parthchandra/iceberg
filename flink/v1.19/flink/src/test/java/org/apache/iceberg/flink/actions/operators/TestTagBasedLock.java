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

import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;

class TestTagBasedLock extends OperatorTestBase {
  @Test
  void testTryLock() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TagBasedLock lockManager1 = new TagBasedLock(tableLoader);
    TagBasedLock lockManager2 = new TagBasedLock(tableLoader);
    assertThat(lockManager1.tryLock()).isTrue();
    assertThat(lockManager1.tryLock()).isFalse();
    assertThat(lockManager2.tryLock()).isFalse();
  }

  @Test
  void testUnLock() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TagBasedLock lockManager1 = new TagBasedLock(tableLoader);
    assertThat(lockManager1.tryLock()).isTrue();

    lockManager1.unlock();
    assertThat(lockManager1.tryLock()).isTrue();
  }
}
