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

import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.DUMMY_NAME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.EVENT_TIME;
import static org.apache.iceberg.flink.actions.operators.ConstantsForTests.TABLE_NAME;
import static org.apache.iceberg.flink.actions.operators.ManifestUpdater.ManifestSource.NEW;
import static org.apache.iceberg.flink.actions.operators.ManifestUpdater.ManifestSource.OLD;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.ManifestForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class TestManifestUpdater extends OperatorTestBase {
  @Test
  void testUpdate() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar, spec varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a', 'p1'), (2, 'b', 'p2')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c', 'p1')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<ManifestFile> oldManifests = table.currentSnapshot().dataManifests(table.io());
    ManifestFile newManifest = concatDataManifest(oldManifests, table);

    ManifestUpdater manifestUpdater = new ManifestUpdater(DUMMY_NAME, tableLoader);
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, Exception>,
            Tuple2<Long, Boolean>>
        testHarness = ManifestUtil.harnessForUpdater(manifestUpdater)) {
      testHarness.open();

      testHarness.processElement1(Tuple3.of(EVENT_TIME, NEW, newManifest), EVENT_TIME);

      for (ManifestFile manifestFile : oldManifests) {
        testHarness.processElement1(Tuple3.of(EVENT_TIME, OLD, manifestFile), EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(new Watermark(EVENT_TIME));

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      assertThat(testHarness.extractOutputValues().stream().map(t -> t.f1))
          .isEqualTo(ImmutableList.of(true));
    }

    table.refresh();

    List<ManifestFile> actual = table.currentSnapshot().allManifests(table.io());
    assertThat(actual).hasSize(1);
    assertThat(actual.get(0).path()).isEqualTo(newManifest.path());
  }

  @Test
  void testStateRestore() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar, PRIMARY KEY(`id`) NOT ENFORCED) WITH ('format-version'='2', 'write.upsert.enabled'='true')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (3, 'c')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    List<ManifestFile> oldDataManifests = table.currentSnapshot().dataManifests(table.io());
    ManifestFile newDataManifest = concatDataManifest(oldDataManifests, table);

    List<ManifestFile> oldDeleteManifests = table.currentSnapshot().deleteManifests(table.io());
    ManifestFile newDeleteManifest = concatDeleteManifest(oldDeleteManifests, table);

    OperatorSubtaskState state;
    ManifestUpdater manifestUpdater = new ManifestUpdater(DUMMY_NAME, tableLoader);
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, Exception>,
            Tuple2<Long, Boolean>>
        testHarness = ManifestUtil.harnessForUpdater(manifestUpdater)) {
      testHarness.open();

      testHarness.processElement1(Tuple3.of(EVENT_TIME, NEW, newDeleteManifest), EVENT_TIME);

      for (ManifestFile manifestFile : oldDataManifests) {
        testHarness.processElement1(Tuple3.of(EVENT_TIME, OLD, manifestFile), EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore from the state and continue
    try (KeyedTwoInputStreamOperatorTestHarness<
            Long,
            Tuple3<Long, ManifestUpdater.ManifestSource, ManifestFile>,
            Tuple2<Long, Exception>,
            Tuple2<Long, Boolean>>
        testHarness = ManifestUtil.harnessForUpdater(manifestUpdater)) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement1(Tuple3.of(EVENT_TIME, NEW, newDataManifest), EVENT_TIME);

      for (ManifestFile manifestFile : oldDeleteManifests) {
        testHarness.processElement1(Tuple3.of(EVENT_TIME, OLD, manifestFile), EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(new Watermark(EVENT_TIME));

      assertThat(testHarness.getSideOutput(ErrorAggregator.ERROR_STREAM)).isNull();
      assertThat(testHarness.extractOutputValues().stream().map(t -> t.f1))
          .isEqualTo(ImmutableList.of(true));
    }

    table.refresh();

    List<ManifestFile> actual = table.currentSnapshot().allManifests(table.io());
    assertThat(actual).hasSize(2);
    assertThat(actual.stream().map(ManifestFile::path).collect(Collectors.toSet()))
        .isEqualTo(ImmutableSet.of(newDataManifest.path(), newDeleteManifest.path()));
  }

  private ManifestFile concatDataManifest(List<ManifestFile> files, Table table)
      throws IOException {
    String target = table.location() + "/concat-data-manifest.avro";
    int count = 0;
    try (ManifestWriter<DataFile> writer =
        ManifestFiles.write(2, table.spec(), table.io().newOutputFile(target), null)) {
      for (ManifestFile old : files) {
        ManifestReader<DataFile> reader = ManifestFiles.read(old, table.io(), table.specs());
        reader
            .iterator()
            .forEachRemaining(
                dataFile ->
                    writer.existing(
                        dataFile, old.snapshotId(), old.sequenceNumber(), old.minSequenceNumber()));
        ++count;
      }
    }

    return new ManifestForTests(new File(target), ManifestContent.DATA, count);
  }

  private ManifestFile concatDeleteManifest(List<ManifestFile> files, Table table)
      throws IOException {
    String target = table.location() + "/concat-delete-manifest.avro";
    int count = 0;
    try (ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(
            2, table.spec(), table.io().newOutputFile(target), null)) {
      for (ManifestFile old : files) {
        ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(old, table.io(), table.specs());
        reader
            .iterator()
            .forEachRemaining(
                dataFile ->
                    writer.existing(
                        dataFile, old.snapshotId(), old.sequenceNumber(), old.minSequenceNumber()));
        ++count;
      }
    }

    return new ManifestForTests(new File(target), ManifestContent.DELETES, count);
  }
}
