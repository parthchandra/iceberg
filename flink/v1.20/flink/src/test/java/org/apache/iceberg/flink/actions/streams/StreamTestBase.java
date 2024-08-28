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

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.apache.iceberg.flink.actions.streams.ScheduledInfraExtension.IGNORE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.actions.operators.FlinkSqlExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

class StreamTestBase {
  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;
  static final String DB_NAME = "db";
  static final String TABLE_NAME = "test_table";
  static final String UID_PREFIX = "UID-Dummy";
  static final String SLOT_SHARING_GROUP = "SlotSharingGroup";
  static final String DUMMY_NAME = "dummy";

  @RegisterExtension
  static MiniClusterExtension miniClusterExtension =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(getConfig())
              .build());

  @BeforeEach
  public void resetMetrics() {
    MetricsReporterFactoryForTests.reset();
  }

  @RegisterExtension
  FlinkSqlExtension sql =
      new FlinkSqlExtension(
          "catalog",
          ImmutableMap.of("type", "iceberg", FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop"),
          DB_NAME);

  void checkUidsAreSet(StreamExecutionEnvironment env, String uidPrefix) {
    env.getTransformations().stream()
        .filter(t -> !(t instanceof SinkTransformation) && !(t.getName().equals(IGNORE)))
        .forEach(
            transformation -> {
              assertThat(transformation.getUid()).isNotNull();
              if (uidPrefix != null) {
                assertThat(transformation.getUid()).contains(UID_PREFIX);
              }
            });
  }

  private static Configuration getConfig() {
    Configuration config = new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG);
    MetricOptions.forReporter(config, "test_reporter")
        .set(MetricOptions.REPORTER_FACTORY_CLASS, MetricsReporterFactoryForTests.class.getName());
    return config;
  }

  void checkSlotSharingGroupsAreSet(StreamExecutionEnvironment env, String name) {
    String nameToCheck = name != null ? name : StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;

    env.getTransformations().stream()
        .filter(t -> !(t instanceof SinkTransformation) && !(t.getName().equals(IGNORE)))
        .forEach(
            t -> {
              assertThat(t.getSlotSharingGroup()).isPresent();
              assertThat(t.getSlotSharingGroup().get().getName()).isEqualTo(nameToCheck);
            });
  }

  /**
   * Checks if the source of the transformation is a {@link
   * org.apache.iceberg.flink.actions.operators.TableChangeSource}. We expect that the source is
   * before the {@link org.apache.iceberg.flink.actions.operators.RateLimiter}. We still return
   * {@link Optional#empty()} if the source is a {@link LegacySourceTransformation}. We expect that
   * in this the source is a {@link ManualSource} created for testing.
   *
   * @param transformation for which we check for the source
   * @return The {@link org.apache.iceberg.flink.actions.operators.TableChangeSource} if we found it
   */
  Transformation<?> source(Transformation<?> transformation) {
    if (transformation.getName().equals("Rate limiter")) {
      assertThat(transformation.getInputs()).hasSize(1);
      assertThat(transformation.getInputs().get(0).getInputs()).hasSize(1);
      Transformation<?> source = transformation.getInputs().get(0).getInputs().get(0);
      if (!(source instanceof LegacySourceTransformation)) {
        // This is the TableChangeSource source
        return source;
      }
    }

    return null;
  }
}
