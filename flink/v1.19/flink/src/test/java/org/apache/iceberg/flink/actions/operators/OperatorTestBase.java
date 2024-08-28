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

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Collection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.actions.streams.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.extension.RegisterExtension;

class OperatorTestBase {
  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;

  @RegisterExtension
  protected static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(getConfig())
              .build());

  @RegisterExtension
  final FlinkSqlExtension sql =
      new FlinkSqlExtension(
          "catalog",
          ImmutableMap.of("type", "iceberg", FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop"),
          "db");

  protected void assertRegex(Collection<String> actual, Collection<String> expected) {
    assertThat(actual).isNotNull().hasSize(expected.size());
    Collection<String> actualCopy = Sets.newHashSet(actual);
    expected.forEach(
        regex -> {
          for (String actualValue : actualCopy) {
            if (actualValue.matches(regex)) {
              actualCopy.remove(actualValue);
              return;
            }
          }
          fail("Expected to find %s in %s, missing %s", expected, actual, regex);
        });
  }

  private static Configuration getConfig() {
    Configuration config = new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG);
    MetricOptions.forReporter(config, "test_reporter")
        .set(MetricOptions.REPORTER_FACTORY_CLASS, MetricsReporterFactoryForTests.class.getName());
    return config;
  }
}
