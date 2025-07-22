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

public class MetricConstants {
  public static final String GROUP_KEY = "compactionTask";
  public static final String GROUP_VALUE_DEFAULT = "compactionTask";

  public static final String MAINTENANCE_ERROR_METRIC = "maintenanceError";

  // DeleteFiles metrics
  public static final String DELETE_FILE_ERROR_METRIC = "deleteError";
  public static final String DELETE_FILE_SUCCESS_METRIC = "deleteSuccess";

  // DataFileUpdater metrics
  public static final String ADDED_DATA_FILE_NUM_METRIC = "addedDataFileNum";
  public static final String ADDED_DATA_FILE_SIZE_METRIC = "addedDataFileSize";
  public static final String REMOVED_DATA_FILE_NUM_METRIC = "removedDataFileNum";
  public static final String REMOVED_DATA_FILE_SIZE_METRIC = "removedDataFileSize";
  public static final String REMOVED_POSITIONAL_FILE_NUM_METRIC = "removedPositionalDeleteFileNum";
  public static final String REMOVED_POSITIONAL_FILE_SIZE_METRIC =
      "removedPositionalDeleteFileSize";
  public static final String REMOVED_EQUALITY_FILE_NUM_METRIC = "removedEqualityDeleteFileNum";
  public static final String REMOVED_EQUALITY_FILE_SIZE_METRIC = "removedEqualityDeleteFileSize";

  // IncompatibleSchemaChangeBlocker metrics
  public static final String INCOMPATIBLE_SCHEMA_CHANGE = "incompatibleSchemaChange";
  public static final String INCOMPATIBLE_SPEC_CHANGE = "incompatibleSpecChange";

  // ManifestUpdater metrics
  public static final String ADDED_MANIFEST_FILE_NUM_METRIC = "addedManifestFileNum";
  public static final String ADDED_MANIFEST_FILE_SIZE_METRIC = "addedManifestFileSize";
  public static final String REMOVED_MANIFEST_FILE_NUM_METRIC = "removedManifestFileNum";
  public static final String REMOVED_MANIFEST_FILE_SIZE_METRIC = "removedManifestFileSize";

  // RateLimiter metrics
  public static final String RATE_LIMITER_TRIGGERED = "rateLimiterTriggered";
  public static final String CONCURRENT_RUN_TRIGGERED = "concurrentRunTriggered";

  // ResultAggregator metrics
  public static final String SUCCESSFUL_TRIGGER_COUNTER = "successfulTrigger";
  public static final String FAILED_TRIGGER_COUNTER = "failedTrigger";
  public static final String SUCCESSFUL_STREAM_COUNTER = "successfulStream";
  public static final String FAILED_STREAM_COUNTER = "failedStream";
  public static final String LAST_RUN_LENGTH = "lastRunLength";

  // StreamScheduler metrics
  public static final String SCHEDULER_FIRED = "schedulerFired";
  public static final String SCHEDULER_SKIPPED = "schedulerSkipped";

  // WriteManifests metrics
  public static final String REMOVED_PARTIAL_MANIFEST_NUM = "removedPartialManifestNum";
  public static final String REMOVED_PARTIAL_MANIFEST_SIZE = "removedPartialManifestSize";

  private MetricConstants() {
    // do not instantiate
  }
}
