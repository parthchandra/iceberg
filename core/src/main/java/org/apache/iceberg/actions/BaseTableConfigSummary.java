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
package org.apache.iceberg.actions;

import java.util.Comparator;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.AnalyzeTable.TableConfigSummary;

public class BaseTableConfigSummary implements TableConfigSummary {

  private final TableMetadata metadata;

  public BaseTableConfigSummary(TableMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public int formatVersion() {
    return metadata.formatVersion();
  }

  @Override
  public String uuid() {
    return metadata.uuid();
  }

  @Override
  public String location() {
    return metadata.location();
  }

  @Override
  public String metadataFileLocation() {
    return metadata.metadataFileLocation();
  }

  @Override
  public long lastUpdatedMs() {
    return metadata.lastUpdatedMillis();
  }

  @Override
  public long lastSequenceNumber() {
    return metadata.lastSequenceNumber();
  }

  @Override
  public int lastAssignedColumnId() {
    return metadata.lastColumnId();
  }

  @Override
  public int lastAssignedPartitionId() {
    return metadata.lastAssignedPartitionId();
  }

  @Override
  public Schema schema() {
    return metadata.schema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return metadata.schemasById();
  }

  @Override
  public PartitionSpec spec() {
    return metadata.spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return metadata.specsById();
  }

  @Override
  public SortOrder sortOrder() {
    return metadata.sortOrder();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return metadata.sortOrdersById();
  }

  @Override
  public Map<String, String> properties() {
    return metadata.properties();
  }

  @Override
  public String toFormattedString() {
    SummaryFormatter formatter = new SummaryFormatter();

    formatter.header("TABLE CONFIGURATION");

    formatter.property("format-version", formatVersion());
    formatter.property("uuid", uuid());
    formatter.property("location", location());
    formatter.property("metadata-file-location", metadataFileLocation());
    formatter.property("last-updated-ms", lastUpdatedMs());
    formatter.property("last-sequence-number", lastSequenceNumber());
    formatter.propertyMap("table-properties", properties());

    formatter.subHeader("Schemas");
    formatter.property("current-schema-id", schema().schemaId());
    formatter.property("last-assigned-column-id", lastAssignedColumnId());
    schemas().keySet().stream()
        .sorted(Comparator.reverseOrder())
        .forEach(
            id -> {
              formatter.subSubHeader(String.format("schema (%d)", id));
              formatter.object(schemas().get(id));
            });

    formatter.subHeader("Specs");
    formatter.property("default-spec-id", spec().specId());
    formatter.property("last-assigned-partition-id", lastAssignedPartitionId());
    specs().keySet().stream()
        .sorted(Comparator.reverseOrder())
        .forEach(
            id -> {
              formatter.subSubHeader(String.format("spec (%d)", id));
              formatter.object(specs().get(id));
            });

    formatter.subHeader("Sort Orders");
    formatter.property("default-sort-order-id", sortOrder().orderId());
    sortOrders().keySet().stream()
        .sorted(Comparator.reverseOrder())
        .forEach(
            id -> {
              formatter.subSubHeader(String.format("sort order (%d)", id));
              formatter.object(sortOrders().get(id));
            });

    return formatter.compileString();
  }
}
