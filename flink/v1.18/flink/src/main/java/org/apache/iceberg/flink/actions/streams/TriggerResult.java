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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class TriggerResult {
  private final long timestamp;
  private final boolean overall;
  private final long length;
  private final List<StreamResult> results;

  public TriggerResult(long timestamp, boolean overall, long length, List<StreamResult> results) {
    this.timestamp = timestamp;
    this.overall = overall;
    this.results = results;
    this.length = length;
  }

  public long timestamp() {
    return timestamp;
  }

  public boolean overall() {
    return overall;
  }

  public long length() {
    return length;
  }

  public List<StreamResult> results() {
    return results;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TriggerResult that = (TriggerResult) other;
    return this.timestamp == that.timestamp
        && this.overall == that.overall
        && this.length == that.length
        && this.results.equals(that.results);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, overall, length, Arrays.hashCode(results.toArray()));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timestamp", timestamp)
        .add("overall", overall)
        .add("length", length)
        .add("results", results)
        .toString();
  }
}
