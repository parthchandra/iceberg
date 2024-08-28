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

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class ScheduleRequest {
  private long timestamp;
  private int id;
  private String name;
  private boolean triggered;

  public ScheduleRequest(long timestamp, int id, String name, boolean triggered) {
    this.timestamp = timestamp;
    this.id = id;
    this.name = name;
    this.triggered = triggered;
  }

  public long timestamp() {
    return timestamp;
  }

  public int id() {
    return id;
  }

  public String name() {
    return name;
  }

  public boolean triggered() {
    return triggered;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timestamp", timestamp)
        .add("id", id)
        .add("name", name)
        .add("triggered", triggered)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ScheduleRequest that = (ScheduleRequest) other;
    return this.timestamp == that.timestamp
        && this.id == that.id
        && this.name.equals(that.name)
        && this.triggered == that.triggered;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, id, name, triggered);
  }
}
