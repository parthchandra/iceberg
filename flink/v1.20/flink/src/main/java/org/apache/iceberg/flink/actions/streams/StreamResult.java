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

import java.util.List;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class StreamResult {
  private final int id;
  private final String name;
  private final boolean scheduled;
  private boolean result;
  private long length;
  private List<Exception> exceptions;

  public StreamResult(int id, String name, boolean scheduled) {
    this.id = id;
    this.name = name;
    this.scheduled = scheduled;
    this.result = false;
    this.length = 0L;
    this.exceptions = Lists.newArrayList();
  }

  public StreamResult(
      int id,
      String name,
      boolean scheduled,
      boolean result,
      long length,
      List<Exception> exceptions) {
    this(id, name, scheduled);
    this.result = result;
    this.length = length;
    this.exceptions.addAll(exceptions);
  }

  public int id() {
    return id;
  }

  public String name() {
    return name;
  }

  public boolean scheduled() {
    return scheduled;
  }

  public boolean result() {
    return result;
  }

  public long length() {
    return length;
  }

  public List<Exception> exceptions() {
    return exceptions;
  }

  public void result(boolean newResult) {
    this.result = newResult;
  }

  public void length(long newLength) {
    this.length = newLength;
  }

  public void addExceptions(List<Exception> newExceptions) {
    exceptions.addAll(newExceptions);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    StreamResult that = (StreamResult) other;
    return this.id == that.id
        && this.name.equals(that.name)
        && this.scheduled == that.scheduled
        && this.result == that.result
        && this.length == that.length
        && this.exceptions.size() == that.exceptions.size();
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, scheduled, result, length, exceptions.size());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("name", name)
        .add("scheduled", scheduled)
        .add("result", result)
        .add("length", length)
        .add("exceptions", exceptions)
        .toString();
  }
}
