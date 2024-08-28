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

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSchedulerTrigger implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSchedulerTrigger.class);
  private final List<Predicate> predicates;

  private StreamSchedulerTrigger(List<Predicate> predicates) {
    Preconditions.checkArgument(!predicates.isEmpty(), "Provide at least 1 condition");

    this.predicates = predicates;
  }

  public boolean check(long currentTime, TableChange event, long lastTime) {
    boolean result =
        predicates.stream()
            .anyMatch(
                p -> {
                  try {
                    return p.evaluate(lastTime, currentTime, event);
                  } catch (Exception e) {
                    throw new RuntimeException("Error accessing state", e);
                  }
                });
    LOG.debug(
        "Checking at {} event: {}, last: {} with result: {}", currentTime, event, lastTime, result);
    return result;
  }

  public static class Builder implements Serializable {
    private Integer commitNumber;
    private Integer fileNumber;
    private Long fileSize;
    private Integer deleteFileNumber;
    private Duration timeout;

    public Builder commitNumber(int newCommitNumber) {
      this.commitNumber = newCommitNumber;
      return this;
    }

    public Builder fileNumber(int newFileNumber) {
      this.fileNumber = newFileNumber;
      return this;
    }

    public Builder fileSize(long newFileSize) {
      this.fileSize = newFileSize;
      return this;
    }

    public Builder deleteFileNumber(int newDeleteFileNumber) {
      this.deleteFileNumber = newDeleteFileNumber;
      return this;
    }

    public Builder timeout(Duration newTimeout) {
      this.timeout = newTimeout;
      return this;
    }

    public StreamSchedulerTrigger build() {
      List<Predicate> predicates = Lists.newArrayList();
      if (commitNumber != null) {
        predicates.add((unused, unused2, change) -> change.commitNum() >= commitNumber);
      }

      if (fileNumber != null) {
        predicates.add(
            (unused, unused2, change) ->
                change.dataFileNum() + change.deleteFileNum() >= fileNumber);
      }

      if (fileSize != null) {
        predicates.add(
            (unused, unused2, change) ->
                change.dataFileSize() + change.deleteFileSize() >= fileSize);
      }

      if (deleteFileNumber != null) {
        predicates.add((unused, unused2, change) -> change.deleteFileNum() >= deleteFileNumber);
      }

      if (timeout != null) {
        predicates.add(
            (lastTrigger, currentTime, change) -> currentTime - lastTrigger >= timeout.toMillis());
      }

      return new StreamSchedulerTrigger(predicates);
    }
  }

  private interface Predicate extends Serializable {
    boolean evaluate(long lastTrigger, long currentTime, TableChange event);
  }
}
