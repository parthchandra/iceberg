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
package org.apache.iceberg.flink.actions;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class ManifestForTests extends GenericManifestFile {
  public ManifestForTests(ManifestFile original) {
    super(
        original.path(),
        original.length(),
        0,
        original.content(),
        0L,
        0L,
        0L,
        0,
        0L,
        0,
        0L,
        0,
        0L,
        ImmutableList.of(),
        ByteBuffer.wrap(new byte[0]));
  }

  public ManifestForTests(File file, ManifestContent content, int existingFilesCount) {
    super(
        file.getPath(),
        file.length(),
        0,
        content,
        -1,
        0,
        null,
        0,
        0L,
        existingFilesCount,
        0L,
        0,
        0L,
        ImmutableList.of(),
        ByteBuffer.wrap(new byte[0]));
  }

  public ManifestForTests(String path) {
    super(
        path,
        10L,
        0,
        ManifestContent.DATA,
        -1,
        0,
        null,
        0,
        0L,
        0,
        0L,
        0,
        0L,
        ImmutableList.of(),
        ByteBuffer.wrap(new byte[0]));
  }
}
