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
package org.apache.iceberg.encryption;

import static org.apache.iceberg.Files.localOutput;

import java.io.File;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class EncryptedLocalOutputFile implements OutputFile, NativelyEncryptedFile {
  private OutputFile localOutputFile;
  private NativeFileCryptoParameters nativeEncryptionParameters;

  public EncryptedLocalOutputFile(File file) {
    localOutputFile = localOutput(file);
  }

  @Override
  public PositionOutputStream create() {
    return localOutputFile.create();
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return localOutputFile.createOrOverwrite();
  }

  @Override
  public String location() {
    return localOutputFile.location();
  }

  @Override
  public InputFile toInputFile() {
    return localOutputFile.toInputFile();
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeEncryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeEncryptionParameters = nativeCryptoParameters;
  }
}
