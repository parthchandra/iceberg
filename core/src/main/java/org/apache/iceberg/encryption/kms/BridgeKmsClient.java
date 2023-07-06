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
package org.apache.iceberg.encryption.kms;

import com.google.errorprone.annotations.MustBeClosed;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This KMSClient implementation bridges a gap between KMS systems able to wrap/unwrap data keys
 * with master keys - and Secret engines that are able only to store the master keys. Working with
 * the latter (like ACI Whisper) requires a user to fetch the keys from the Secret engine and to
 * place them in a temp file or in a system environment variable. The BridgeKmsClient loads the
 * master keys from the file or environment, and wraps/unwraps data keys locally.
 */
public class BridgeKmsClient extends LocalKeyWrapClient {
  private static final Logger LOG = LoggerFactory.getLogger(BridgeKmsClient.class);

  /**
   * Table properties must keep unique identifier of the key source storage (eg Whisper AD group id
   * + bucket + secret name). The value of this property is a free-format string that helps readers
   * find where the table keys are kept.
   */
  private static final String KEY_SOURCE_UID = "kms.client.bridge.key.source.uid";

  /**
   * After the keys are retrieved from the source storage (eg Whisper), they can be passed to the
   * reader process in one of two ways. This table property, when set to "true", triggers their
   * passing via a system environment variable. By default, it's "false".
   */
  private static final String REQUIRE_ENV_KEY_PASSING =
      "kms.client.bridge.require.environment.keys";

  /** The default name of system environment variable for key passing. */
  private static final String DEFAULT_ENV_VAR_FOR_KEYS = "BRIDGE_VERSIONED_KEY_LIST";

  /** The name of system environment variable for key passing can be changed with this parameter. */
  private static final String ENV_VAR_FOR_KEYS = "kms.client.bridge.environment.keys.var";

  /**
   * After the keys are retrieved from the source storage (eg Whisper), they can be passed to the
   * reader runtime in one of two ways. This table property facilitates their passing via a file (or
   * a set of files fitting a pattern, with a format defined in
   * https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String))
   */
  private static final String KEY_FILE = "kms.client.bridge.key.file.path.pattern";

  private static final String SPLIT_PATTERN = "\\s*,[,\\s]*";

  private ConcurrentMap<String, ConcurrentMap<String, byte[]>> masterKeyHistoryMap =
      Maps.newConcurrentMap();
  private String latestKeyVersionString;

  @Override
  public boolean supportsKeyGeneration() {
    return false;
  }

  @Override
  public KeyGenerationResult generateKey(String keyId) {
    throw new UnsupportedOperationException("BridgeKmsClient doesn't generate keys");
  }

  @Override
  protected MasterKeyWithVersion getLatestMasterKey(String masterKeyIdentifier) {
    byte[] masterKey = masterKeyHistoryMap.get(latestKeyVersionString).get(masterKeyIdentifier);
    Preconditions.checkArgument(
        masterKey != null,
        "Failed to find master key: "
            + masterKeyIdentifier
            + ". Key version: "
            + latestKeyVersionString);

    return new MasterKeyWithVersion(masterKey, latestKeyVersionString);
  }

  @Override
  protected byte[] getMasterKey(String masterKeyIdentifier, String masterKeyVersion) {
    Map<String, byte[]> masterKeyMap = masterKeyHistoryMap.get(masterKeyVersion);
    Preconditions.checkArgument(
        masterKeyMap != null, "Failed to find keys with version " + masterKeyVersion);

    byte[] masterKey = masterKeyMap.get(masterKeyIdentifier);
    Preconditions.checkArgument(
        masterKey != null,
        "Failed to find master key: " + masterKeyIdentifier + ". Key version: " + masterKeyVersion);

    return masterKey;
  }

  @Override
  public void initialize(Map<String, String> kmsClientProperties) {
    Preconditions.checkNotNull(
        kmsClientProperties.get(KEY_SOURCE_UID),
        "Table properties must keep unique identifier of key source (eg Whisper AD group id + bucket). "
            + "Set the "
            + KEY_SOURCE_UID
            + " property - any string that helps readers find the table key");
    String[] masterKeys = loadMasterKeys(kmsClientProperties);
    parseKeyList(masterKeys);
  }

  /**
   * This method loads the master keys from a system property, or from a file. But if needed, a user
   * can extend the BridgeKmsClient class and override this method with a code that loads the keys
   * from any other source.
   *
   * @param kmsClientProperties table properties passed to KmsClient
   * @return array of strings, formatted as "{key_name}:v{key_version_int}:{key_content_base64}",
   *     for example "keyA:v0:CxhhcDxuaE9lNXZfLTdMf1wjZy4sa1leNK3k3SrYkc4="
   */
  protected String[] loadMasterKeys(Map<String, String> kmsClientProperties) {
    String[] result;

    // First, check if should be in system environment
    boolean requireEnvPassing =
        Boolean.parseBoolean(kmsClientProperties.get(REQUIRE_ENV_KEY_PASSING));
    if (requireEnvPassing) {
      String keyListVarName =
          kmsClientProperties.getOrDefault(ENV_VAR_FOR_KEYS, DEFAULT_ENV_VAR_FOR_KEYS);
      String envMasterKeyList = System.getenv(keyListVarName);
      Preconditions.checkArgument(
          envMasterKeyList != null, "Failed to load keys - not found in environment parameters");
      result = envMasterKeyList.split(SPLIT_PATTERN);
      LOG.info("{} master key(s) loaded from system environment", result.length);
    } else if (kmsClientProperties.containsKey(KEY_FILE)) {
      // Second, try to read keys from file
      String filePath = kmsClientProperties.get(KEY_FILE);
      result = readMasterKeysFromFiles(filePath);
      LOG.info("{} master key(s) loaded from file {}", result.length, filePath);
    } else {
      throw new RuntimeException(
          "Failed to load keys - no environment or file source is configured");
    }

    return result;
  }

  private static String[] readMasterKeysFromFiles(final String filePathPattern) {
    try (Stream<Path> matchedPaths = matchedFilePaths(filePathPattern)) {
      final String fileContent =
          matchedPaths.map(path -> readFile(path.toString())).collect(Collectors.joining(","));
      Preconditions.checkArgument(
          fileContent.length() > 0,
          "Couldn't read keys. Verify existence and content of file(s): " + filePathPattern);
      return fileContent.split(SPLIT_PATTERN);
    }
  }

  @MustBeClosed
  private static Stream<Path> matchedFilePaths(final String filePathPattern) {
    final File absolutePattern = new File(filePathPattern).getAbsoluteFile();
    final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + absolutePattern);
    try {
      return Files.list(FileSystems.getDefault().getPath(absolutePattern.getParent()))
          .filter(matcher::matches);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to find files matching pattern " + filePathPattern, e);
    }
  }

  private static String readFile(final String filePath) {
    try (BufferedReader fileReader = new BufferedReader(new FileReader(filePath))) {
      StringBuilder lines = new StringBuilder();
      for (String line; (line = fileReader.readLine()) != null; ) {
        lines.append(line);
        lines.append(",");
      }
      return lines.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read keys from file " + filePath, e);
    }
  }

  private void parseKeyList(String[] masterKeys) {
    int latestKeyVersion = -1;

    for (String masterKey : masterKeys) {
      String[] parts = masterKey.split(":", -1);
      Preconditions.checkArgument(
          parts.length == 3,
          "Versioned key " + masterKey + " does not have 3 components, separated by :");
      String keyName = parts[0].trim();
      String keyVersion = parts[1].trim();
      Preconditions.checkArgument(
          keyVersion.startsWith("v"), "Key version " + keyVersion + " does not start with v");

      int version;
      try {
        version = Integer.parseInt(keyVersion.substring(1));
      } catch (NumberFormatException e) {
        throw new RuntimeException(
            "Version component was not an Integer - " + keyVersion.substring(1), e);
      }

      if (version > latestKeyVersion) {
        latestKeyVersion = version;
      }

      Map<String, byte[]> keyMap =
          masterKeyHistoryMap.computeIfAbsent(keyVersion, k -> Maps.newConcurrentMap());
      String key = parts[2].trim();
      byte[] keyBytes;
      try {
        keyBytes = Base64.getDecoder().decode(key);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException("Key content is not a valid base64 scheme - " + key);
      }
      keyMap.put(keyName, keyBytes);
    }

    latestKeyVersionString = "v" + latestKeyVersion;
  }
}
