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

package org.apache.iceberg;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.encryption.EncryptionAlgorithm;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EnvelopeConfiguration;
import org.apache.iceberg.encryption.EnvelopeEncryptionManager;
import org.apache.iceberg.encryption.KmsClient;
import org.apache.iceberg.encryption.KmsUtil;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.ENCRYPTION_DATA_ALGORITHM;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DATA_ALGORITHM_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_KMS_CLIENT_CUSTOM_PROPERTIES_PREFIX;
import static org.apache.iceberg.TableProperties.ENCRYPTION_KMS_CLIENT_IMPL;
import static org.apache.iceberg.TableProperties.ENCRYPTION_PUSHDOWN_ENABLED;
import static org.apache.iceberg.TableProperties.ENCRYPTION_PUSHDOWN_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

/**
 * SPI interface to abstract table metadata access and updates.
 */
public interface TableOperations {

  /**
   * Return the currently loaded table metadata, without checking for updates.
   *
   * @return table metadata
   */
  TableMetadata current();

  /**
   * Return the current table metadata after checking for updates.
   *
   * @return table metadata
   */
  TableMetadata refresh();

  /**
   * Replace the base table metadata with a new version.
   * <p>
   * This method should implement and document atomicity guarantees.
   * <p>
   * Implementations must check that the base metadata is current to avoid overwriting updates.
   * Once the atomic commit operation succeeds, implementations must not perform any operations that
   * may fail because failure in this method cannot be distinguished from commit failure.
   * <p>
   * Implementations must throw a {@link org.apache.iceberg.exceptions.CommitStateUnknownException}
   * in cases where it cannot be determined if the commit succeeded or failed.
   * For example if a network partition causes the confirmation of the commit to be lost,
   * the implementation should throw a CommitStateUnknownException. This is important because downstream users of
   * this API need to know whether they can clean up the commit or not, if the state is unknown then it is not safe
   * to remove any files. All other exceptions will be treated as if the commit has failed.
   *
   * @param base     table metadata on which changes were based
   * @param metadata new table metadata with updates
   */
  void commit(TableMetadata base, TableMetadata metadata);

  /**
   * Returns a {@link FileIO} to read and write table data and metadata files.
   */
  FileIO io();

  /**
   * Returns an {@link org.apache.iceberg.encryption.EncryptionManager} for a table.
   * <p>
   * If table metadata/properties are not available yet, or if encryption is not configured in the table properties,
   * a PlaintextEncryptionManager is returned (no encryption).
   * Otherwise, an EnvelopeEncryptionManager is returned, configured with table key(s) and other parameters set up in
   * the table properties.
   */
  default EncryptionManager encryption() {
    TableMetadata tableMetadata = current();
    if (null == tableMetadata) {
      return new PlaintextEncryptionManager();
    }
    return createEncryptionManager(tableMetadata);
  }

  static EncryptionManager createEncryptionManager(TableMetadata tableMetadata) {
    Properties clientSideEncryptionProperties = null;
    String clientSideEncryptionConfigFile = System.getenv(EnvelopeEncryptionManager.CLIENT_SIDE_CRYPTO_CONFIG_FILE);
    String clientSideEncrPropSource = "System environment variable " +
        EnvelopeEncryptionManager.CLIENT_SIDE_CRYPTO_CONFIG_FILE;
    if (null == clientSideEncryptionConfigFile) {
      clientSideEncryptionConfigFile = System.getProperty(EnvelopeEncryptionManager.clientSideEncryptionConfigFile);
      clientSideEncrPropSource = "System property " + EnvelopeEncryptionManager.clientSideEncryptionConfigFile;
    }

    if (null != clientSideEncryptionConfigFile) {
      clientSideEncryptionProperties = new Properties();
      try {
        clientSideEncryptionProperties.load(new FileInputStream(clientSideEncryptionConfigFile));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to load client-side encryption properties from " +
            clientSideEncryptionConfigFile + ", configured via " + clientSideEncrPropSource, e);
      }
    }

    Map<String, String> tableProperties = tableMetadata.properties();

    // Verify that table encryption properties are not tampered with in storage, by comparing with client-side
    // encryption properties (if set)
    if (null != clientSideEncryptionProperties) {
      Set<String> keys = clientSideEncryptionProperties.stringPropertyNames();
      for (String key : keys) {
        if (!tableProperties.containsKey(key)) {
          throw new RuntimeException(EnvelopeEncryptionManager.encryptionConfigMismatchMessagePrefix +
              "Property " + key + " not found in table properties. Configured to " +
              clientSideEncryptionProperties.getProperty(key) + " in " + clientSideEncryptionConfigFile +
              ". Source of client-side configuration: " + clientSideEncrPropSource);
        }

        if (!tableProperties.get(key).equals(clientSideEncryptionProperties.getProperty(key))) {
          throw new RuntimeException(EnvelopeEncryptionManager.encryptionConfigMismatchMessagePrefix +
              "Property " + key + " is set in table properties to : " + tableProperties.get(key) +
              " and in client-side properties to : " + clientSideEncryptionProperties.getProperty(key) +
              ", set in " + clientSideEncryptionConfigFile +
              ". Source of client-side configuration: " + clientSideEncrPropSource);
        }
      }
    }

    String tableKeyId = PropertyUtil.propertyAsString(tableProperties, ENCRYPTION_TABLE_KEY, null);
    if (null == tableKeyId) { // Unencrypted table
      return new PlaintextEncryptionManager();
    }

    // At this point, we have an encrypted table
    boolean pushdown = PropertyUtil.propertyAsBoolean(tableProperties,
        ENCRYPTION_PUSHDOWN_ENABLED, ENCRYPTION_PUSHDOWN_ENABLED_DEFAULT);

    String dataEncryptionAlgorithm = PropertyUtil.propertyAsString(tableProperties,
        ENCRYPTION_DATA_ALGORITHM, ENCRYPTION_DATA_ALGORITHM_DEFAULT);

    EnvelopeConfiguration dataEncryptionConfig = EnvelopeConfiguration.builder()
        .singleWrap(tableKeyId)
        .useAlgorithm(EncryptionAlgorithm.valueOf(dataEncryptionAlgorithm))
        .build();

    String kmsClientImpl = PropertyUtil.propertyAsString(tableProperties, ENCRYPTION_KMS_CLIENT_IMPL, null);
    Preconditions.checkArgument(null != kmsClientImpl,
        "KMS Client implementation class is not set (via " + ENCRYPTION_KMS_CLIENT_IMPL + " table property)");

    // Pass custom kms configuration from table properties
    Map<String, String> kmsProperties = Maps.newHashMap();
    for (Map.Entry<String, String> property : tableProperties.entrySet()) {
      if (property.getKey().startsWith(ENCRYPTION_KMS_CLIENT_CUSTOM_PROPERTIES_PREFIX)) {
        kmsProperties.put(property.getKey(), property.getValue());
      }
    }

    KmsClient kmsClient = KmsUtil.loadKmsClient(kmsClientImpl, kmsProperties);
    int dataKeyLength = PropertyUtil.propertyAsInt(tableProperties, ENCRYPTION_DEK_LENGTH,
        ENCRYPTION_DEK_LENGTH_DEFAULT);

    return new EnvelopeEncryptionManager(pushdown, dataEncryptionConfig, kmsClient, dataKeyLength);
  }

  /**
   * Given the name of a metadata file, obtain the full path of that file using an appropriate base
   * location of the implementation's choosing.
   * <p>
   * The file may not exist yet, in which case the path should be returned as if it were to be created
   * by e.g. {@link FileIO#newOutputFile(String)}.
   */
  String metadataFileLocation(String fileName);

  /**
   * Returns a {@link LocationProvider} that supplies locations for new new data files.
   *
   * @return a location provider configured for the current table state
   */
  LocationProvider locationProvider();

  /**
   * Return a temporary {@link TableOperations} instance that uses configuration from uncommitted metadata.
   * <p>
   * This is called by transactions when uncommitted table metadata should be used; for example, to create a metadata
   * file location based on metadata in the transaction that has not been committed.
   * <p>
   * Transactions will not call {@link #refresh()} or {@link #commit(TableMetadata, TableMetadata)}.
   *
   * @param uncommittedMetadata uncommitted table metadata
   * @return a temporary table operations that behaves like the uncommitted metadata is current
   */
  default TableOperations temp(TableMetadata uncommittedMetadata) {
    return this;
  }

  /**
   * Create a new ID for a Snapshot
   *
   * @return a long snapshot ID
   */
  default long newSnapshotId() {
    UUID uuid = UUID.randomUUID();
    long mostSignificantBits = uuid.getMostSignificantBits();
    long leastSignificantBits = uuid.getLeastSignificantBits();
    return (mostSignificantBits ^ leastSignificantBits) & Long.MAX_VALUE;
  }

}
