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

import static org.apache.iceberg.TableProperties.ENCRYPTION_KMS_CLIENT_CUSTOM_PROPERTIES_PREFIX;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

public class DefaultEncryptionManagerFactory implements EncryptionManagerFactory {
  public static final String ICEBERG_ENCRYPTION_TABLE_KEY = "iceberg.encryption.table.key.id";

  private static class KmsClientSupplier {
    private final Map<String, String> catalogProperties;
    private final boolean ignoreTableProperties;

    KmsClientSupplier(Map<String, String> catalogProperties, boolean ignoreTableProperties) {
      this.catalogProperties = catalogProperties;
      this.ignoreTableProperties = ignoreTableProperties;
    }

    KmsClient create(Map<String, String> tableProperties) {
      // load kms impl from catalog properties, if not present fall back to table properties (if
      // allowed).
      String kmsImpl = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL);
      if (null == kmsImpl && !ignoreTableProperties) {
        kmsImpl = tableProperties.get(TableProperties.ENCRYPTION_KMS_CLIENT_IMPL);
      }

      Preconditions.checkArgument(
          null != kmsImpl,
          "KMS Client implementation class is not set (via "
              + CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL
              + " catalog property nor "
              + TableProperties.ENCRYPTION_KMS_CLIENT_IMPL
              + " table property). "
              + "IgnoreTableProperties is: "
              + ignoreTableProperties);

      final Map<String, String> props = Maps.newHashMap();

      if (!ignoreTableProperties) {
        for (Map.Entry<String, String> property : tableProperties.entrySet()) {
          if (property.getKey().startsWith(ENCRYPTION_KMS_CLIENT_CUSTOM_PROPERTIES_PREFIX)) {
            props.put(property.getKey(), property.getValue());
          }
        }
      }

      // Important: put catalog props after table props (former overrides latter)
      props.putAll(this.catalogProperties);

      return KmsUtil.loadKmsClient(kmsImpl, props);
    }
  }

  private KmsClientSupplier kmsClientSupplier;
  private KmsClient client;
  private Map<String, String> catalogPropertyMap;
  private boolean ignoreTableProperties;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.catalogPropertyMap = catalogProperties;
    ignoreTableProperties =
        PropertyUtil.propertyAsBoolean(
            catalogProperties,
            CatalogProperties.ENCRYPTION_IGNORE_TABLE_PROPS,
            CatalogProperties.ENCRYPTION_IGNORE_TABLE_PROPS_DEFAULT);
    kmsClientSupplier = new KmsClientSupplier(catalogProperties, ignoreTableProperties);
  }

  @Override
  public EncryptionManager create(TableMetadata tableMetadata) {
    if (null == tableMetadata) {
      return PlaintextEncryptionManager.INSTANCE;
    }

    Map<String, String> tableProperties = tableMetadata.properties();

    String tableKeyId = getTableKeyId(tableProperties);
    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.INSTANCE;
    } else {
      return new EnvelopeEncryptionManager(kmsClient(tableProperties), tableProperties, tableKeyId);
    }
  }

  private synchronized KmsClient kmsClient(Map<String, String> tableProperties) {
    if (client == null) {
      client = kmsClientSupplier.create(tableProperties);
    }
    return client;
  }

  private String getTableKeyId(Map<String, String> tableProperties) {
    String keyId = System.getProperty(ICEBERG_ENCRYPTION_TABLE_KEY);
    if (null == keyId) {
      keyId = catalogPropertyMap.get(ENCRYPTION_TABLE_KEY);
    }
    if (null == keyId && !ignoreTableProperties) {
      keyId = tableProperties.get(ENCRYPTION_TABLE_KEY);
    }

    return keyId;
  }

  @Override
  public synchronized void close() throws IOException {
    if (client != null) {
      client.close();
      client = null;
    }
  }
}
