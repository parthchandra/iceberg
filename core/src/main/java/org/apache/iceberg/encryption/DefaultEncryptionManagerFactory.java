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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.ENCRYPTION_KMS_CLIENT_CUSTOM_PROPERTIES_PREFIX;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

public class DefaultEncryptionManagerFactory implements EncryptionManagerFactory {

  private static class KmsClientSupplier {
    private final Map<String, String> catalogProperties;

    KmsClientSupplier(Map<String, String> catalogProperties) {
      this.catalogProperties = catalogProperties;
    }

    KmsClient create(Map<String, String> tableProperties) {
      final Map<String, String> props = Maps.newHashMap();
      props.putAll(this.catalogProperties);
      // load kms impl from catalog properties, if not present fall back to table properties.
      final String kmsImpl = props.computeIfAbsent(
          CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL,
          k -> tableProperties.get(TableProperties.ENCRYPTION_KMS_CLIENT_IMPL));

      Preconditions.checkArgument(null != kmsImpl, "KMS Client implementation class is not set (via " +
          CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL + " catalog property ) nor " +
          TableProperties.ENCRYPTION_KMS_CLIENT_IMPL + " table property");

      for (Map.Entry<String, String> property : tableProperties.entrySet()) {
        if (property.getKey().startsWith(ENCRYPTION_KMS_CLIENT_CUSTOM_PROPERTIES_PREFIX)) {
          props.put(property.getKey(), property.getValue());
        }
      }
      return KmsUtil.loadKmsClient(kmsImpl, props);
    }
  }

  private KmsClientSupplier kmsClientSupplier;

  private KmsClient client;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    kmsClientSupplier = new KmsClientSupplier(catalogProperties);
  }

  @Override
  public EncryptionManager create(TableMetadata tableMetadata) {
    if (null == tableMetadata) {
      return PlaintextEncryptionManager.INSTANCE;
    }

    Map<String, String> tableProperties = tableMetadata.properties();

    String tableKeyId = PropertyUtil.propertyAsString(tableProperties, ENCRYPTION_TABLE_KEY, null);
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

  @Override
  public synchronized void close() throws IOException {
    if (client != null) {
      client.close();
      client = null;
    }
  }
}
