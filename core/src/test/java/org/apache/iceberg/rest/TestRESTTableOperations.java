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
package org.apache.iceberg.rest;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.DefaultEncryptionManagerFactory;
import org.apache.iceberg.encryption.EncryptionManagerFactory;
import org.apache.iceberg.encryption.EnvelopeEncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.kms.UnitestKMS;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;

public class TestRESTTableOperations {

  private static final int PORT = 1080;
  private static final String URI = String.format("http://127.0.0.1:%d", PORT);
  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";
  private static final Schema TABLE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(2, "data", Types.StringType.get()));

  private static final Map<String, String> catalogProperties =
      ImmutableMap.of(CatalogProperties.ENCRYPTION_IGNORE_TABLE_PROPS, "false");
  private static ClientAndServer mockServer;
  private static RESTClient restClient;
  private static FileIO fileIO;
  private static EncryptionManagerFactory encryptionManagerFactory;

  @BeforeAll
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    encryptionManagerFactory = new DefaultEncryptionManagerFactory();
    encryptionManagerFactory.initialize(catalogProperties);
    mockServer = startClientAndServer(PORT);
    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();
    fileIO = CatalogUtil.loadFileIO(DEFAULT_FILE_IO_IMPL, catalogProperties, conf);
  }

  @AfterAll
  public static void stopServer() throws IOException {
    mockServer.stop();
    restClient.close();
    fileIO.close();
    encryptionManagerFactory.close();
  }

  @Test
  public void testCSESupport() {
    Namespace namespace = Namespace.of("encryptionManagerFact");
    TableIdentifier plainTextEncryptionManagerId =
        TableIdentifier.of(namespace, "PlaintextEncryptionManager");
    TableIdentifier envelopedEncryptionManagerId =
        TableIdentifier.of(namespace, "EnvelopedEncryptionManager");
    ResourcePaths resourcePaths = ResourcePaths.forCatalogProperties(catalogProperties);

    String noCSESupportTable = resourcePaths.table(plainTextEncryptionManagerId);
    String cseSupportTable = resourcePaths.table(envelopedEncryptionManagerId);

    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            TABLE_SCHEMA,
            PartitionSpec.unpartitioned(),
            "file://tmp/db/table",
            ImmutableMap.of(
                TableProperties.ENCRYPTION_KMS_CLIENT_IMPL,
                UnitestKMS.class.getCanonicalName(),
                TableProperties.ENCRYPTION_TABLE_KEY,
                UnitestKMS.MASTER_KEY_NAME1,
                TableProperties.FORMAT_VERSION,
                "2"));

    RESTTableOperations restTableOperationsNoCSESupport =
        new RESTTableOperations(
            restClient, noCSESupportTable, null, fileIO, encryptionManagerFactory, null);
    RESTTableOperations restTableOperationsWithCSESupport =
        new RESTTableOperations(
            restClient, cseSupportTable, null, fileIO, encryptionManagerFactory, tableMetadata);
    Assertions.assertThat(restTableOperationsNoCSESupport.encryption())
        .isInstanceOf(PlaintextEncryptionManager.class);
    Assertions.assertThat(restTableOperationsWithCSESupport.encryption())
        .isInstanceOf(EnvelopeEncryptionManager.class);
  }
}
