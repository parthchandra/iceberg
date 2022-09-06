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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.KmsClient;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/**
 * Typically, KMS systems support in-server key wrapping. Their clients should implement KmsClient
 * interface directly. An extension of the LocalKeyWrapClient class should be used only in
 * situations where in-server wrapping is not supported. The wrapping will be done locally then -
 * the master keys will be fetched from the KMS server via the getMasterKey functions, and used to
 * encrypt a DEK or KEK by AES GCM cipher.
 *
 * <p>Master keys have a version, updated upon key rotation in KMS. In the read path, the master
 * keys are fetched by the key ID and version, and cached locally in this class - so they can be
 * re-used for reading different files without additional KMS interactions. In the write path, the
 * master keys are fetched by the key ID only - the assumption is that KMS will send the current
 * (latest) version of the key; therefore the keys are not cached in this class - writing new files
 * will be done with the latest key version, sent from the KMS. However, the classes that extend the
 * LocalKeyWrapClient class, can decide to cache the master keys in the write path, and handle key
 * rotation manually.
 */
public abstract class LocalKeyWrapClient implements KmsClient {

  private static final String LOCAL_WRAP_TYPE_FIELD = "localWrappingType";
  private static final String LOCAL_WRAP_TYPE1 = "LKW1";
  private static final String LOCAL_WRAP_MASTER_KEY_VERSION_FIELD = "masterKeyVersion";
  private static final String LOCAL_WRAP_ENCRYPTED_KEY_FIELD = "encryptedKey";

  public static class MasterKeyWithVersion {
    private final byte[] masterKey;
    private final String masterKeyVersion;

    public MasterKeyWithVersion(byte[] masterKey, String masterKeyVersion) {
      this.masterKey = masterKey;
      this.masterKeyVersion = masterKeyVersion;
    }

    private byte[] getKey() {
      return masterKey;
    }

    private String getVersion() {
      return masterKeyVersion;
    }
  }

  /**
   * KMS systems wrap keys by encrypting them by master keys, and attaching additional information
   * (such as the version number of the masker key) to the result of encryption. The master key
   * version is required for key rotation.
   *
   * <p>LocalKeyWrapClient class writes (and reads) the "key wrap information" as a flat json with
   * the following fields: 1. "localWrappingType" - a String, with the type of key material. In the
   * current version, only one value is allowed - "LKW1" (stands for "local key wrapping, version
   * 1") 2. "masterKeyVersion" - a String, with the master key version. 3. "encryptedKey" - a
   * String, with the key encrypted by the master key (base64-encoded).
   */
  private static class LocalKeyWrap {
    private final String encryptedEncodedKey;
    private final String masterKeyVersion;

    private LocalKeyWrap(String masterKeyVersion, String encryptedEncodedKey) {
      this.masterKeyVersion = masterKeyVersion;
      this.encryptedEncodedKey = encryptedEncodedKey;
    }

    private static LocalKeyWrap parse(String wrappedKey) {
      JsonNode json;
      try {
        json =
            JsonUtil.mapper()
                .readValue(wrappedKey.getBytes(StandardCharsets.UTF_8), JsonNode.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      String localWrappingType = JsonUtil.getString(LOCAL_WRAP_TYPE_FIELD, json);
      if (!LOCAL_WRAP_TYPE1.equals(localWrappingType)) {
        throw new RuntimeException(
            "Can't parse unsupported localWrappingType: " + localWrappingType);
      }

      String masterKeyVersion = JsonUtil.getString(LOCAL_WRAP_MASTER_KEY_VERSION_FIELD, json);
      String encryptedEncodedKey = JsonUtil.getString(LOCAL_WRAP_ENCRYPTED_KEY_FIELD, json);

      return new LocalKeyWrap(masterKeyVersion, encryptedEncodedKey);
    }

    private String getMasterKeyVersion() {
      return masterKeyVersion;
    }

    private String getEncryptedKey() {
      return encryptedEncodedKey;
    }
  }

  @Override
  public String wrapKey(ByteBuffer keyBuffer, String wrappingKeyId) {
    byte[] key = keyBuffer.array();
    MasterKeyWithVersion masterKeyLastVersion = getLatestMasterKey(wrappingKeyId);
    checkMasterKeyLength(
        masterKeyLastVersion.getKey().length, wrappingKeyId, masterKeyLastVersion.getVersion());
    Ciphers.AesGcmEncryptor keyEncryptor =
        new Ciphers.AesGcmEncryptor(masterKeyLastVersion.getKey());
    String encryptedEncodedKey =
        Base64.getEncoder().encodeToString(keyEncryptor.encrypt(key, null));
    try {
      return toJson(masterKeyLastVersion.getVersion(), encryptedEncodedKey);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to wrap with keyID " + wrappingKeyId, e);
    }
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    LocalKeyWrap keyWrap = LocalKeyWrap.parse(wrappedKey);
    String encryptedEncodedKey = keyWrap.getEncryptedKey();
    byte[] masterKey = getMasterKey(wrappingKeyId, keyWrap.getMasterKeyVersion());
    checkMasterKeyLength(masterKey.length, wrappingKeyId, keyWrap.getMasterKeyVersion());
    Ciphers.AesGcmDecryptor keyDecryptor = new Ciphers.AesGcmDecryptor(masterKey);
    byte[] encryptedKey = Base64.getDecoder().decode(encryptedEncodedKey);
    return ByteBuffer.wrap(keyDecryptor.decrypt(encryptedKey, null));
  }

  private void checkMasterKeyLength(int keyLength, String keyID, String keyVersion) {
    Preconditions.checkArgument(
        (16 == keyLength || 24 == keyLength || 32 == keyLength),
        "Got key with wrong length: "
            + keyLength
            + " (must be 16,24 or 32). Key ID: "
            + keyID
            + ", version: "
            + keyVersion);
  }

  private static String toJson(String masterKeyVersion, String encryptedKey) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    JsonGenerator generator = JsonUtil.factory().createGenerator(stream);
    generator.writeStartObject();
    generator.writeStringField(LOCAL_WRAP_TYPE_FIELD, LOCAL_WRAP_TYPE1);
    generator.writeStringField(LOCAL_WRAP_MASTER_KEY_VERSION_FIELD, masterKeyVersion);
    generator.writeStringField(LOCAL_WRAP_ENCRYPTED_KEY_FIELD, encryptedKey);
    generator.writeEndObject();
    generator.flush();
    return stream.toString("UTF-8");
  }

  /** Write path: Get the current (latest) version of the master key */
  protected abstract MasterKeyWithVersion getLatestMasterKey(String masterKeyIdentifier);

  /** Read path: Get master key with a given version */
  protected abstract byte[] getMasterKey(String masterKeyIdentifier, String masterKeyVersion);
}
