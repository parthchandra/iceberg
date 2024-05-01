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
package org.apache.iceberg.aws.apple;

import static org.apache.iceberg.aws.apple.AppleAwsClientFactory.AWS_REGION;
import static org.apache.iceberg.aws.apple.AppleAwsClientFactory.S3A_PROXY_HOST;
import static org.apache.iceberg.aws.apple.AppleAwsClientFactory.S3A_PROXY_PORT;
import static org.apache.iceberg.aws.apple.AppleAwsClientFactory.S3A_PROXY_SECURED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;

public class TestAppleAwsClientFactory extends TestBase {

  private static final String REGION = "us-west-1";
  private static final Map<String, String> PROPERTIES =
      ImmutableMap.<String, String>builder().put(AWS_REGION, REGION).build();

  private static final Map<String, String> PROXY_CONFIG =
      ImmutableMap.<String, String>builder()
          .put(S3A_PROXY_HOST, "my.proxy.com")
          .put(S3A_PROXY_PORT, "10")
          .build();

  private static final Map<String, String> PROXY_SECURED_CONFIG =
      ImmutableMap.<String, String>builder()
          .put(S3A_PROXY_HOST, "my.proxy.com")
          .put(S3A_PROXY_PORT, "10")
          .put(S3A_PROXY_SECURED, "true")
          .build();

  private static final Map<String, String> LATE_PROPERTY =
      ImmutableMap.<String, String>builder().put("delegatetoken", "newvalue").build();

  private static final Map<String, String> USER_SET =
      ImmutableMap.<String, String>builder()
          .put("fs.s3a.appleconnect.username", "JAppleseed")
          .put("fs.s3a.appleconnect.password", "tree")
          .put("fs.s3a.appleconnect.totpSecret", "topttree")
          .put("fs.s3a.appleconnect.deviceId", "treehouse")
          .put("fs.s3a.appleconnect.awsrole", "treescout")
          .put("fs.s3a.appleconnect.accountId", "treeid")
          .build();

  private static final Map<String, String> PATH_SET =
      ImmutableMap.<String, String>builder()
          .put("fs.s3a.appleconnect.identityCertPath", "JAppleseed")
          .put("fs.s3a.appleconnect.identityCertKeyPath", "tree")
          .put("fs.s3a.appleconnect.awsrole", "treescout")
          .put("fs.s3a.appleconnect.accountId", "treeid")
          .build();

  @SuppressWarnings("RegexpSingleline")
  @BeforeEach
  public void clearHadoopConf() {
    SparkSession.setActiveSession(spark);
    PATH_SET.keySet().forEach(spark.sparkContext().hadoopConfiguration()::unset);
    USER_SET.keySet().forEach(spark.sparkContext().hadoopConfiguration()::unset);
    PROXY_CONFIG.keySet().forEach(spark.sparkContext().hadoopConfiguration()::unset);
    LATE_PROPERTY.keySet().forEach(spark.sparkContext().hadoopConfiguration()::unset);
  }

  @SuppressWarnings("checkstyle:RegexpSingleline")
  private static void setHadoopConf(Map<String, String> conf) {
    conf.entrySet()
        .forEach(
            entry ->
                spark.sparkContext().hadoopConfiguration().set(entry.getKey(), entry.getValue()));
  }

  private static <T extends Serializable> byte[] serializeKryo(T obj, Kryo kryo) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Output output = new Output(byteArrayOutputStream);
    kryo.writeObject(output, obj);
    output.close();
    return byteArrayOutputStream.toByteArray();
  }

  private static <T extends Serializable> byte[] serialize(T obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return baos.toByteArray();
  }

  private static <T extends Serializable> T deserialize(byte[] bytes, Class<T> classTarget)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    Object obj = ois.readObject();
    return classTarget.cast(obj);
  }

  private static <T extends Serializable> T deserializeKryo(
      byte[] bytes, Class<T> classTarget, Kryo kryo) {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    Input input = new Input(byteArrayInputStream);
    T deserializedObject = kryo.readObject(input, classTarget);
    input.close();
    return deserializedObject;
  }

  @Test
  @SuppressWarnings("checkstyle:RegexpSingleline")
  public void testSerialization() {
    SparkSession activeSession = spark.newSession();
    SparkSession.setActiveSession(activeSession);
    PROXY_CONFIG
        .entrySet()
        .forEach(entry -> activeSession.conf().set(entry.getKey(), entry.getValue()));
    setHadoopConf(USER_SET);
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(PROPERTIES);

    AppleAwsClientFactory serializedFactory;

    try {
      serializedFactory = deserialize(serialize(factory), AppleAwsClientFactory.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertThat(serializedFactory.config())
        .isNotSameAs(activeSession.sparkContext().hadoopConfiguration());
    assertThat(serializedFactory.config()).containsAll(USER_SET.entrySet());
    assertThat(serializedFactory.config()).containsAll(PROPERTIES.entrySet());
    assertThat(serializedFactory.config()).containsAll(PROXY_CONFIG.entrySet());
  }

  @Test
  @SuppressWarnings("checkstyle:RegexpSingleline")
  public void testSerializationKryo() {
    SparkSession activeSession = spark.newSession();
    SparkSession.setActiveSession(activeSession);
    PROXY_CONFIG
        .entrySet()
        .forEach(entry -> activeSession.conf().set(entry.getKey(), entry.getValue()));
    setHadoopConf(USER_SET);
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(PROPERTIES);

    AppleAwsClientFactory serializedFactory;

    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    try {
      serializedFactory =
          deserializeKryo(serializeKryo(factory, kryo), AppleAwsClientFactory.class, kryo);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertThat(serializedFactory.config()).isNotSameAs(spark.sparkContext().hadoopConfiguration());
    assertThat(serializedFactory.config()).containsAll(USER_SET.entrySet());
    assertThat(serializedFactory.config()).containsAll(PROPERTIES.entrySet());
    assertThat(serializedFactory.config()).containsAll(PROXY_CONFIG.entrySet());
  }

  @Test
  public void testDynamicCredentialProvider() {
    ImmutableList<String> classNames =
        ImmutableList.of(
            "com.apple.awsappleconnect.java.provider.CachedAWSAppleConnectCredentialsProvider",
            "com.apple.awsappleconnect.java.provider.STSAssumeRoleFromCredentialsCacheProvider",
            "com.apple.awsappleconnect.java.provider.STSAssumeRoleCredentialsProvider",
            "com.apple.awsappleconnect.java.provider.AWSAppleConnectCredentialsProvider");

    classNames.forEach(
        className -> {
          AppleAwsClientFactory factory = new AppleAwsClientFactory();
          Map<String, String> conf = Maps.newHashMap(USER_SET);
          conf.put("fs.s3a.aws.credentials.provider", className);
          setHadoopConf(conf);
          factory.initialize(conf);

          try {
            Object instance = factory.getCredentialProviderConstructor().newInstance();
            assertThat(instance.getClass()).isAssignableFrom(Class.forName(className));
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testMissingParameters() {
    AppleAwsClientFactory factory = new AppleAwsClientFactory();

    // Should work with User Config Set
    setHadoopConf(USER_SET);
    factory.initialize(Collections.emptyMap());
    clearHadoopConf();

    factory = new AppleAwsClientFactory();

    // Should work with identity Config set
    setHadoopConf(PATH_SET);
    factory.initialize(PATH_SET);
    clearHadoopConf();

    assertThatThrownBy(
            () -> new AppleAwsClientFactory().initialize(Collections.emptyMap()),
            "Should report missing properties")
        .hasMessageContaining("Cannot initialize AppleAwsClientFactory missing properties:");
  }

  @SuppressWarnings("RegexpSingleline")
  @Test
  public void testGetConfigFromSparkHadoopEnv() {
    SparkSession freshSession = spark.newSession();
    Configuration conf = sparkContext.hadoopConfiguration();
    USER_SET.entrySet().forEach(kv -> conf.set(kv.getKey(), kv.getValue()));
    SparkSession.setActiveSession(freshSession);

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(Collections.emptyMap());

    assertThat(factory.config()).containsAll(USER_SET.entrySet());
  }

  @Test
  public void testUnsecuredProxyConfig() {
    setHadoopConf(USER_SET);
    setHadoopConf(PROXY_CONFIG);

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(Collections.emptyMap());
    ProxyConfiguration proxy = factory.getProxy();

    assertThat(proxy.host()).isEqualTo("my.proxy.com");
    assertThat(proxy.port()).isEqualTo(10);
    assertThat(proxy.scheme()).contains("http");
  }

  @Test
  public void testSecuredProxyConfig() {
    setHadoopConf(USER_SET);
    setHadoopConf(PROXY_SECURED_CONFIG);

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(Collections.emptyMap());
    ProxyConfiguration proxy = factory.getProxy();

    assertThat(proxy.host()).isEqualTo("my.proxy.com");
    assertThat(proxy.port()).isEqualTo(10);
    assertThat(proxy.scheme()).contains("https");
  }

  @Test
  public void testS3AsyncClient() {
    setHadoopConf(USER_SET);

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(PROPERTIES);

    assertThat(factory.s3Async()).isNotNull();
  }

  @Test
  public void testSignerOverride() {
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    setHadoopConf(USER_SET);
    Map<String, String> properties = Maps.newHashMap(PROPERTIES);
    properties.put(S3FileIOProperties.ENDPOINT, "https://unknown:1234");
    properties.put(
        S3FileIOProperties.S3_CUSTOM_SIGNERS,
        String.format("MySigner:%s", TestSigner.class.getName()));
    properties.put(S3FileIOProperties.S3_SIGNING_ALGORITHM, "MySigner");
    factory.initialize(properties);
    S3Client s3Client = factory.s3();
    S3ServiceClientConfiguration s3ServiceClientConfiguration =
        s3Client.serviceClientConfiguration();
    ClientOverrideConfiguration clientOverrideConfiguration =
        s3ServiceClientConfiguration.overrideConfiguration();
    Optional<Signer> signer =
        clientOverrideConfiguration.advancedOption(SdkAdvancedClientOption.SIGNER);
    assertThat(signer.isPresent()).as("Signer Option").isTrue();
    assertThat(signer.get() instanceof TestSigner).as("Signer is TestSigner").isTrue();
  }

  @Test
  public void testSignerOverrideList() {
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    setHadoopConf(USER_SET);
    Map<String, String> properties = Maps.newHashMap(PROPERTIES);
    properties.put(S3FileIOProperties.ENDPOINT, "https://unknown:1234");
    properties.put(
        S3FileIOProperties.S3_CUSTOM_SIGNERS,
        String.format("T1:dummy, T2:%s, T3:dummy", TestSigner.class.getName()));
    properties.put(S3FileIOProperties.S3_SIGNING_ALGORITHM, "T2");
    factory.initialize(properties);
    S3Client s3Client = factory.s3();
    S3ServiceClientConfiguration s3ServiceClientConfiguration =
        s3Client.serviceClientConfiguration();
    ClientOverrideConfiguration clientOverrideConfiguration =
        s3ServiceClientConfiguration.overrideConfiguration();
    Optional<Signer> signer =
        clientOverrideConfiguration.advancedOption(SdkAdvancedClientOption.SIGNER);
    assertThat(signer.isPresent()).as("Signer Option").isTrue();
    assertThat(signer.get() instanceof TestSigner).as("Signer is TestSigner").isTrue();
  }

  @Test
  public void testSignerNegative() {
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    setHadoopConf(USER_SET);
    Map<String, String> properties = Maps.newHashMap(PROPERTIES);
    properties.put(S3FileIOProperties.ENDPOINT, "https://unknown:1234");
    properties.put(
        S3FileIOProperties.S3_CUSTOM_SIGNERS,
        String.format("T1:dummy, T2:%s, T3:dummy", TestSigner.class.getName()));
    factory.initialize(properties);

    assertThatThrownBy(factory::s3)
        .hasMessageContaining(
            "fs.s3a.signing-algorithm must be specified along with fs.s3a.custom.signers");
  }

  @Test
  public void testSignerNegative2() {
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    setHadoopConf(USER_SET);
    Map<String, String> properties = Maps.newHashMap(PROPERTIES);
    properties.put(S3FileIOProperties.ENDPOINT, "https://unknown:1234");
    properties.put(S3FileIOProperties.S3_CUSTOM_SIGNERS, TestSigner.class.getName());
    properties.put(S3FileIOProperties.S3_SIGNING_ALGORITHM, "blah");
    factory.initialize(properties);

    assertThatThrownBy(factory::s3)
        .hasMessageContaining("Invalid format (Expected name, name:SignerClass) for CustomSigner");
  }
}
