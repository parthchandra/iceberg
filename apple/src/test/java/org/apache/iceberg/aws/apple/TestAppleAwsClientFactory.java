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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

public class TestAppleAwsClientFactory extends SparkTestBase {

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
  @Before
  public void clearHadoopConf() {
    SparkSession.setActiveSession(spark);
    PATH_SET.keySet().forEach(spark.sparkContext().hadoopConfiguration()::unset);
    USER_SET.keySet().forEach(spark.sparkContext().hadoopConfiguration()::unset);
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

  @Test
  public void testSerialization() {
    Map<String, String> expectedConfig = USER_SET;

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(expectedConfig);

    AppleAwsClientFactory serializedFactory;

    try {
      serializedFactory = deserialize(serialize(factory), AppleAwsClientFactory.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Configuration actual = serializedFactory.config();

    expectedConfig.forEach((k, v) -> Assert.assertEquals("Missing config key", v, actual.get(k)));
  }

  @Test
  public void testLowerCasedParams() {
    Map<String, String> expectedConfig = USER_SET;

    Map<String, String> lowerCaseConfig =
        USER_SET.entrySet().stream()
            .collect(
                Collectors.toMap(k -> k.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(lowerCaseConfig);
    expectedConfig.forEach(
        (k, v) -> Assert.assertEquals("Missing config key", v, factory.config().get(k)));
  }

  @Test
  public void testDynamicCredentialProvider() {
    Map<String, String> expectedConfig = USER_SET;

    ImmutableList<String> classNames =
        ImmutableList.of(
            "com.apple.awsappleconnect.java.provider.STSAssumeRoleCredentialsProvider",
            "com.apple.awsappleconnect.java.provider.STSAssumeRoleCredentialsProvider",
            "com.apple.awsappleconnect.java.provider.AWSAppleConnectCredentialsProvider");

    classNames.forEach(
        className -> {
          AppleAwsClientFactory factory = new AppleAwsClientFactory();
          Map<String, String> conf = Maps.newHashMap(expectedConfig);
          conf.put("fs.s3a.aws.credentials.provider", className);
          factory.initialize(conf);
          String providerName =
              factory.getCredentialProviderConstructor().getConstructedClass().getName();
          Assert.assertEquals(className, providerName);
        });
  }

  @Test
  public void testMissingParameters() {
    AppleAwsClientFactory factory = new AppleAwsClientFactory();

    // Should work with User Config Set
    factory.initialize(USER_SET);

    // Should work with identity Config set
    factory.initialize(PATH_SET);

    AssertHelpers.assertThrows(
        "Should report missing properties",
        IllegalArgumentException.class,
        "Cannot initialize AppleAwsClientFactory missing properties:",
        () -> factory.initialize(Collections.emptyMap()));
  }

  @Test
  public void testGetConfigFromSparkEnv() {
    SparkSession freshSession = spark.newSession();
    USER_SET.entrySet().forEach(kv -> freshSession.conf().set(kv.getKey(), kv.getValue()));
    SparkSession.setActiveSession(freshSession);

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(Collections.emptyMap());

    USER_SET.forEach(
        (k, v) -> Assert.assertEquals("Missing config key", v, factory.config().get(k)));
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

    USER_SET.forEach(
        (k, v) -> Assert.assertEquals("Missing config key", v, factory.config().get(k)));
  }

  @Test
  public void testProxyConfig() throws IOException {
    SparkSession freshSession = spark.newSession();
    USER_SET.entrySet().forEach(kv -> freshSession.conf().set(kv.getKey(), kv.getValue()));

    freshSession.conf().set(AppleAwsClientFactory.AWS_REGION, "us-west-2");
    freshSession.conf().set(AppleAwsClientFactory.PROXY_HOST, "my.proxy.com");
    freshSession.conf().set(AppleAwsClientFactory.PROXY_PORT, 10);

    SparkSession.setActiveSession(freshSession);
    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(Collections.emptyMap());
    ProxyConfiguration proxy = factory.getProxy();

    Assert.assertEquals("my.proxy.com", proxy.host());
    Assert.assertEquals(10, proxy.port());
  }
}
