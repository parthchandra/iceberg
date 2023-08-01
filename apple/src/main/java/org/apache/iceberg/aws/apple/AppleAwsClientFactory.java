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

import com.apple.awsappleconnect.java.credentials.AWSAppleCredentials;
import com.apple.awsappleconnect.java.credentials.AWSAppleCredentialsProviderBase;
import com.apple.awsappleconnect.java.provider.AWSAppleConnectCredentialsProvider;
import com.apple.awsappleconnect.java.service.CredentialsConstants;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * A Spark-Iceberg Client Factory which uses the Apple Connect - Mascot authentication method
 * provided by {@link AWSAppleConnectCredentialsProvider}. Configured using Iceberg Catalog
 * properties with keys identical to those required for <a
 * href="https://github.pie.apple.com/pie-spark/awsappleconnect-spark-utils">hadoop
 * configuration</a>. Interprets properties from the Hadoop Configuration or Spark Configuration
 * (spark.hadoop.*)
 */
public class AppleAwsClientFactory implements AwsClientFactory, KryoSerializable {

  public static final String AWS_REGION = "aws_region";

  static final String S3A_PROXY_HOST = "fs.s3a.proxy.host";
  static final String S3A_PROXY_PORT = "fs.s3a.proxy.port";
  static final String S3A_PROXY_SECURED = "fs.s3a.proxy.ssl.enabled";
  static final String S3A_CREDENTIAL_PROVIDER = "fs.s3a.aws.credentials.provider";
  static final String S3A_STS_REGION = "fs.s3a.assumed.role.sts.endpoint.region";

  private static final String HTTP = "http://";
  private static final String HTTPS = "https://";

  private static final Logger LOG = LoggerFactory.getLogger(AppleAwsClientFactory.class);

  private static final List<Set<String>> REQUIRED_KEYS =
      ImmutableList.of(
          ImmutableSet.of(
              CredentialsConstants.AC_USERNAME_KEY,
              CredentialsConstants.AC_PASSWORD_KEY,
              CredentialsConstants.AC_TOTP_KEY,
              CredentialsConstants.AC_DEVICEID_KEY,
              CredentialsConstants.AC_AWS_ROLE_KEY,
              CredentialsConstants.AC_ACCOUNT_ID_KEY),
          ImmutableSet.of(
              CredentialsConstants.AC_IDENTITY_CERT_PATH_KEY,
              CredentialsConstants.AC_IDENTITY_CERT_KEY_PATH_KEY,
              CredentialsConstants.AC_AWS_ROLE_KEY,
              CredentialsConstants.AC_ACCOUNT_ID_KEY));

  private static final int PROXY_PORT_DEFAULT = 443;
  private static final boolean PROXY_SECURED_DEFAULT = false;

  // This will be serialized to executors in spark so the provider will need to be reinitialized
  private transient AwsCredentialsProvider lazyProvider;

  private transient SdkHttpClient lazyHttpClient;

  private SerializableConfiguration lazyConfig;
  private Map<String, String> properties;

  @VisibleForTesting
  Configuration config() {
    if (lazyConfig == null) {
      Configuration conf = SparkSession.active().sessionState().newHadoopConf();
      properties.entrySet().forEach(entry -> conf.set(entry.getKey(), entry.getValue()));
      lazyConfig = new SerializableConfiguration(conf);
    }

    return lazyConfig.get();
  }

  @VisibleForTesting
  DynConstructors.Ctor<AWSAppleCredentialsProviderBase> getCredentialProviderConstructor() {
    String credentialProviderClassName =
        config().get(S3A_CREDENTIAL_PROVIDER, AWSAppleConnectCredentialsProvider.class.getName());

    LOG.info("Setting up S3FileIO with CredentialProvider {}", credentialProviderClassName);
    return DynConstructors.builder(AWSAppleCredentialsProviderBase.class)
        .impl(credentialProviderClassName, Configuration.class)
        .build();
  }

  private synchronized AwsCredentialsProvider provider() {
    if (lazyProvider != null) {
      return lazyProvider;
    }

    try {
      DynConstructors.Ctor<AWSAppleCredentialsProviderBase> ctor =
          getCredentialProviderConstructor();
      AWSAppleCredentialsProviderBase provider = ctor.invoke(null, config());
      lazyProvider = new CredentialProviderWrapper(provider);
    } catch (Exception e) {
      throw new RuntimeException("Could not create Apple Aws Credentials Cache Provider", e);
    }

    return lazyProvider;
  }

  private <T extends AwsClientBuilder> T maybeApplyRegion(T builder) {
    String region = config().get(AWS_REGION, config().get(S3A_STS_REGION));

    if (region != null) {
      return (T) builder.region(Region.of(region));
    }
    return builder;
  }

  @VisibleForTesting
  ProxyConfiguration getProxy() {
    String proxyHost = config().get(S3A_PROXY_HOST);

    if (proxyHost != null) {
      int proxyPort = config().getInt(S3A_PROXY_PORT, PROXY_PORT_DEFAULT);

      boolean proxySecured = config().getBoolean(S3A_PROXY_SECURED, PROXY_SECURED_DEFAULT);

      String scheme = proxySecured ? HTTPS : HTTP;

      // ProxyConfig will ignore everything but host and port
      return ProxyConfiguration.builder()
          .endpoint(URI.create(scheme + proxyHost + ":" + proxyPort))
          .build();
    }
    return null;
  }

  synchronized SdkHttpClient httpClient() {
    if (lazyHttpClient == null) {
      // TODO Support more clients here
      ApacheHttpClient.Builder clientBuilder = ApacheHttpClient.builder();

      ProxyConfiguration proxy = getProxy();
      if (proxy != null) {
        clientBuilder = clientBuilder.proxyConfiguration(proxy);
      }

      lazyHttpClient = clientBuilder.build();
    }
    return lazyHttpClient;
  }

  @Override
  public S3Client s3() {
    return maybeApplyRegion(S3Client.builder())
        .httpClient(httpClient())
        .credentialsProvider(provider())
        .build();
  }

  @Override
  public GlueClient glue() {
    return maybeApplyRegion(GlueClient.builder())
        .httpClient(httpClient())
        .credentialsProvider(provider())
        .build();
  }

  @Override
  public KmsClient kms() {
    return maybeApplyRegion(KmsClient.builder())
        .httpClient(httpClient())
        .credentialsProvider(provider())
        .build();
  }

  @Override
  public DynamoDbClient dynamo() {
    return maybeApplyRegion(DynamoDbClient.builder())
        .httpClient(httpClient())
        .credentialsProvider(provider())
        .build();
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = props;

    List<List<String>> missingRequired =
        REQUIRED_KEYS.stream()
            .map(
                keyset ->
                    keyset.stream()
                        .filter(key -> config().get(key) == null)
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());

    boolean requirementsMet = missingRequired.stream().anyMatch(l -> l.size() == 0);

    Preconditions.checkArgument(
        requirementsMet,
        String.format(
            "Cannot initialize AppleAwsClientFactory missing properties:\n%s",
            missingRequired.stream().map(List::toString).collect(Collectors.joining("\n OR \n"))));
  }

  @Override
  public void write(Kryo kryo, Output output) {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(byteOutputStream);

    try {
      config().write(outputStream);
      outputStream.close();
    } catch (IOException e) {
      throw new RuntimeException("Could not Kryo Serialize Configuration", e);
    }

    kryo.writeObject(output, byteOutputStream.toByteArray());
    kryo.writeClassAndObject(output, Maps.newHashMap(properties));
  }

  @Override
  public void read(Kryo kryo, Input input) {
    byte[] inputData = kryo.readObject(input, byte[].class);
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(inputData));

    Configuration conf = new Configuration(false);
    try {
      conf.readFields(inputStream);
      inputStream.close();
    } catch (IOException e) {
      throw new RuntimeException("Could not Kryo Serialize Configuration", e);
    }

    this.lazyConfig = new SerializableConfiguration(conf);
    this.properties = (Map<String, String>) kryo.readClassAndObject(input);
  }

  static class CredentialProviderWrapper implements AwsCredentialsProvider {
    private final AWSAppleCredentialsProviderBase v1Provider;

    CredentialProviderWrapper(AWSAppleCredentialsProviderBase v1Provider) {
      this.v1Provider = v1Provider;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      AWSAppleCredentials credentials = (AWSAppleCredentials) v1Provider.getCredentials();
      return AwsSessionCredentials.create(
          credentials.getAWSAccessKeyId(),
          credentials.getAWSSecretKey(),
          credentials.getSessionToken());
    }
  }
}
