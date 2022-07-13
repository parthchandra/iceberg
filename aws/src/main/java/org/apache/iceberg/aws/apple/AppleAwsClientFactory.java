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
import com.apple.awsappleconnect.java.provider.CachedAWSAppleConnectCredentialsProvider;
import com.apple.awsappleconnect.java.provider.STSAssumeRoleFromCredentialsCacheProvider;
import com.apple.awsappleconnect.java.service.CredentialsConstants;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * A Spark-Iceberg Client Factory which uses the Apple Connect - Mascot authentication method provided by
 * {@link STSAssumeRoleFromCredentialsCacheProvider}.
 * Configured using Iceberg Catalog properties with keys identical to those required for
 * <a href="https://github.pie.apple.com/pie-spark/awsappleconnect-spark-utils"> hadoop configuration</a>
 */
public class AppleAwsClientFactory implements AwsClientFactory {

  private static final Map<String, String> constantsKeyMap = ImmutableList.of(
      CredentialsConstants.AC_USERNAME_KEY,
      CredentialsConstants.AC_PASSWORD_KEY,
      CredentialsConstants.AC_TOTP_KEY,
      CredentialsConstants.AC_DEVICEID_KEY,
      CredentialsConstants.AC_AWS_ROLE_KEY,
      CredentialsConstants.AC_ACCOUNT_ID_KEY,
      CredentialsConstants.AC_CREDENTIALS_EARLYEXPIRYDELTA_KEY,
      CredentialsConstants.AC_CACHESERVER_HOST_KEY,
      CredentialsConstants.AC_CACHESERVER_PORT_KEY,
      CredentialsConstants.AC_TLS_CA_CHAIN_PATH_KEY,
      CredentialsConstants.AC_TLS_CERT_PATH_KEY,
      CredentialsConstants.AC_TLS_PVTKEY_PATH_KEY,
      CredentialsConstants.AC_TLS_CA_CHAIN_KEY,
      CredentialsConstants.AC_TLS_CERT_KEY,
      CredentialsConstants.AC_TLS_PVTKEY_KEY,
      CredentialsConstants.AC_TLS_GROUPID_KEY,
      CredentialsConstants.AC_TLS_CERT_VALIDITY_KEY,
      CredentialsConstants.S3_PROXY_HOST,
      CredentialsConstants.S3_PROXY_PORT,
      CredentialsConstants.S3_CONNECTION_SSL_ENABLED,
      CredentialsConstants.AC_CREDENTIALS_MAX_TRY_ATTEMPTS,
      CredentialsConstants.ASSUMED_ROLE_STS_ENDPOINT_REGION_KEY,
      CredentialsConstants.ASSUMED_ROLE_ARN_KEY
  ).stream().collect(Collectors.toMap(k -> k.toLowerCase(Locale.ROOT), Function.identity()));

  private static final List<String> REQUIRED_KEYS = Arrays.asList(
      CredentialsConstants.AC_USERNAME_KEY,
      CredentialsConstants.AC_PASSWORD_KEY,
      CredentialsConstants.AC_TOTP_KEY,
      CredentialsConstants.AC_DEVICEID_KEY,
      CredentialsConstants.AC_AWS_ROLE_KEY,
      CredentialsConstants.AC_ACCOUNT_ID_KEY);

  // This will be serialized to executors in spark so the provider will need to be reinitialized
  private transient AwsCredentialsProvider lazyProvider;

  // Keep the configuration for generating new credential providers
  private SerializableConfiguration configuration;

  Configuration config() {
    return configuration.get();
  }

  private synchronized AwsCredentialsProvider provider() {
    if (lazyProvider != null) {
      return lazyProvider;
    }

    Configuration conf = configuration.get();
    AWSAppleCredentialsProviderBase credentialProvider;

    try {
      if (conf.get(CredentialsConstants.ASSUMED_ROLE_ARN_KEY) != null) {
        credentialProvider = new STSAssumeRoleFromCredentialsCacheProvider(configuration.get());
      } else {
        credentialProvider = new CachedAWSAppleConnectCredentialsProvider(configuration.get());
      }
      lazyProvider = new CredentialProviderWrapper(credentialProvider);
    } catch (Exception e) {
      throw new RuntimeException("Could not create Apple Aws Credentials Cache Provider", e);
    }

    return lazyProvider;
  }

  private <T extends AwsClientBuilder> T maybeApplyRegion(T builder) {
    String region = config().get(CredentialsConstants.ASSUMED_ROLE_STS_ENDPOINT_REGION_KEY);
    if (region == null) {
      config().get("aws-region");
    }

    if (region != null) {
      return (T) builder.region(Region.of(region));
    }
    return builder;
  }

  @Override
  public S3Client s3() {
    return maybeApplyRegion(S3Client.builder()).credentialsProvider(provider()).build();
  }

  @Override
  public GlueClient glue() {
    return maybeApplyRegion(GlueClient.builder()).credentialsProvider(provider()).build();
  }

  @Override
  public KmsClient kms() {
    return maybeApplyRegion(KmsClient.builder()).credentialsProvider(provider()).build();
  }

  @Override
  public DynamoDbClient dynamo() {
    return maybeApplyRegion(DynamoDbClient.builder()).credentialsProvider(provider()).build();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    Configuration newConfig = new Configuration();

    // The Catalog passes through case-insensitive properties which are lower cased here, we need to revert them
    properties.forEach((k, v) -> newConfig.set(constantsKeyMap.getOrDefault(k, k), v));

    List<String> missingRequired = REQUIRED_KEYS.stream()
        .filter(k -> newConfig.get(k) == null)
        .collect(Collectors.toList());

    Preconditions.checkArgument(missingRequired.isEmpty(),
        String.format("Cannot initialize AppleAwsClientFactory missing properties %s", missingRequired));

    configuration = new SerializableConfiguration(newConfig);
  }

  class CredentialProviderWrapper implements AwsCredentialsProvider {
    private final AWSAppleCredentialsProviderBase v1Provider;

    CredentialProviderWrapper(AWSAppleCredentialsProviderBase v1Provider) {
      this.v1Provider = v1Provider;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      AWSAppleCredentials credentials = (AWSAppleCredentials) v1Provider.getCredentials();
      return AwsSessionCredentials.create(credentials.getAWSAccessKeyId(),
          credentials.getAWSSecretKey(), credentials.getSessionToken());
    }
  }
}
