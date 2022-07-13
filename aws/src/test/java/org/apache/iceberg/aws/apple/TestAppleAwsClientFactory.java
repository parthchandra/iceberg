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

import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.utils.ImmutableMap;

public class TestAppleAwsClientFactory {

  @Test
  public void testSerialization() {
    Map<String, String> expectedConfig =
        ImmutableMap.<String, String>builder()
            .put("fs.s3a.appleconnect.username", "JAppleseed")
            .put("fs.s3a.appleconnect.password", "tree")
            .put("fs.s3a.appleconnect.totpSecret", "topttree")
            .put("fs.s3a.appleconnect.deviceId", "treehouse")
            .put("fs.s3a.appleconnect.awsrole", "treescout")
            .put("fs.s3a.appleconnect.accountId", "treeid")
            .build();

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(expectedConfig);

    AppleAwsClientFactory serializedFactory =
        SerializationUtils.deserialize(SerializationUtils.serialize(factory));
    Configuration actual = serializedFactory.config();

    expectedConfig.forEach((k, v) -> Assert.assertEquals("Missing config key", v, actual.get(k)));
  }

  @Test
  public void testLowerCasedParams() {
    Map<String, String> expectedConfig =
        ImmutableMap.<String, String>builder()
            .put("fs.s3a.appleconnect.username", "JAppleseed")
            .put("fs.s3a.appleconnect.password", "tree")
            .put("fs.s3a.appleconnect.totpSecret", "topttree")
            .put("fs.s3a.appleconnect.deviceId", "treehouse")
            .put("fs.s3a.appleconnect.awsrole", "treescout")
            .put("fs.s3a.appleconnect.accountId", "treeid")
            .build();

    Map<String, String> lowerCaseConfig =
        ImmutableMap.<String, String>builder()
            .put("fs.s3a.appleconnect.username", "JAppleseed")
            .put("fs.s3a.appleconnect.password", "tree")
            .put("fs.s3a.appleconnect.totpsecret", "topttree")
            .put("fs.s3a.appleconnect.deviceid", "treehouse")
            .put("fs.s3a.appleconnect.awsrole", "treescout")
            .put("fs.s3a.appleconnect.accountId", "treeid")
            .build();

    AppleAwsClientFactory factory = new AppleAwsClientFactory();
    factory.initialize(lowerCaseConfig);
    expectedConfig.forEach(
        (k, v) -> Assert.assertEquals("Missing config key", v, factory.config().get(k)));
  }
}
