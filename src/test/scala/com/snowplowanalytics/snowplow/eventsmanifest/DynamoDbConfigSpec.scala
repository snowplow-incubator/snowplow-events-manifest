/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.eventsmanifest

// Commons
import org.apache.commons.codec.binary.Base64

// Specs2
import org.specs2.mutable.Specification

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.DynamoDbConfig.CredentialsAuth

import SpecHelpers._

class DynamoDbConfigSpec extends Specification {
  lazy val goodConfig: String = {
    val jsonConfig =
      s"""|{
          |  "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
          |  "data": {
          |    "name": "local",
          |    "auth": {
          |      "accessKeyId": "fakeAccessKeyId",
          |      "secretAccessKey": "fakeSecretAccessKey"
          |    },
          |    "awsRegion": "us-west-1",
          |    "dynamodbTable": "snowplow-integration-test-crossbatch-dedupe",
          |    "id": "56799a26-980c-4148-8bd9-c021b988c669",
          |    "purpose": "EVENTS_MANIFEST"
          |  }
          |}""".stripMargin
    val encoder = new Base64(true)
    new String(encoder.encode(jsonConfig.replaceAll("[\n\r]", "").getBytes()))
  }

  lazy val missingCredentialsConfig: String = {
    val jsonConfig =
      s"""|{
          |  "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
          |  "data": {
          |    "name": "local",
          |    "auth": null,
          |    "awsRegion": "us-west-1",
          |    "dynamodbTable": "snowplow-integration-test-crossbatch-dedupe",
          |    "id": "56799a26-980c-4148-8bd9-c021b988c669",
          |    "purpose": "EVENTS_MANIFEST"
          |  }
          |}""".stripMargin
    val encoder = new Base64(true)
    new String(encoder.encode(jsonConfig.replaceAll("[\n\r]", "").getBytes()))
  }

  "DynamoDbConfig constructors" should {
    val goodConfigExpected = DynamoDbConfig("local", Some(CredentialsAuth("fakeAccessKeyId", "fakeSecretAccessKey")), "us-west-1", "snowplow-integration-test-crossbatch-dedupe")
    val missingCredentialsConfigExpected = DynamoDbConfig("local", None, "us-west-1", "snowplow-integration-test-crossbatch-dedupe")

    "work with unwrapped configs/resolvers" in {
      val config = DynamoDbConfig.extractFromBase64(goodConfig, igluResolver).value
      config must beRight(goodConfigExpected)
    }

    "support nullable AWS credentials" in {
      val config = DynamoDbConfig.extractFromBase64(missingCredentialsConfig, igluResolver).value
      config must beRight(missingCredentialsConfigExpected)
    }
  }
}
