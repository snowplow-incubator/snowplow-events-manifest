/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
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

import java.util.UUID

import io.circe.literal._

// Specs2
import org.specs2.mutable.Specification

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig.DynamoDb

import SpecHelpers._

class EventsManifestConfigSpec extends Specification {
  val goodConfig = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
      "data": {
        "name": "local",
        "auth": {
          "accessKeyId": "fakeAccessKeyId",
          "secretAccessKey": "fakeSecretAccessKey"
        },
        "awsRegion": "us-west-1",
        "dynamodbTable": "snowplow-integration-test-crossbatch-dedupe",
        "id": "56799a26-980c-4148-8bd9-c021b988c669",
        "purpose": "EVENTS_MANIFEST"
      }
    }"""

  val goodOldConfig = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/1-0-0",
      "data": {
        "name": "local",
        "accessKeyId": "fakeAccessKeyId",
        "secretAccessKey": "fakeSecretAccessKey",
        "awsRegion": "us-west-1",
        "dynamodbTable": "snowplow-integration-test-crossbatch-dedupe",
        "purpose": "DUPLICATE_TRACKING"
      }
    }"""

  lazy val missingCredentialsConfig = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
      "data": {
        "name": "local",
        "auth": null,
        "awsRegion": "us-west-1",
        "dynamodbTable": "snowplow-integration-test-crossbatch-dedupe",
        "id": "56799a26-980c-4148-8bd9-c021b988c669",
        "purpose": "EVENTS_MANIFEST"
      }
    }"""

  "DynamoDbConfig constructors" should {
    val goodConfigExpected = DynamoDb(Some(UUID.fromString("56799a26-980c-4148-8bd9-c021b988c669")), "local", Some(DynamoDb.Credentials("fakeAccessKeyId", "fakeSecretAccessKey")), "us-west-1", "snowplow-integration-test-crossbatch-dedupe")
    val goodOldConfigExpected = DynamoDb(None, "local", Some(DynamoDb.Credentials("fakeAccessKeyId", "fakeSecretAccessKey")), "us-west-1", "snowplow-integration-test-crossbatch-dedupe")
    val missingCredentialsConfigExpected = DynamoDb(Some(UUID.fromString("56799a26-980c-4148-8bd9-c021b988c669")), "local", None, "us-west-1", "snowplow-integration-test-crossbatch-dedupe")

    "work with unwrapped configs/resolvers" in {
      val config = EventsManifestConfig.parseJson(igluResolver, goodConfig).value
      config must beRight(goodConfigExpected)
    }

    "support nullable AWS credentials" in {
      val config = EventsManifestConfig.parseJson(igluResolver, missingCredentialsConfig).value
      config must beRight(missingCredentialsConfigExpected)
    }

    "support 1-0-0 config" in {
      val config = EventsManifestConfig.parseJson(igluResolver, goodOldConfig).value
      config must beRight(goodOldConfigExpected)
    }
  }
}
