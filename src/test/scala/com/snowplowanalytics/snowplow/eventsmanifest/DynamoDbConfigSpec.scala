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

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.jackson.parseJson
import com.github.fge.jsonschema.core.report.ProcessingMessage

// Iglu
import com.snowplowanalytics.iglu.client.Resolver

// Specs2
import org.specs2.mutable.Specification

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.DynamoDbConfig.CredentialsAuth

class DynamoDbConfigSpec extends Specification {
  lazy val igluResolver: Resolver = {
    val jsonConfig =
      s"""|{
          |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
          |  "data": {
          |    "cacheSize": 500,
          |    "repositories": [
          |      {
          |        "name": "Iglu Central",
          |        "priority": 0,
          |        "vendorPrefixes": [ "com.snowplowanalytics" ],
          |        "connection": {
          |          "http": {
          |            "uri": "http://iglucentral.com"
          |          }
          |        }
          |      }
          |    ]
          |  }
          |}""".stripMargin
    val parsedConfig = parseJson(jsonConfig)
    Resolver.parse(parsedConfig).toOption.get
  }

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

    "return Success(Some) if the config is valid" in {
      val config = DynamoDbConfig.extract(Some(goodConfig).success, igluResolver.success)
      config mustEqual Some(goodConfigExpected).success
    }

    "return Success(None) if the config was empty" in {
      val config = DynamoDbConfig.extract(None.success, igluResolver.success)
      config mustEqual None.success
    }

    "return Failure if the config was invalid" in {
      val configError = new ProcessingMessage().setMessage("config")
      val resolverError = new ProcessingMessage().setMessage("resolver")
      val config = DynamoDbConfig.extract(configError.failure, NonEmptyList(resolverError).failure)
      config mustEqual NonEmptyList(configError, resolverError).failure
    }

    "work with unwrapped configs/resolvers" in {
      val config = DynamoDbConfig.extractFromBase64(goodConfig, igluResolver)
      config mustEqual goodConfigExpected.success
    }

    "support nullable AWS credentials" in {
      val config = DynamoDbConfig.extractFromBase64(missingCredentialsConfig, igluResolver)
      config mustEqual missingCredentialsConfigExpected.success
    }
  }
}
