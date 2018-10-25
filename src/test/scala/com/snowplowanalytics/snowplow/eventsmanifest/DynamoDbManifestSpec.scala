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

// scala
import scala.collection.JavaConverters._

// AWS
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}

// Specs2
import org.specs2.mutable.Specification

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.DynamoDbConfig.CredentialsAuth

class DynamoDbManifestSpec extends Specification {
  val config = DynamoDbConfig("local", Some(CredentialsAuth("fakeAccessKeyId", "fakeSecretAccessKey")), "us-west-1", "snowplow-integration-test-crossbatch-dedupe")
  val credentials = new BasicAWSCredentials(config.auth.get.accessKeyId, config.auth.get.secretAccessKey)
  val client: AmazonDynamoDB = AmazonDynamoDBClientBuilder
    .standard()
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", config.awsRegion))
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .build()

  sequential
  "DynamoDbStorage" should {
    "throw an exception if the dedupe table doesn't exist" in {
      DynamoDbManifest.checkTable(client, config.dynamodbTable) must throwA[IllegalStateException]
    }

    "create the dedupe table and confirm that it exists" in {
      val tableDescription = DynamoDbManifest.createTable(client, config.dynamodbTable)
      DynamoDbManifest.checkTable(client, config.dynamodbTable) mustEqual config.dynamodbTable
    }

    val dynamoDbStorage = new DynamoDbManifest(client, config.dynamodbTable)

    "successfully push a new event" in {
      dynamoDbStorage.put("c6ef3124-b53a-4b13-a233-0088f79dcbcb", "AADCE520E20C2899F4CED228A79A3083", "2017-01-26 00:01:25.292") should beTrue
    }

    "overwrite event with identical id/fingerprint and matching timestamp" in {
      dynamoDbStorage.put("c6ef3124-b53a-4b13-a233-0088f79dcbcb", "AADCE520E20C2899F4CED228A79A3083", "2017-01-26 00:01:25.292") should beTrue
    }

    "NOT overwrite event with identical id/fingerprint but different timestamp" in {
      dynamoDbStorage.put("c6ef3124-b53a-4b13-a233-0088f79dcbcb", "AADCE520E20C2899F4CED228A79A3083", "2017-01-26 00:02:25.292") should beFalse
    }

    "convert list arguments into DynamoDB-compatible hashmaps" in {
      val attributes = List(("1", 1), ("2", "2"), ("3", true))
      val expected = Map("1" -> new AttributeValue().withN("1"), "2" -> new AttributeValue().withS("2"), "3" -> null).asJava
      DynamoDbManifest.attributeValues(attributes) mustEqual expected
    }
  }
}
