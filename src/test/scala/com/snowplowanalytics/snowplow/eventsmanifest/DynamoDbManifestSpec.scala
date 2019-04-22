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

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

// scala
import scala.collection.JavaConverters._

// AWS
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest, ScanResult}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}

// Specs2
import org.specs2.mutable.Specification

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig.DynamoDb
import com.snowplowanalytics.snowplow.eventsmanifest.DynamoDbManifest._

class DynamoDbManifestSpec extends Specification {

  private val TstampFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"))

  def parseTstamp(str: String): Instant =
    ZonedDateTime.parse(str, TstampFormatter).toInstant

  val config = DynamoDb(None, "local", Some(DynamoDb.Credentials("fakeAccessKeyId", "fakeSecretAccessKey")), "us-west-1", "snowplow-integration-test-crossbatch-dedupe")
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
      val _ = DynamoDbManifest.createTable(client, config.dynamodbTable, None, None)
      DynamoDbManifest.checkTable(client, config.dynamodbTable) mustEqual config.dynamodbTable
    }

    val dynamoDbStorage = new DynamoDbManifest(client, config.dynamodbTable)

    "successfully push a new event" in {
      dynamoDbStorage.put(
        UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        "AADCE520E20C2899F4CED228A79A3083",
        parseTstamp("2017-01-26 00:01:25.292")) should beTrue
    }

    "overwrite event with identical id/fingerprint and matching timestamp" in {
      dynamoDbStorage.put(
        UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        "AADCE520E20C2899F4CED228A79A3083",
        parseTstamp("2017-01-26 00:01:25.292")) should beTrue
    }

    "NOT overwrite event with identical id/fingerprint but different timestamp" in {
      dynamoDbStorage.put(
        UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        "AADCE520E20C2899F4CED228A79A3083",
        parseTstamp("2017-01-26 00:02:25.292")) should beFalse
    }

    "NOT overwrite event with identical id but different fingerprint/timestamp" in {
      dynamoDbStorage.put(
        UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        "15A65BB354B1265A775FC3C60AAD5E58",
        parseTstamp("2017-01-26 00:02:25.292")) should beTrue
    }

    "NOT overwrite event with identical fingerprint but different id/timestamp" in {
      dynamoDbStorage.put(
        UUID.fromString("d7f5fa2f-3b3a-4c97-a030-8e05d9f40b0a"),
        "AADCE520E20C2899F4CED228A79A3083",
        parseTstamp("2017-01-26 00:02:25.292")) should beTrue
    }

    "correctly populate the manifest" in {
      val expected = List(
        attributeValues(List(
          EventIdColumn -> "d7f5fa2f-3b3a-4c97-a030-8e05d9f40b0a",
          FingerprintColumn -> "AADCE520E20C2899F4CED228A79A3083",
          EtlTstampColumn -> 1485388945,
          TimeToLiveColumn -> 1500940945)
        ),
        attributeValues(List(
          EventIdColumn -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
          FingerprintColumn -> "15A65BB354B1265A775FC3C60AAD5E58",
          EtlTstampColumn -> 1485388945,
          TimeToLiveColumn -> 1500940945)
        ),
        attributeValues(List(
          EventIdColumn -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
          FingerprintColumn -> "AADCE520E20C2899F4CED228A79A3083",
          EtlTstampColumn -> 1485388885,
          TimeToLiveColumn -> 1500940885)
        )
      ).asJava
      val request = new ScanRequest().withTableName(config.dynamodbTable)
      val response: ScanResult = client.scan(request)
      response.getItems mustEqual expected
    }

    "convert list arguments into DynamoDB-compatible hashmaps" in {
      val attributes = List(("1", 1), ("2", "2"), ("3", true))
      val expected = Map("1" -> new AttributeValue().withN("1"), "2" -> new AttributeValue().withS("2"), "3" -> null).asJava
      DynamoDbManifest.attributeValues(attributes) mustEqual expected
    }
  }
}
