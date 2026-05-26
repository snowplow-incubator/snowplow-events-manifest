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

// java.time
import java.time.Instant
import java.util.UUID

// Scala
import scala.jdk.CollectionConverters._

// AWS SDK v2
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

/**
  * Wrapper for a DynamoDB client that handles interactions with the events manifest table.
  * Due to containing lots of mutable state, references and unserializable objects this wrapper
  * should be constructed as late as possible - straight inside a `ShredJob`,
  *
  * Initialized via `EventsManifest.initStorage()`,
  *
  * @param client AWS DynamoDB client object
  * @param table  AWS DynamoDB table name
  */
case class DynamoDbManifest(client: DynamoDbClient, table: String) extends EventsManifest {

  import DynamoDbManifest._

  /**
    * Try to store parts of an event into a previously specified table.
    *
    * @param eventId          Snowplow event ID (UUID)
    * @param eventFingerprint enriched event's fingerprint
    * @param etlTstamp        enrichment job's timestamp
    * @return true if the event is successfully stored in the table,
    *         false if both eventId and fingerprint are already in the table
    */
  def put(eventId: UUID, eventFingerprint: String, etlTstamp: Instant): Boolean = {
    val etl: Int = (etlTstamp.toEpochMilli / 1000).toInt
    val ttl = etl + 180 * 24 * 60 * 60
    val putRequest = PutItemRequest.builder()
      .tableName(table)
      .conditionExpression(
        s"(attribute_not_exists($EventIdColumn) AND attribute_not_exists($FingerprintColumn)) OR $EtlTstampColumn = :tst"
      )
      .expressionAttributeValues(
        Map(":tst" -> AttributeValue.builder().n(etl.toString).build()).asJava
      )
      .item(attributeValues(List(
        EventIdColumn     -> eventId.toString,
        FingerprintColumn -> eventFingerprint,
        EtlTstampColumn   -> etl,
        TimeToLiveColumn  -> ttl)))
      .build()

    try {
      client.putItem(putRequest)
      true
    } catch {
      case _: ConditionalCheckFailedException => false
    }
  }
}

object DynamoDbManifest {

  val EventIdColumn     = "eventId"
  val FingerprintColumn = "fingerprint"
  val EtlTstampColumn   = "etlTime"
  val TimeToLiveColumn  = "ttl"

  /**
    * Check that a table is available (block for some time if necessary).
    *
    * @param client AWS DynamoDB client with an established connection
    * @param name   DynamoDB table name
    * @return either the same table name or an exception
    */
  @throws[IllegalStateException]
  def checkTable(client: DynamoDbClient, name: String): String = {
    val request = DescribeTableRequest.builder().tableName(name).build()
    val result = try {
      Option(client.describeTable(request).table())
    } catch {
      case _: ResourceNotFoundException => None
    }
    result match {
      case Some(_) =>
        val _ = waitForActive(client, name)
        name
      case None =>
        throw new IllegalStateException("Amazon DynamoDB table for event manifest is unavailable")
    }
  }

  /**
    * Create a DynamoDB table with indices designed to store event manifests.
    * Unlike the underlying `createTable` API call, this is a blocking operation.
    *
    * @param client        AWS DynamoDB client with an established connection
    * @param name          DynamoDB table name
    * @param readCapacity  DynamoDB reads/second
    * @param writeCapacity DynamoDB writes/second
    * @return table description object
    */
  def createTable(client: DynamoDbClient, name: String, readCapacity: Option[Long], writeCapacity: Option[Long]): TableDescription = {
    val pks = List(
      AttributeDefinition.builder().attributeName(EventIdColumn).attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName(FingerprintColumn).attributeType(ScalarAttributeType.S).build()
    )
    val schema = List(
      KeySchemaElement.builder().attributeName(EventIdColumn).keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName(FingerprintColumn).keyType(KeyType.RANGE).build()
    )

    val readCapacityUnits: Long  = readCapacity.getOrElse(20L)
    val writeCapacityUnits: Long = writeCapacity.getOrElse(100L)
    val request = CreateTableRequest.builder()
      .tableName(name)
      .attributeDefinitions(pks.asJava)
      .keySchema(schema.asJava)
      .provisionedThroughput(
        ProvisionedThroughput.builder()
          .readCapacityUnits(readCapacityUnits)
          .writeCapacityUnits(writeCapacityUnits)
          .build()
      )
      .build()
    client.createTable(request)

    val description = waitForActive(client, name)
    val ttlSpecification = TimeToLiveSpecification.builder()
      .attributeName(TimeToLiveColumn)
      .enabled(true)
      .build()
    val ttlRequest = UpdateTimeToLiveRequest.builder()
      .tableName(name)
      .timeToLiveSpecification(ttlSpecification)
      .build()
    client.updateTimeToLive(ttlRequest)

    description
  }

  /**
    * Blocking method to wait until a table is in ACTIVE state.
    */
  def waitForActive(client: DynamoDbClient, name: String): TableDescription = {
    val describeRequest = DescribeTableRequest.builder().tableName(name).build()
    client.waiter().waitUntilTableExists(describeRequest)
    client.describeTable(describeRequest).table()
  }

  /**
    * Helper method to transform list arguments into a DynamoDB-compatible hash map.
    *
    * @param attributes list of key-value pairs (where values can only be strings or integers)
    * @return Java-compatible Hash-map
    */
  def attributeValues(attributes: Seq[(String, Any)]): java.util.Map[String, AttributeValue] =
    attributes
      .map { case (k, v) => (k, asAttributeValue(v)) }
      .toMap
      .asJava

  /**
    * Convert **only** strings and numbers to DynamoDB-compatible attribute data.
    */
  def asAttributeValue(v: Any): AttributeValue = {
    v match {
      case s: String           => AttributeValue.builder().s(s).build()
      case n: java.lang.Number => AttributeValue.builder().n(n.toString).build()
      case _                   => null
    }
  }
}
