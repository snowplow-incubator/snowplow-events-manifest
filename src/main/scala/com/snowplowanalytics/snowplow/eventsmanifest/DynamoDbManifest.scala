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

// java.time
import java.time.ZonedDateTime

// Scala
import scala.collection.JavaConverters._

// AWS
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.model._

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
case class DynamoDbManifest(client: AmazonDynamoDB, table: String) extends EventsManifest {

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
  def put(eventId: String, eventFingerprint: String, etlTstamp: String): Boolean = {
    val etl: Int = (ZonedDateTime.parse(etlTstamp, EventsManifest.RedshiftTstampFormatter).toInstant.toEpochMilli / 1000).toInt
    val ttl = etl + 180 * 24 * 60 * 60
    val putRequest = putRequestDummy
      .withExpressionAttributeValues(Map(":tst" -> new AttributeValue().withN(etl.toString)).asJava)
      .withItem(attributeValues(List(
        EventIdColumn -> eventId,
        FingerprintColumn -> eventFingerprint,
        EtlTstampColumn -> etl,
        TimeToLiveColumn -> ttl)
      ))

    try {
      client.putItem(putRequest)
      true
    } catch {
      case _: ConditionalCheckFailedException => false
    }
  }

  /**
    * A conditional write request that will pass if both `eventId` AND `fingerprint` are not present in the table,
    * effectively meaning that only non-duplicates will be written.
    *
    * Dupes can still pass if an event's `etl_timestamp` matches, effectively meaning that a previous shred was
    * interrupted and an event is being overwritten.
    */
  val putRequestDummy: PutItemRequest = new PutItemRequest()
    .withTableName(table)
    .withConditionExpression(s"(attribute_not_exists($EventIdColumn) AND attribute_not_exists($FingerprintColumn)) OR $EtlTstampColumn = :tst")
}

object DynamoDbManifest {

  val EventIdColumn = "eventId"
  val FingerprintColumn = "fingerprint"
  val EtlTstampColumn = "etlTime"
  val TimeToLiveColumn = "ttl"

  /**
    * Check that a table is available (block for some time if necessary).
    *
    * @param client AWS DynamoDB client with an established connection
    * @param name   DynamoDB table name
    * @return either the same table name or an exception
    */
  @throws[IllegalStateException]
  def checkTable(client: AmazonDynamoDB, name: String): String = {
    val request = new DescribeTableRequest().withTableName(name)
    val result = try {
      Option(client.describeTable(request).getTable)
    } catch {
      case _: ResourceNotFoundException => None
    }
    result match {
      case Some(description) =>
        waitForActive(client, name, description)
        name
      case None =>
        throw new IllegalStateException("Amazon DynamoDB table for event manifest is unavailable")
    }
  }

  /**
    * Create a DynamoDB table with indices designed to store event manifests.
    * Unlike `DynamoDB#createTable`, this is a blocking operation.
    *
    * @param client AWS DynamoDB client with an established connection
    * @param name   DynamoDB table name
    * @return table description object
    */
  def createTable(client: AmazonDynamoDB, name: String): TableDescription = {
    val pks = List(
      new AttributeDefinition(EventIdColumn, ScalarAttributeType.S),
      new AttributeDefinition(FingerprintColumn, ScalarAttributeType.S))
    val schema = List(
      new KeySchemaElement(EventIdColumn, KeyType.HASH),
      new KeySchemaElement(FingerprintColumn, KeyType.RANGE))

    val request = new CreateTableRequest()
      .withTableName(name)
      .withAttributeDefinitions(pks.asJava)
      .withKeySchema(schema.asJava)
      .withProvisionedThroughput(new ProvisionedThroughput(100L, 100L))
    val response = client.createTable(request)

    val description = waitForActive(client, name, response.getTableDescription)
    val ttlSpecification = new TimeToLiveSpecification()
      .withAttributeName(TimeToLiveColumn)
      .withEnabled(true)
    val ttlRequest = new UpdateTimeToLiveRequest()
      .withTableName(name)
      .withTimeToLiveSpecification(ttlSpecification)
    client.updateTimeToLive(ttlRequest) // Update when the table is active

    description
  }

  /**
    * Blocking method to reassure that a table is available for read.
    */
  @throws[ResourceNotFoundException]
  def waitForActive(client: AmazonDynamoDB, name: String, description: TableDescription): TableDescription = {
    new Table(client, name, description).waitForActive()
  }

  /**
    * Helper method to transform list arguments into a DynamoDB-compatible hash map.
    *
    * @param attributes list of key-value pairs (where values can only be strings or integers)
    * @return Java-compatible Hash-map
    */
  def attributeValues(attributes: Seq[(String, Any)]): java.util.Map[String, AttributeValue] =
    attributes.toMap.mapValues(asAttributeValue).asJava

  /**
    * Convert **only** strings and numbers to DynamoDB-compatible attribute data.
    */
  def asAttributeValue(v: Any): AttributeValue = {
    val value = new AttributeValue
    v match {
      case s: String => value.withS(s)
      case n: java.lang.Number => value.withN(n.toString)
      case _ => null
    }
  }
}
