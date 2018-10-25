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
import java.time.ZoneId
import java.time.format.DateTimeFormatter

// Scala
import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// JSON Schema
import com.github.fge.jsonschema.core.report.ProcessingMessage

// AWS
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

// Iglu
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.DynamoDbConfig.CredentialsAuth

/**
  * Common trait for event duplicate storages, storing a triple of event attributes
  * that enable cross-batch event deduplication.
  * Currently implemented by `DynamoDbStorage`.
  */
trait EventsManifest {

  /**
    * Try to store parts of an event into a `duplicates` table.
    *
    * @param eventId          Snowplow event ID (UUID)
    * @param eventFingerprint enriched event's fingerprint
    * @param etlTstamp        enrichment job's timestamp
    * @return true if the event is successfully stored in the table,
    *         false if both eventId and fingerprint are already in the table
    */
  def put(eventId: String, eventFingerprint: String, etlTstamp: String): Boolean
}


/**
  * Companion object containing utility fields and methods.
  */
object EventsManifest {

  /**
    * Trait to hold all possible types for events manifest configs.
    */
  trait EventsManifestConfig

  /**
    * Default datetime format (etl_tstamp, dvce_sent_tstamp, dvce_created_tstamp)
    */
  val RedshiftTstampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"))

  /**
    * Universal constructor that initializes a `DuplicateStorage` instance
    * based on a `DuplicateStorage` config.
    *
    * @param config Configuration required to initialize a `DuplicateStorage`
    * @return a valid `DuplicateStorage` instance if no exceptions were thrown
    */
  def initStorage(config: EventsManifestConfig): Validation[ProcessingMessage, EventsManifest] =
    config match {
      case DynamoDbConfig(_, auth, awsRegion, tableName) =>
        try {
          val client = auth match {
            case Some(CredentialsAuth(accessKeyId, secretAccessKey)) =>
              val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
              AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build()
            case _ =>
              AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(awsRegion)
                .build()
          }
          val table = DynamoDbManifest.checkTable(client, tableName)
          new DynamoDbManifest(client, table).success
        } catch {
          case NonFatal(e) =>
            toProcMsg("Cannot initialize duplicate storage:\n" + Option(e.getMessage).getOrElse("")).failure
        }
    }
}
