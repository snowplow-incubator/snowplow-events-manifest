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

import java.time.Instant
import java.util.UUID

// Scala
import scala.util.control.NonFatal

// AWS
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig.DynamoDb.Credentials

/**
  * Common trait for events manifest storages, storing a triple of event attributes
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
  def put(eventId: UUID, eventFingerprint: String, etlTstamp: Instant): Boolean
}


/**
  * Companion object containing utility fields and methods.
  */
object EventsManifest {

  /**
    * Universal constructor that initializes a `DuplicateStorage` instance
    * based on a `DuplicateStorage` config.
    *
    * @param config Configuration required to initialize a `DuplicateStorage`
    * @return a valid `DuplicateStorage` instance if no exceptions were thrown
    */
  def initStorage(config: EventsManifestConfig): Either[String, EventsManifest] =
    config match {
      case EventsManifestConfig.DynamoDb(_, _, auth, awsRegion, tableName) =>
        try {
          val client = auth match {
            case Some(Credentials(accessKeyId, secretAccessKey)) =>
              val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
              AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build()
            case None =>
              AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(awsRegion)
                .build()
          }
          val table = DynamoDbManifest.checkTable(client, tableName)
          Right(new DynamoDbManifest(client, table))
        } catch {
          case NonFatal(e) =>
            Left("Cannot initialize duplicate storage\n" + Option(e.getMessage).getOrElse(""))
        }
    }
}
