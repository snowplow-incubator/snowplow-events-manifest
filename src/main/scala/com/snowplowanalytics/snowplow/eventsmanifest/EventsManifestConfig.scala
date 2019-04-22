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

import cats.Monad
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.show._
import cats.syntax.apply._
import cats.instances.option._
import cats.effect.Clock

import io.circe.{ Json, Decoder }
import io.circe.syntax._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.{ SelfDescribingData, SchemaKey, SchemaVer }
import com.snowplowanalytics.iglu.core.circe.implicits._


/** Trait to hold all possible types for events manifest configs */
sealed trait EventsManifestConfig {
  def name: String
}

object EventsManifestConfig {
  import DynamoDb.Credentials

  /** Parse any known config as `EventsManifestConfig` and validate it against Iglu schema */
  def parseJson[F[_]: Monad: RegistryLookup: Clock](igluClient: Client[F, Json], json: Json): EitherT[F, String, EventsManifestConfig] =
    for {
      payload <- SelfDescribingData.parse(json).toEitherT[F].leftMap(c => s"Config JSON is not self-describing, $c")
      _ <- igluClient.check(payload).leftMap(_.asJson.noSpaces)
      config = payload.schema match {
        case SchemaKey("com.snowplowanalytics.snowplow.storage", "amazon_dynamodb_config", _, SchemaVer.Full(1 | 2, _, _)) =>
          DynamoDb.extract(payload)
        case _ => s"Cannot parse ${payload.schema.toSchemaUri} as Events Manifest config, ${DynamoDb.Schema.toSchemaUri} is supported ".asLeft
      }
      result <- config.toEitherT[F]
    } yield result

  /**
    * Configuration required to set up an events manifest table in DynamoDB.
    * Corresponds to `iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0`
    *
    * @param id            Unique machine-readable storage identificator (optional because of 1-0-0 support)
    * @param name          An arbitrary human-readable name for the storage target
    * @param auth          Some for embedded AWS credentials, None for default credentials provider
    * @param awsRegion     AWS region
    * @param dynamodbTable AWS DynamoDB table name
    */
  case class DynamoDb(id: Option[UUID],
                      name: String,
                      auth: Option[Credentials],
                      awsRegion: String,
                      dynamodbTable: String) extends EventsManifestConfig

  object DynamoDb {

    val Schema = SchemaKey("com.snowplowanalytics.snowplow.storage", "amazon_dynamodb_config", "jsonschema", SchemaVer.Full(2, 0, 0))

    /**
      * Optional configuration for embedded AWS credentials.
      *
      * @param accessKeyId     AWS access key id
      * @param secretAccessKey AWS secret access key
      */
    case class Credentials(accessKeyId: String, secretAccessKey: String)

    /** Extract DynamoDB config without checking against its schema */
    def extract(payload: SelfDescribingData[Json]): Either[String, DynamoDb] =
      payload.schema match {
        // com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0
        case SchemaKey("com.snowplowanalytics.snowplow.storage", "amazon_dynamodb_config", _, SchemaVer.Full(_, _, _)) =>
          payload.data.as[DynamoDb].leftMap(_.show)
        case _ => s"Cannot parse ${payload.schema.toSchemaUri} as DynamoDB config, ${Schema.toSchemaUri} is latest supported ".asLeft
      }

    implicit val credentialsAuthDecoder: Decoder[Credentials] =
      Decoder.instance { cursor =>
        for {
          accessKeyId <- cursor.downField("accessKeyId").as[String]
          secretAccessKey <- cursor.downField("secretAccessKey").as[String]
        } yield Credentials(accessKeyId, secretAccessKey)
      }

    implicit val dynamoDbConfigCirceDecoder: Decoder[DynamoDb] =
      Decoder.instance { cursor =>
        for {
          id   <- cursor.downField("id").as[Option[UUID]]
          name <- cursor.downField("name").as[String]

          // Support both 1-0-0 and 2-0-0
          auth <- cursor.downField("auth").as[Option[Credentials]]
          accessKeyId <- cursor.downField("accessKeyId").as[Option[String]]
          secretAccessKey <- cursor.downField("secretAccessKey").as[Option[String]]
          oldCreds = (accessKeyId, secretAccessKey).mapN(Credentials.apply)
          creds = auth.orElse(oldCreds)

          region <- cursor.downField("awsRegion").as[String]
          table <- cursor.downField("dynamodbTable").as[String]
        } yield DynamoDb(id, name, creds, region, table)
      }
  }
}
