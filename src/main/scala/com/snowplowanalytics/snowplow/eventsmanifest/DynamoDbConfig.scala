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

// Java
import java.nio.charset.StandardCharsets.UTF_8

import cats.data.EitherT
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

// cats
import cats.Monad
import cats.syntax.either._
import cats.syntax.show._
import cats.effect.Clock

// JSON
import io.circe.{ Json, Decoder }
import io.circe.parser.parse

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Iglu
import com.snowplowanalytics.iglu.client.Client

// This library
import com.snowplowanalytics.snowplow.eventsmanifest.DynamoDbConfig.CredentialsAuth
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig

/**
  * Configuration required to set up an events manifest table in DynamoDB.
  * Corresponds to `iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0`
  *
  * @param name          An arbitrary human-readable name for the storage target
  * @param auth          Some for embedded AWS credentials, None for default credentials provider
  * @param awsRegion     AWS region
  * @param dynamodbTable AWS DynamoDB table name
  */
case class DynamoDbConfig(name: String, auth: Option[CredentialsAuth], awsRegion: String, dynamodbTable: String) extends EventsManifestConfig


object DynamoDbConfig {

  /**
    * Optional configuration for embedded AWS credentials.
    *
    * @param accessKeyId     AWS access key id
    * @param secretAccessKey AWS secret access key
    */
  case class CredentialsAuth(accessKeyId: String, secretAccessKey: String)

  /**
    * Extract `DynamoDbConfig` from a base64-encoded self-describing JSON,
    * also checking that it is valid.
    *
    * @param base64Config base64-encoded self-describing JSON with an `amazon_dynamodb_config` instance
    * @param igluClient Iglu-resolver to check that the JSON instance in `base64` is valid
    * @return Failure if the JSON was invalid or didn't correspond to a `DynamoDbConfig`,
    *         Success(DynamoDbConfig) otherwise
    */
  // TODO: test invalid base64
  def extractFromBase64[F[_]: Monad: RegistryLookup: Clock]
                       (base64Config: String, igluClient: Client[F, Json]): EitherT[F, String, DynamoDbConfig] =
    for {
      json    <- base64ToJson(base64Config).toEitherT[F]
      payload <- SelfDescribingData.parse(json).toEitherT[F].leftMap(c => s"Config JSON is not self-describing, $c")
      _       <- igluClient.check(payload).leftMap(_.toString)
      config  <- payload.data.as[DynamoDbConfig].toEitherT[F].leftMap(_.show)
    } yield config

  implicit val credentialsAuthDecoder: Decoder[CredentialsAuth] =
    Decoder.instance { cursor =>
      for {
        accessKeyId <- cursor.downField("accessKeyId").as[String]
        secretAccessKey <- cursor.downField("secretAccessKey").as[String]
      } yield CredentialsAuth(accessKeyId, secretAccessKey)
    }

  implicit val dynamoDbConfigCirceDecoder: Decoder[DynamoDbConfig] =
    Decoder.instance { cursor =>
      for {
        name <- cursor.downField("name").as[String]
        creds <- cursor.downField("auth").as[Option[CredentialsAuth]]
        region <- cursor.downField("awsRegion").as[String]
        table <- cursor.downField("dynamodbTable").as[String]
      } yield DynamoDbConfig(name, creds, region, table)
    }


  /** Convert a base64-encoded JSON String into a JSON */
  private def base64ToJson(str: String): Either[String, Json] =
    for {
      str <- Either.catchNonFatal {
        val decodedBytes = new Base64(true).decode(str) // Decode Base64 to JSON String
        new String(decodedBytes, UTF_8) // Must specify charset (EMR uses US_ASCII)
      }.leftMap(e => Option(e.getMessage).getOrElse(e.toString))
      json <- parse(str).leftMap(_.show)
    } yield json


}
