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

// Scala
import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// JSON
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.fge.jsonschema.core.report.ProcessingMessage
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JValue}

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Iglu
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods._

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

  implicit val formats = DefaultFormats

  /**
    * Convert a base64-encoded JSON String into a JsonNode.
    *
    * @param str   base64-encoded JSON
    * @param field Arbitrary human-readable field name
    */
  private def base64ToJsonNode(str: String, field: String): Validation[ProcessingMessage, JsonNode] =
    try {
      val decodedBytes = new Base64(true).decode(str) // Decode Base64 to JSON String
      val raw = new String(decodedBytes, UTF_8) // Must specify charset (EMR uses US_ASCII)
      Option(new ObjectMapper().readTree(raw)) // Convert JSON String to JsonNode
        .toSuccess(s"Field [$field]: invalid JSON [$raw] with parsing error: mapping resulted in null")
        .toProcessingMessage
    } catch {
      case NonFatal(e) =>
        s"Field [$field]: exception converting [$str] to JsonNode: [${e.getMessage}]"
          .fail
          .toProcessingMessage
    }


  /**
    * Extract a `DynamoDbConfig` from a base64-encoded self-describing JSON.
    *
    * @param base64Config base64-encoded self-describing JSON with an `amazon_dynamodb_config` instance
    * @param igluResolver Iglu-resolver to check that the JSON instance in `base64` is valid
    * @return Success(None) if the config was not extracted from the `Option`,
    *         Failure if the JSON was invalid or didn't correspond to a `DynamoDbConfig`,
    *         Success(DynamoDbConfig) otherwise
    */
  def extract(base64Config: Validation[ProcessingMessage, Option[String]], igluResolver: ValidationNel[ProcessingMessage, Resolver]): ValidationNel[ProcessingMessage, Option[DynamoDbConfig]] = {
    val nestedValidation = (base64Config.toValidationNel |@| igluResolver) { (config: Option[String], resolver: Resolver) =>
      config match {
        case Some(encodedConfig) =>
          for {config <- extractFromBase64(encodedConfig, resolver)} yield config.some
        case None => none.successNel
      }
    }

    nestedValidation.flatMap(identity) // Combine nested validations
  }

  /**
    * Extract `DynamoDbConfig` from a base64-encoded self-describing JSON,
    * also checking that it is valid.
    *
    * @param base64Config base64-encoded self-describing JSON with an `amazon_dynamodb_config` instance
    * @param igluResolver Iglu-resolver to check that the JSON instance in `base64` is valid
    * @return Failure if the JSON was invalid or didn't correspond to a `DynamoDbConfig`,
    *         Success(DynamoDbConfig) otherwise
    */
  def extractFromBase64(base64Config: String, igluResolver: Resolver): ValidationNel[ProcessingMessage, DynamoDbConfig] = {
    base64ToJsonNode(base64Config, "events manifest") // Decode base64
      .toValidationNel // Fix container type
      .flatMap { node: JsonNode => node.validate(dataOnly = true)(igluResolver) } // Validate against schema
      .map(fromJsonNode) // Transform to JValue
      .flatMap { json: JValue => // Extract config instance
      Validation.fromTryCatch(json.extract[DynamoDbConfig]).leftMap(e => toProcMsg(e.getMessage)).toValidationNel
    }
  }
}
