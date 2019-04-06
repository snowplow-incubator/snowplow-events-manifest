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

import java.util.concurrent.TimeUnit

import cats.Id
import cats.effect.Clock

import io.circe.Json
import io.circe.literal._

import scala.concurrent.duration.TimeUnit

// Iglu
import com.snowplowanalytics.iglu.client.Client

object SpecHelpers {
  lazy val igluResolver: Client[Id, Json] = {
    val jsonConfig =
      json"""{
        "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
        "data": {
          "cacheSize": 500,
          "repositories": [
            {
              "name": "Iglu Central",
              "priority": 0,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "http://iglucentral.com"
                }
              }
            }
          ]
        }
      }"""
    Client.parseDefault[Id](jsonConfig).fold(throw _, identity)
  }

  implicit val idClock: Clock[Id] = new Clock[Id] {
    final def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

    final def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }
}
