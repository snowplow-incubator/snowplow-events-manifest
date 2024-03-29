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
import sbt._

object Dependencies {
  // Scala
  val igluClient    = "com.snowplowanalytics" %% "iglu-scala-client"      % "3.0.0"

  // Java
  val dynamodb      = "com.amazonaws"         % "aws-java-sdk-dynamodb"   % "1.11.870"

  // Scala (test only)
  val circeLiteral  = "io.circe"              %% "circe-literal"          % "0.14.1"  % Test
  val scalaCheck    = "org.scalacheck"        %% "scalacheck"             % "1.17.0"  % Test
  val specs2        = "org.specs2"            %% "specs2-core"            % "4.17.0"  % Test
}
