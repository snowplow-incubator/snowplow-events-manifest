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
lazy val root = project
  .in(file("."))
  .settings(
    organization := "com.snowplowanalytics",
    name := "snowplow-events-manifest",
    version := "0.3.0",
    description := "Identifying duplicate events across batches",
    scalaVersion := "2.12.8",
    crossScalaVersions := Seq("2.12.8", "2.13.2"),
    scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    javacOptions := BuildSettings.javaCompilerOptions,
    libraryDependencies ++= Seq(
      Dependencies.igluClient,
      Dependencies.dynamodb,
      Dependencies.circeLiteral,
      Dependencies.scalaCheck,
      Dependencies.specs2
    )
  )
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.coverageSettings)
  .settings(BuildSettings.dynamoDbSettings)
