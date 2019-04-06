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
    version := "0.2.0",
    description := "Identifying duplicate events across batches",
    scalaVersion := "2.12.8",
    crossScalaVersions := Seq("2.11.12", "2.12.8"),
    scalacOptions := BuildSettings.compilerOptions ++ (if (scalaVersion.value.startsWith("2.12")) Seq(
      "-Ywarn-unused:privates",   // Warn if a private member is unused.
      "-Ywarn-unused:patvars",    // Warn if a variable bound in a pattern is unused.
      "-Ywarn-unused:params",     // Warn if a value parameter is unused.
      "-Ywarn-unused:locals",     // Warn if a local definition is unused.
      "-Ywarn-unused:imports",    // Warn if an import selector is not referenced.
      "-Ywarn-unused:implicits",  // Warn if an implicit parameter is unused.
      "-Xlint:constant",          // Evaluation of a constant arithmetic expression results in an error.
      "-Ywarn-extra-implicit"     // Warn when more than one implicit parameter section is defined.
    ) else Seq()) ,
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
