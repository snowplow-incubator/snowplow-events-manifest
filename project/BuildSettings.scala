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
import sbt._
import Keys._

// dynver plugin
import sbtdynver.DynVerPlugin.autoImport._

// DynamoDB Local
import com.localytics.sbt.dynamodb.DynamoDBLocalKeys._

// tpolecat
import org.typelevel.scalacoptions.ScalacOptions
import org.typelevel.sbt.tpolecat.TpolecatPlugin.autoImport._



object BuildSettings {

  lazy val buildSettings = Seq[Setting[_]](
    organization := "com.snowplowanalytics",
    name := "snowplow-events-manifest",
    description := "Identifying duplicate events across batches",
    scalaVersion := "2.13.18",
    crossScalaVersions := Seq("2.13.18", "2.12.21"),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    Test / parallelExecution := false, // possible race bugs
    // specs2 mutable DSL (sequential / should / in) works via side effects;
    // the returned Fragment/Arguments values are intentionally discarded.
    // -Wnonunit-statement is a Scala 2.13-only flag; suppressing it here is
    // safe because the same pattern is not flagged by the 2.12 equivalent.
    Test / tpolecatExcludeOptions += ScalacOptions.warnNonUnitStatement,
    Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    javacOptions := Seq(
      "-source", "17",
      "-target", "17"
    )
  )


  // Bintray publishing settings
  lazy val publishSettings = Seq[Setting[_]](
    publishArtifact := true,
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    developers := List(
      Developer(
        "Snowplow Analytics Ltd",
        "Snowplow Analytics Ltd",
        "support@snowplowanalytics.com",
        url("https://snowplowanalytics.com")
      )
    )
  )

  lazy val dynamoDbSettings = Seq(
    startDynamoDBLocal := startDynamoDBLocal.dependsOn(Test / compile).value,
    Test / test := (Test / test).dependsOn(startDynamoDBLocal).value,
    Test / testOnly := (Test / testOnly).dependsOn(startDynamoDBLocal).evaluated,
    Test / testOptions += dynamoDBLocalTestCleanup.value
  )
}
