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

// Bintray plugin
import bintray.BintrayPlugin._
import bintray.BintrayKeys._

// Scoverage
import scoverage.ScoverageKeys._

// DynamoDB Local
import com.localytics.sbt.dynamodb.DynamoDBLocalKeys._

object BuildSettings {

  lazy val publishSettings = bintraySettings ++ Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    publishArtifact in Test := false,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    bintrayOrganization := Some("snowplow"),
    bintrayRepository := "snowplow-maven",
    pomIncludeRepository := { _ => false },
    homepage := Some(url("https://github.com/snowplow-incubator/snowplow-events-manifest")),
    scmInfo := Some(ScmInfo(url("https://github.com/snowplow-incubator/snowplow-events-manifest"),
      "scm:git@github.com:snowplow-incubator/snowplow-events-manifest.git")),
    pomExtra := (
      <developers>
        <developer>
          <name>Snowplow Analytics Ltd</name>
          <email>support@snowplowanalytics.com</email>
          <organization>Snowplow Analytics Ltd</organization>
          <organizationUrl>http://snowplowanalytics.com</organizationUrl>
        </developer>
      </developers>)
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  lazy val coverageSettings = Seq(
    coverageMinimum := 90
  )

  lazy val dynamoDbSettings = Seq(
    startDynamoDBLocal := startDynamoDBLocal.dependsOn(compile in Test).value,
    test in Test := (test in Test).dependsOn(startDynamoDBLocal).value,
    testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal).evaluated,
    testOptions in Test += dynamoDBLocalTestCleanup.value
  )
}
