# Snowplow Events Manifest

[![Build Status](https://api.travis-ci.org/snowplow-incubator/snowplow-events-manifest.svg)](https://travis-ci.org/snowplow-incubator/snowplow-events-manifest)
[![Maven Central](https://img.shields.io/maven-central/v/com.snowplowanalytics/snowplow-events-manifest_2.12.svg)](https://maven-badges.herokuapp.com/maven-central/com.snowplowanalytics/snowplow-events-manifest_2.12)
[![codecov](https://codecov.io/gh/snowplow-incubator/snowplow-events-manifest/branch/master/graph/badge.svg)](https://codecov.io/gh/snowplow-incubator/snowplow-events-manifest)
[![Join the chat at https://gitter.im/snowplow-incubator/snowplow-events-manifest](https://badges.gitter.im/snowplow-incubator/snowplow-events-manifest.svg)](https://gitter.im/snowplow-incubator/snowplow-events-manifest?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A standalone Scala library that contains logic used for cross-batch natural deduplication of Snowplow events, responsible for deduplication in our AWS-based pipelines. It works by extracting the `event_id` and `event_fingerprint` of an event, as well as `etl_tstamp` which identifies a single batch, then storing these properties in a DynamoDB table. Duplicate events with the same ID and fingerprint that were seen in previous batches are silently dropped from the Snowflake Transformer output.

The library uses a configuration file with the following properties:

* `name` - Required human-readable configuration name, e.g. `ACME deduplication config`.
* `id` - Required machine-readable configuration id, e.g. UUID.
* `auth` - An object containing information about authentication use to read and write data to DynamoDB. This can either use a `accessKeyId`/`secretAccessKey` AWS credentials pair or be set to `null`, in which case default credentials will be retrieved using the standard provider chain.
* `awsRegion` - AWS Region used by Transformer to access DynamoDB.
* `dynamodbTable` - DynamoDB table used to store information about duplicate events.
* `purpose` - Always `EVENTS_MANIFEST`.

An example of this configuration is as follows:

```json
{
  "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
  "data": {
    "name": "ACME deduplication config",
    "auth": {
      "accessKeyId": "fakeAccessKeyId",
      "secretAccessKey": "fakeSecretAccessKey"
    },
    "awsRegion": "us-east-1",
    "dynamodbTable": "acme-crossbatch-dedupe",
    "id": "ce6c3ff2-8a05-4b70-bbaa-830c163527da",
    "purpose": "EVENTS_MANIFEST"
  }
}
```

## Copyright and license

Copyright (c) 2018 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
