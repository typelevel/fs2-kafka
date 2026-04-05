---
id: overview
title: Overview
---

Functional backpressured streams for consuming and producing Kafka records. Exposes a small interface, while taking care of common functionality: batch consuming and producing records, batched offset commits, offset commit recovery, and topic administration, while also simplifying client configuration.

Documentation is kept up-to-date, currently documenting v@LATEST_VERSION@ on Scala @DOCS_SCALA_MINOR_VERSION@.

## Getting Started

To get started with [sbt](https://scala-sbt.org), simply add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@LATEST_VERSION@"
```

Published for Scala @SCALA_PUBLISH_VERSIONS@. For changes, refer to the [release notes](https://github.com/fd4s/fs2-kafka/releases).

For Scala 2.12, enable partial unification by adding the following line to `build.sbt`.

```scala
scalacOptions += "-Ypartial-unification"
```

### Modules

For [Avro](https://avro.apache.org) support using [Vulcan](modules.md#vulcan), add the following lines to your `build.sbt` file.

```scala
resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "@ORGANIZATION@" %% "@VULCAN_MODULE_NAME@" % "@LATEST_VERSION@"
```

In order to test vulcan codecs against a [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html) using
[munit](https://scalameta.org/munit/), add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@VULCAN_TESTKIT_MUNIT_MODULE_NAME@" % "@LATEST_VERSION@"
```

#### External Modules

Following is an incomplete list of third-party integrations.

- [trace4cats-kafka](https://github.com/trace4cats/trace4cats-kafka)

### Signatures

Stable release artifacts are signed with the [`E97C 64AB 4987 7F74`](https://keys.openpgp.org/search?q=D9A5006CBC771CEAEB0CA118E97C64AB49877F74) key.

### Compatibility

Backwards binary-compatibility for the library is guaranteed between minor and patch versions.<br>
Version `@LATEST_MAJOR_VERSION@.a.b` is backwards binary-compatible with `@LATEST_MAJOR_VERSION@.c.d` for any `a > c` or `a = c` and `b > d`.

Please note binary-compatibility is not guaranteed between milestone releases.

### Snapshot Releases

To use the latest snapshot release, add the following lines to your `build.sbt` file.

```scala
resolvers += Resolver.sonatypeCentralSnapshots

libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@LATEST_SNAPSHOT_VERSION@"
```

## Dependencies

Refer to the table below for dependencies and version support across modules.

| Module                 | Dependencies                                                                                                                                                      | Scala                               |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `@CORE_MODULE_NAME@`   | [FS2 @FS2_VERSION@](https://github.com/functional-streams-for-scala/fs2), [Apache Kafka Client @KAFKA_VERSION@](https://github.com/apache/kafka)                  | Scala @CORE_CROSS_SCALA_VERSIONS@   |
| `@VULCAN_MODULE_NAME@` | [Vulcan @VULCAN_VERSION@](https://github.com/fd4s/vulcan), [Confluent Kafka Avro Serializer @CONFLUENT_VERSION@](https://github.com/confluentinc/schema-registry) | Scala @VULCAN_CROSS_SCALA_VERSIONS@ |

## Adopters

Following are some companies using FS2 Kafka in production.<br>
Feel free to raise a pull request to add your company to the list.

- [ITV](https://github.com/itv)
- [OVO Energy](https://github.com/ovotech)
- [Stuart](https://github.com/StuartApp)

## Inspiration

Library is heavily inspired by ideas from [Alpakka Kafka](https://github.com/akka/alpakka-kafka).

## Participation

FS2 Kafka embraces pure, typeful, idiomatic functional programming in Scala, and wants to provide a safe and friendly environment for teaching, learning, and contributing as described in the [Scala Code of Conduct](https://www.scala-lang.org/conduct/).

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html). Refer to the [license file](https://github.com/fd4s/fs2-kafka/blob/master/license.txt).
