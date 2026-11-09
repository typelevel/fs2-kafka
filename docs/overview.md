# Overview

Functional backpressured streams for consuming and producing Kafka records. Exposes a small interface, while taking care of common functionality: batch consuming and producing records, batched offset commits, offset commit recovery, and topic administration, while also simplifying client configuration.

Documentation is kept up-to-date, currently documenting v@VERSION@ on Scala @SCALA_DOCS_VERSION@.

## Getting Started

To get started with [sbt](https://scala-sbt.org), simply add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@VERSION@"
```

Published for Scala @SCALA_PUBLISH_VERSIONS@.

For changes between versions, please refer to the [release notes](https://github.com/typelevel/fs2-kafka/releases).

For Scala 2.12, enable partial unification by adding the following line to `build.sbt`.

```scala
scalacOptions += "-Ypartial-unification"
```

For [share groups](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) (KIP-932), add the experimental [share consumers](share-consumers.md) module.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@SHARE_MODULE_NAME@" % "@VERSION@"
```

### External Modules

Following is an incomplete list of modules maintained in separate repositories.

- [fs2-kafka-vulcan](https://github.com/typelevel/fs2-kafka-vulcan) for [Avro](https://avro.apache.org) support using [Vulcan](https://typelevel.org/vulcan).
- [trace4cats-kafka](https://github.com/trace4cats/trace4cats-kafka)

### Signatures

Stable release artifacts are signed with the [`14CA 4AC4 FDB0 0D5F`](https://keys.openpgp.org/search?q=598648A65DF64506) key.

### Compatibility

Backwards binary-compatibility for the library is guaranteed between minor and patch versions.

Version `@MAJOR_VERSION@.a.b` is backwards binary-compatible with `@MAJOR_VERSION@.c.d` for any `a > c` or `a = c` and `b > d`.

Please note binary-compatibility is not guaranteed between milestone releases.

### Snapshot Releases

To use the latest snapshot release, add the following lines to your `build.sbt` file.

```scala
resolvers += Resolver.sonatypeCentralSnapshots

libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@SNAPSHOT_VERSION@"
```

## Dependencies

Refer to the table below for dependencies and version support across modules.

| Module               | Dependencies                                                                                                                  | Scala                             |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `@CORE_MODULE_NAME@` | [FS2 @FS2_VERSION@](https://github.com/typelevel/fs2), [Apache Kafka Client @KAFKA_VERSION@](https://github.com/apache/kafka) | Scala @CORE_CROSS_SCALA_VERSIONS@ |

## Adopters

Following are some companies using FS2 Kafka in production.

Feel free to raise a pull request to add your company to the list.

- [ITV](https://github.com/itv)
- [OVO Energy](https://github.com/ovotech)
- [Stuart](https://github.com/StuartApp)

## Inspiration

Library is heavily inspired by ideas from [Alpakka Kafka](https://github.com/akka/alpakka-kafka).

## Participation

Everyone is expected to follow the [Typelevel Code of Conduct](https://typelevel.org/code-of-conduct/) as described [here](https://github.com/typelevel/.github/blob/main/CODE_OF_CONDUCT.md).

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html). Refer to the [license file](https://github.com/typelevel/fs2-kafka/blob/main/license.txt).
