{%
  laika.firstHeaderAsTitle = false
  laika.template = "/helium/templates/landing.template.html"
%}

[![Typelevel Organization Project](https://img.shields.io/badge/typelevel-organization%20project-ff6169)](https://typelevel.org)
[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/typelevel/fs2-kafka/ci.yml?branch=main)](https://github.com/typelevel/fs2-kafka/actions)
[![Discord](https://img.shields.io/discord/632277896739946517?color=5865f2)](https://discord.gg/fjqZMuqzAp)
[![Version](https://img.shields.io/maven-central/v/org.typelevel/fs2-kafka_3?label=version&color=orange)](https://index.scala-lang.org/typelevel/fs2-kafka)

Functional streams for Kafka with [FS2](https://fs2.io) and the official Apache Kafka client.

Project is under active development. Feedback and contributions welcome.

### Getting Started

To get started with [sbt](https://scala-sbt.org), simply add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@VERSION@"
```

Published for Scala @SCALA_PUBLISH_VERSIONS@.

For changes between versions, please refer to the [release notes](https://github.com/typelevel/fs2-kafka/releases).
