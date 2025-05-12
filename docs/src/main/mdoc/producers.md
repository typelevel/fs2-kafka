---
id: producers
title: Producers
---

Producers support publishing of records. Producers make use of the [Java Kafka producer][java-kafka-producer], which becomes especially important for [settings](#settings).

The following imports are assumed throughout this page.

```scala mdoc:silent
import scala.concurrent.duration._

import cats.effect._
import cats.syntax.all._
import fs2.kafka._
```

## Serializers

[`Serializer`][serializer] describes functional composable serializers for record keys and values. We generally require two serializers: one for the record key and one for the record value. Serializers are provided implicitly for many standard library types, including:

- `Array[Byte]`, `Double`, `Float`, `Int`, `Long`, `Short`, `String`, and `UUID`.

There are also serializers for types which carry special meaning:

- `Option[A]` to serialize occurrances of `None` as `null`, and

- `Unit` to ignore the value and always serialize as `null`.

For more involved types, we need to resort to custom serializers.

### Custom Serializers

`Serializer[F[_], A]` describes a function `A => F[Array[Byte]]`, while also having access to the topic name and record [`Headers`][headers]. There are many [functions][serializer$] available for creating custom serializers, with the most basic one being `instance`, which simply creates a serializer from a provided function.

```scala mdoc:silent
Serializer.instance[IO, String] { (topic, headers, s) =>
  IO.pure(s.getBytes("UTF-8"))
}
```

If the serializer only needs access to the bytes, like in the case above, use `lift`.

```scala mdoc:silent
Serializer.lift[IO, String](s => IO.pure(s.getBytes("UTF-8")))
```

To support different serializers for different topics, use `topic` to pattern match on the topic name.

```scala mdoc:silent
Serializer.topic[KeyOrValue, IO, Int] {
  case "first"  => Serializer[IO, String].contramap(_.show)
  case "second" => Serializer[IO, Int]
}
```

For unmatched topics, an [`UnexpectedTopicException`][unexpectedtopicexception] is raised.

Use `headers` for different deserializers depending on record headers.

```scala mdoc:silent
Serializer.headers[IO, Int] { headers =>
  headers("format").map(_.as[String]) match {
    case Some("string") => Serializer[IO, String].contramap(_.show)
    case Some("int")    => Serializer[IO, Int]
    case Some(format)   => Serializer.failWith(s"unknown format: $format")
    case None           => Serializer.failWith("format header is missing")
  }
}
```

In the example above, `failWith` raises a [`SerializationException`][serializationexception] with the provided message.

### Java Interoperability

If we have a Java Kafka serializer, use `delegate` to create a `Serializer`.

```scala mdoc:silent
Serializer.delegate[IO, String] {
  new KafkaSerializer[String] {

    def serialize(topic: String, value: String): Array[Byte] =
      value.getBytes("UTF-8")

  }
}
```

If the serializer performs _side effects_, follow with `suspend` to capture them properly.

```scala mdoc:silent
Serializer
  .delegate[IO, String] {
    new KafkaSerializer[String] {

      def serialize(topic: String, value: String): Array[Byte] = {
        println(s"serializing record on topic $topic")
        value.getBytes("UTF-8")
      }

    }
  }
  .suspend
```

Note that `close` and `configure` won't be called for the delegates.

## Settings

In order to create a [`KafkaProducer`][kafkaproducer], we first need to create [`ProducerSettings`][producersettings]. At the very minimum, settings include the effect type to use, and the key and value serializers. More generally, [`ProducerSettings`][producersettings] contain everything necessary to create a [`KafkaProducer`][kafkaproducer]. If serializers are available implicitly for the key and value type, we can use the syntax in the following example.

```scala mdoc:silent
val producerSettings =
  ProducerSettings[IO, String, String].withBootstrapServers("localhost:9092")
```

We can also specify the serializers explicitly when necessary.

```scala mdoc:silent
ProducerSettings(
  keySerializer = Serializer[IO, String],
  valueSerializer = Serializer[IO, String]
).withBootstrapServers("localhost:9092")
```

[`ProducerSettings`][producersettings] provides functions for configuring both the Java Kafka producer and options specific to the library. If functions for configuring certain properties of the Java Kafka producer is missing, we can instead use `withProperty` or `withProperties` together with constants from [`ProducerConfig`][producerconfig]. Available properties for the Java Kafka producer are described in the [documentation](http://kafka.apache.org/documentation/#producerconfigs).

### Default Settings

The following settings are specific to the library.

- `withCloseTimeout` controls the timeout when waiting for producer shutdown. Default is 60 seconds.

- `withCreateProducer` changes how the underlying Java Kafka producer is created. The default merely creates a Java `KafkaProducer` instance using set properties, but this function allows overriding the behaviour for e.g. testing purposes.

## Producer Creation

Once [`ProducerSettings`][producersettings] is defined, use `KafkaProducer.stream` to create a [`KafkaProducer`][kafkaproducer] instance.

```scala mdoc:silent
object ProducerExample extends IOApp.Simple {

  val run: IO[Unit] =
    KafkaProducer.stream(producerSettings).compile.drain

}
```

There is also `KafkaProducer.resource` for when it's preferable to work with `Resource`. Both these functions create an underlying Java Kafka producer. They both also guarantee resource cleanup, i.e. closing the Kafka producer instance.

In the example above, we simply create the producer and then immediately shutdown after resource cleanup. [`KafkaProducer`][kafkaproducer] only supports producing records, and there is a separate producer available to support [transactions](transactions.md).

## Producing Records

If we're only producing records in one part of our stream, using `produce` is convenient.

```scala mdoc:silent
val consumerSettings =
  ConsumerSettings[IO, String, String]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")

object ProduceExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("topic")
        .records
        .map { committable =>
          val key    = committable.record.key
          val value  = committable.record.value
          val record = ProducerRecord("topic", key, value)
          ProducerRecords.one(record)
        }
        .through(KafkaProducer.pipe(producerSettings))

    stream.compile.drain.as(ExitCode.Success)
  }

}
```

In the stream above, we're simply producing the records we receive back to the topic.

The `produce` function creates a `KafkaProducer` and produces records in `ProducerRecords`, which is al alias for `fs2.Chunk`. Once all records have been produced in the `ProducerRecords`, the inner effect will complete with a `ProducerResult`, which is an alias for `Chunk[(ProducerRecord[K, V], RecordMetadata)]`.

If we're producing in multiple places in our stream, we can create the `KafkaProducer` ourselves, and pass it to the `pipe` function.

```scala mdoc:silent
object PartitionedProduceExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaProducer
        .stream(producerSettings)
        .flatMap { producer =>
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo("topic")
            .partitionedRecords
            .map { partition =>
              partition
                .map { committable =>
                  val key    = committable.record.key
                  val value  = committable.record.value
                  val record = ProducerRecord("topic", key, value)
                  ProducerRecords.one(record)
                }
                .through(KafkaProducer.pipe(producer))
            }
            .parJoinUnbounded
        }

    stream.compile.drain.as(ExitCode.Success)
  }

}
```

If we need more control of how records are produced, we can use `KafkaProducer#produce`. The function returns two effects, e.g. `IO[IO[...]]`, where the first effect puts the records in the producer's buffer, and the second effects waits for the records to have been sent.

```scala mdoc:silent
object KafkaProducerProduceExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaProducer
        .stream(producerSettings)
        .flatMap { producer =>
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo("topic")
            .records
            .map { committable =>
              val key    = committable.record.key
              val value  = committable.record.value
              val record = ProducerRecord("topic", key, value)
              ProducerRecords.one(record)
            }
            .evalMap(producer.produce)
            .groupWithin(500, 15.seconds)
            .evalMap(_.sequence)
        }

    stream.compile.drain.as(ExitCode.Success)
  }

}
```

The example above puts 500 records in the producer's buffer or however many can be put in the buffer every 15 seconds, and then waits for those records to finish sending before continuing. Using `produce` allows more precise control of how records are put in the buffer and when we wait for records to send.

Sometimes there is a need to wait for individual `ProducerRecords` to send. In this case, we can `flatten` the result of `produce` to both send the record and wait for the send to complete. Note that this should generally be avoided, as it achieves poor performance.

```scala mdoc:silent
object KafkaProducerProduceFlattenExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val stream =
      KafkaProducer
        .stream(producerSettings)
        .flatMap { producer =>
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo("topic")
            .records
            .map { committable =>
              val key    = committable.record.key
              val value  = committable.record.value
              val record = ProducerRecord("topic", key, value)
              ProducerRecords.one(record)
            }
            .evalMap { record =>
              producer.produce(record).flatten
            }
        }

    stream.compile.drain.as(ExitCode.Success)
  }

}
```

[headers]: @API_BASE_URL@/Headers.html
[java-kafka-producer]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/producer/KafkaProducer.html
[kafkaproducer]: @API_BASE_URL@/KafkaProducer.html
[producerconfig]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/producer/ProducerConfig.html
[producersettings]: @API_BASE_URL@/ProducerSettings.html
[serializationexception]: @API_BASE_URL@/SerializationException.html
[serializer]: @API_BASE_URL@/Serializer.html
[serializer$]: @API_BASE_URL@/Serializer$.html
[unexpectedtopicexception]: @API_BASE_URL@/UnexpectedTopicException.html
