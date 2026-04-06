---
id: consumers
title: Consumers
---

Consumers support subscribing to topics, record streaming and deserialization, as well as miscellaneous utility functionality, such as seeking to offsets, or checking what the end offsets are for a topic. Consumers make use of the [Java Kafka consumer][java-kafka-consumer], which becomes especially important for [settings](#settings). For consumer implementation details, refer to the [technical details](technical-details.md) section.

The following imports are assumed throughout this page.

```scala mdoc:silent
import scala.concurrent.duration._

import cats.effect._
import cats.syntax.all._
import fs2._
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
```

## Deserializers

[`Deserializer`][deserializer] describes functional composable deserializers for record keys and values. We generally require two deserializers: one for the record key and one for the record value. Deserializers are provided implicitly for many standard library types, including:

- `Array[Byte]`, `Double`, `Float`, `Int`, `Long`, `Short`, `String`, and `UUID`.

There are also deserializers for types which carry special meaning:

- `Option[A]` to deserialize occurrances of `null` as `None`, and

- `Unit` to ignore the serialized bytes and always use `()`.

For more involved types, we need to resort to custom deserializers.

### Custom Deserializers

`Deserializer[F[_], A]` describes a function `Array[Byte] => F[A]`, while also having access to the topic name and record [`Headers`][headers]. There are many [functions][deserializer$] available for creating custom deserializers, with the most basic one being `instance`, which simply creates a deserializer from a provided function.

```scala mdoc:silent
Deserializer.instance { (topic, headers, bytes) =>
  IO.pure(bytes.dropWhile(_ == 0))
}
```

If the deserializer only needs access to the bytes, like in the case above, use `lift`.

```scala mdoc:silent
Deserializer.lift(bytes => IO.pure(bytes.dropWhile(_ == 0)))
```

To support different deserializers for different topics, use `topic` to pattern match on the topic name.

```scala mdoc:silent
Deserializer.topic[KeyOrValue, IO, String] {
  case "first"  => Deserializer[IO, String]
  case "second" => Deserializer[IO, Int].map(_.show)
}
```

For unmatched topics, an [`UnexpectedTopicException`][unexpectedtopicexception] is raised.

Use `headers` for different deserializers depending on record headers.

```scala mdoc:silent
Deserializer.headers[IO, String] { headers =>
  headers("format").map(_.as[String]) match {
    case Some("string") => Deserializer[IO, String]
    case Some("int")    => Deserializer[IO, Int].map(_.show)
    case Some(format)   => Deserializer.failWith(s"unknown format: $format")
    case None           => Deserializer.failWith("format header is missing")
  }
}
```

In the example above, `failWith` raises a [`DeserializationException`][deserializationexception] with the provided message.

### Java Interoperability

If we have a Java Kafka deserializer, use `delegate` to create a `Deserializer`.

```scala mdoc:silent
Deserializer.delegate[IO, String] {
  new KafkaDeserializer[String] {

    def deserialize(topic: String, data: Array[Byte]): String =
      new String(data)

  }
}
```

If the deserializer performs _side effects_, follow with `suspend` to capture them properly.

```scala mdoc:silent
Deserializer
  .delegate[IO, String] {
    new KafkaDeserializer[String] {

      def deserialize(topic: String, data: Array[Byte]): String = {
        println(s"deserializing record on topic $topic")
        new String(data)
      }

    }
  }
  .suspend
```

Note that `close` and `configure` won't be called for the delegates.

## Settings

In order to create a [`KafkaConsumer`][kafkaconsumer], we first need to create [`ConsumerSettings`][consumersettings]. At the very minimum, settings include the effect type to use, and the key and value deserializers. More generally, [`ConsumerSettings`][consumersettings] contain everything necessary to create a [`KafkaConsumer`][kafkaconsumer]. If deserializers are available implicitly for the key and value type, we can use the syntax in the following example.

```scala mdoc:silent
val consumerSettings =
  ConsumerSettings[IO, String, String]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")
```

We can also specify the deserializers explicitly when necessary.

```scala mdoc:silent
ConsumerSettings(
  keyDeserializer = Deserializer[IO, String],
  valueDeserializer = Deserializer[IO, String]
).withAutoOffsetReset(AutoOffsetReset.Earliest)
  .withBootstrapServers("localhost:9092")
  .withGroupId("group")
```

[`ConsumerSettings`][consumersettings] provides functions for configuring both the Java Kafka consumer and options specific to the library. If functions for configuring certain properties of the Java Kafka consumer is missing, we can instead use `withProperty` or `withProperties` together with constants from [`ConsumerConfig`][consumerconfig]. Available properties for the Java Kafka consumer are described in the [documentation](http://kafka.apache.org/documentation/#consumerconfigs).

### Default Settings

The following Java Kafka consumer properties are overridden by default.

- `auto.offset.reset` is set to `none`, to avoid the surprising `latest` default.

- `enable.auto.commit` is set to `false`, since offset commits are managed manually.

Use `withAutoOffsetReset` and `withEnableAutoCommit` to change these properties.

In addition, there are several settings specific to the library.

- `withCloseTimeout` controls the timeout when waiting for consumer shutdown. Default is 20 seconds.

- `withCommitRecovery` defines how offset commit exceptions are recovered. See [`CommitRecovery.Default`][commitrecovery-default].

- `withCommitTimeout` sets the timeout for offset commits. Default is 15 seconds.

- `withMaxPrefetchBatches` adjusts the maximum number of record batches per topic-partition to prefetch before backpressure is applied. The default is 2, meaning there can be up to 2 record batches per topic-partition waiting to be processed.

- `withPollInterval` alters how often consumer `poll` should take place. Default is 50 milliseconds.

- `withPollTimeout` modifies for how long `poll` is allowed to block. Default is 50 milliseconds.

- `withRecordMetadata` defines what metadata to include in `OffsetAndMetadata` for consumed records. This effectively allows us to store metadata along with offsets when committed to Kafka. The default is for no metadata to be included.

## Consumer Creation

Once [`ConsumerSettings`][consumersettings] is defined, use `KafkaConsumer.stream` to create a [`KafkaConsumer`][kafkaconsumer] instance.

```scala mdoc:silent
object ConsumerExample extends IOApp.Simple {

  val run: IO[Unit] =
    KafkaConsumer.stream(consumerSettings).compile.drain

}
```

There is also `KafkaConsumer.resource` for when it's preferable to work with `Resource`. Both these functions create an underlying Java Kafka consumer and start work in the background to support record streaming. In addition, they both also guarantee resource cleanup (closing the Kafka consumer and stopping background work).

In the example above, we simply create the consumer and then immediately shutdown after resource cleanup. [`KafkaConsumer`][kafkaconsumer] supports much of the Java Kafka consumer functionality in addition to record streaming, but for streaming records, we first have to subscribe to a topic.

## Topic Subscription

We can use `subscribe` with a non-empty collection of topics, or `subscribeTo` for varargs support. There is also an option to `subscribe` using a `Regex` regular expression for the topic names, in case the exact topic names are not known up-front. When allocating a consumer in a `Stream` context, these are available as extension methods directly on the `Stream`.

```scala mdoc:silent
object ConsumerSubscribeExample extends IOApp.Simple {

  val run: IO[Unit] =
    KafkaConsumer.stream(consumerSettings).subscribeTo("topic").compile.drain

}
```

Note that only automatic partition assignment is supported. Like in the [consumer creation](#consumer-creation) section, the example above only creates a consumer (guaranteeing resource cleanup) and subscribes to a topic. No records are yet streamed from the topic, for which we'll have to use `stream` or `partitionedStream`.

## Record Streaming

Once subscribed to at least one topic, we can use `stream` for a `Stream` of [`CommittableConsumerRecord`][committableconsumerrecord]s. Each record contains a deserialized [`ConsumerRecord`][consumerrecord], as well as a [`CommittableOffset`][committableoffset] for managing [offset commits](#offset-commits). Streams guarantee records in topic-partition order, but not ordering across partitions. This is the same ordering guarantee that Kafka provides.

```scala mdoc:silent
object ConsumerStreamExample extends IOApp.Simple {

  val run: IO[Unit] =
    KafkaConsumer.stream(consumerSettings).subscribeTo("topic").records.compile.drain

}
```

Note that this is an infinite stream, meaning it will only terminate if it's interrupted, errors, or if we turn it into a finite stream (using e.g. `take`). It's usually desired that consumer streams keep running indefinitely, so that incoming records get processed quickly &mdash; one notable exception being when testing streams. Also, you could gracefully stop stream using `stopConsuming` method. More info about it in the [graceful shutdown](#graceful-shutdown) section.

When using `stream`, records on all assigned partitions end up in the same `Stream`. Depending on how records are processed, we might want to separate records per topic-partition. This exact functionality is provided by `partitionedStream`.

```scala mdoc:silent
object ConsumerPartitionedStreamExample extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val stream =
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("topic")
        .partitionedRecords
        .map { partitionStream =>
          partitionStream.evalMap { committable =>
            processRecord(committable.record)
          }
        }
        .parJoinUnbounded

    stream.compile.drain
  }

}
```

The `partitionStream` in the example above is a `Stream` of records for a single topic-partition. We define the processing per topic-partition rather than across all partitions, as was the case with `stream`. The example will run `processRecord` on every record, one-at-a-time in-order per topic-partition. However, multiple partitions are processed at the same time when using `parJoinUnbounded`.

Note that we have to use `parJoinUnbounded` here so that all partitions are processed. While `parJoinUnbounded` doesn't limit the number of streams running concurrently, the actual limit is the number of assigned partitions. In fact, `stream` is just an alias for `partitionedStream.parJoinUnbounded`.

Sometimes it could be desirable to not just get streams for each topic-partition (like in `partitionedStream`), but also have additional information, from which topic-partition each stream produces records. There is a `partitionsMapStream` method for that. It has the next signature:

```scala
def partitionsMapStream: Stream[F, Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]]
```

Each element of `partitionsMapStream` contains a current assignment. The current assignment is the `Map`, where keys are a `TopicPartition`, and values are streams with records for a particular `TopicPartition`.

New assignments will be received on each rebalance. On rebalance, Kafka revoke all previously assigned partitions, and after that assigned new partitions all at once. `partitionsMapStream` reflects this process in a streaming manner. It means that you could use `partitionsMapStream` for some custom rebalance handling.

Note, that partition streams for revoked partitions will be closed after the new assignment comes.

When processing of records is independent of each other, as is the case with `processRecord` above, it's often easier and more performant to use `stream` and `mapAsync`, as seen in the example below. Generally, it's crucial to ensure there are no data races between processing of any two records.

```scala mdoc:silent
object ConsumerMapAsyncExample extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val stream =
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("topic")
        .records
        .mapAsync(25) { committable =>
          processRecord(committable.record)
        }

    stream.compile.drain
  }

}
```

## Offset Commits

Offsets commits are managed manually, which is important for ensuring at-least-once delivery. This means that, by [default](#default-settings), automatic offset commits are disabled. If you're sure you don't need at-least-once delivery, you can re-enable automatic offset commits using `withEnableAutoCommit` on [`ConsumerSettings`][consumersettings], and then ignore the [`CommittableOffset`][committableoffset] part of [`CommittableConsumerRecord`][committableconsumerrecord], keeping only the [`ConsumerRecord`][consumerrecord].

### Working on `Chunk`

Use cases that require at-least-once delivery make it necessary to commit the offset of messages only after the message has been successfully processed. Implementing this correctly can be challenging, especially when the business logic requires advanced data manipulation with concurrency, batching, filtering and the like:

- When consuming multiple messages from the same partition concurrently, a consumer might lose messages if the commits happen out of order and a message that is not the last one on its partition can't be processed and has to be retried.
- When filtering messages, it's important to still commit the offset of the filtered message because if this message is the latest one on its partition, it will get re-sent infinitely.
- For performance reasons, it makes sense to batch the offsets when committing them.

The recommended pattern for these use cases is by working on the `Chunk`s of records that are part of the `Stream`. The library supports that with the `consumeChunk` method:

```scala mdoc:silent
object ConsumerChunkExample extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecords(records: Chunk[ConsumerRecord[String, String]]): IO[CommitNow] =
      records.traverse(record => IO.println(s"Processing record: $record")).as(CommitNow)

    KafkaConsumer.stream(consumerSettings).subscribeTo("topic").consumeChunk(processRecords)
  }

}
```

Note that this method uses `partitionedStream`, which means that all the partitions assigned to the consumer will be processed concurrently.

As a user, you don't have to care about the offset commits, all you have to do is implement a function that processes all records in the `Chunk`, and return a `IO[CommitNow]`. After this action finished, the offsets for all messages in the `Chunk` will be committed. `CommitNow` is basically the same as `Unit`, but helps in making it clear when the processing of messages has been finished and it's time to commit.

This brings several benefits:

- **Correctness:** You can focus on implementing your business logic, without having to worry about offset commits or propagating the correct offsets through your code. Offsets are committed correctly afterwards.
- **Performance:** Typical performance improvements are bulk-writes to a database, or using concurrency to speed things up. These patterns can be used liberally when working on the records in a `Chunk`, without having to sacrifice correctness.
- **Flexibility:** Besides using batching and concurrency, you might want to filter out messages, or process them in a different order than they appear on the partitions. As long as you work on a single `Chunk` and make sure that the processing is finished when you return `CommitNow`, you can do all that.
- A concrete example that makes use of these ideas is to group all the messages in the `Chunk` by key and then only process the last message for each key (basically doing what Kafka's log compaction does). In many occasions, it's also possible to process the messages for different keys concurrently, which drastically increases the available concurrency.

If the chunk size doesn't fit your needs, the first way to start tuning is the `max.poll.records` config property of your consumer.

### Committing manually

If `consumeChunk` doesn't work for you, you can always commit your offsets manually.

Offset commits are usually done in batches for performance reasons. We normally don't need to commit every offset, but only the last processed offset. There is a trade-off in how much reprocessing we have to do when we restart versus the performance implication of committing more frequently. Depending on our situation, we'll then choose an appropriate frequency for offset commits.

We should keep the [`CommittableOffset`][committableoffset] in our `Stream` once we've finished processing the record. For at-least-once delivery, it's essential that offset commits preserve topic-partition ordering, so we have to make sure we keep offsets in the same order as we receive them. There is one convenience function for the most common batch committing scenario, `commitBatchWithin`.

```scala mdoc:silent
object ConsumerCommitBatchExample extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val stream =
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("topic")
        .records
        .mapAsync(25) { committable =>
          processRecord(committable.record).as(committable.offset)
        }
        .through(commitBatchWithin(500, 15.seconds))

    stream.compile.drain
  }

}
```

The example above commits once every 500 offsets or 15 seconds, whichever happens first. The batch commit functions uses [`CommittableOffsetBatch`][committableoffsetbatch] and provided [functions][committableoffsetbatch$] for batching offsets.

The batch commit functions uses [`CommittableOffsetBatch`][committableoffsetbatch] and provided [functions][committableoffsetbatch$] for batching offsets. For more involved batch commit scenarios, we can use [`CommittableOffsetBatch`][committableoffsetbatch] to batch offsets, while having custom logic to determine batch frequency.

For at-least-once delivery, offset commit has to be the last step in the stream. Anything that happens after offset commit cannot be part of the at-least-once guarantee. This is the main reason why batch commit functions return `Unit`, as they should always be the last part of the stream definition.

If we're sure we need to commit every offset, we can `commit` individual [`CommittableOffset`][committableoffset]s. Note there is a substantial performance implication to committing every offset, and it should be avoided when possible. The approach also limits parallelism, since offset commits need to preserve topic-partition ordering.

## Graceful shutdown

With the fs2-kafka you could gracefully shutdown a `KafkaConsumer`. Consider this example:

```scala mdoc:silent
object NoGracefulShutdownExample extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: CommittableConsumerRecord[IO, String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    def run(consumer: KafkaConsumer[IO, String, String]): IO[Unit] = {
      consumer.subscribeTo("topic") >> consumer
        .stream
        .evalMap { msg =>
          processRecord(msg).as(msg.offset)
        }
        .through(commitBatchWithin(100, 15.seconds))
        .compile
        .drain
    }

    KafkaConsumer.resource(consumerSettings).use(run)
  }

}
```

When this application will be closed (for example, using Ctrl + C in the terminal) the `stream` inside the `run` function will be simply interrupted. It means that there is no guarantee that all in-flight records will be processed to the end of the stream, and there is no guarantee that all records will pass through all stream steps. For example, a record could be processed in the `processRecord`, but not committed. Note that even when a stream is interrupted all resources will be safely closed.

Usually, this is normal behavior for Kafka consumers because most of them work with the _at least once_ semantics. But sometimes, it is necessary to process all in-flight messages and close the `KafkaConsumer` instance only after that.

To achieve this behavior we could use a `stopConsuming` method on a` KafkaConsumer`. Calling this method has the next effects:

1. After this call no more data will be fetched from Kafka through the `poll` method.
2. All currently running streams will continue to run until all in-flight messages will be processed.
   It means that streams will be completed when all fetched messages will be processed.

We could combine `stopConsuming` with the custom resource handling and implement a graceful shutdown. Let's try it. For cats-effect 2 it may looks like this:

```scala mdoc:silent
import cats.effect.{Deferred, Ref}

object WithGracefulShutdownExampleCE2 extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: CommittableConsumerRecord[IO, String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    def run(consumer: KafkaConsumer[IO, String, String]): IO[Unit] = {
      consumer.subscribeTo("topic") >> consumer
        .stream
        .evalMap { msg =>
          processRecord(msg).as(msg.offset)
        }
        .through(commitBatchWithin(100, 15.seconds))
        .compile
        .drain
    }

    def handleError(e: Throwable): IO[Unit] = IO(println(e.toString))

    for {
      stoppedDeferred            <- Deferred[IO, Either[Throwable, Unit]] // [1]
      gracefulShutdownStartedRef <- Ref[IO].of(false)                     // [2]
      _                          <- KafkaConsumer
             .resource(consumerSettings)
             .allocated
             .bracketCase {
               case (consumer, _) => // [3]
                 run(consumer)
                   .attempt
                   .flatMap { result: Either[Throwable, Unit] => // [4]
                     gracefulShutdownStartedRef
                       .get
                       .flatMap {
                         case true  => stoppedDeferred.complete(result) // [5]
                         case false => IO.pure(result).rethrow          // [6]
                       }
                   }
                   .uncancelable // [7]
             } {
               case ((consumer, closeConsumer), exitCase) => // [8]
                 (exitCase match {
                   case Outcome.Errored(e) => handleError(e) // [9]
                   case _                  =>
                     for {
                       _          <- gracefulShutdownStartedRef.set(true) // [10]
                       _          <- consumer.stopConsuming               // [11]
                       stopResult <-
                         stoppedDeferred
                           .get // [12]
                           .timeoutTo(
                             10.seconds,
                             IO.pure(Left(new RuntimeException("Graceful shutdown timed out")))
                           ) // [13]
                       _ <- stopResult match { // [14]
                              case Right(()) => IO.unit
                              case Left(e)   => handleError(e)
                            }
                     } yield ()
                 }).guarantee(closeConsumer) // [15]
             }
    } yield ()
  }

}
```

1. We need a `Deferred` to wait until records processing is finished.
2. Also, we need some flag to distinguish between graceful and regular shutdown. It would be needed for error handling.
3. We need somehow implement our custom closing logic. To do this we can use `allocated` with the `bracketCase` instead of `use` on the consumer resource. This is a low-level API for `Resource` specifically for cases like this.
4. In the `use` section of `bracketCase` we start our main application logic. When graceful shutdown will be started, the `run` function will return either some result (in our case there is no result) or failed with an error. This result should be passed to a `stoppedDeferred`. To not lose errors we should use `attempt` on this result to convert it to an `Either[Throwable, Unit]`.
5. If a graceful shutdown started, we pass `result` to a `stoppedDeferred`.
6. If a graceful shutdown is not started we pass `result` further with the `rethrow`. This case is needed mostly for cases when the `run` function failed with an error during its work.
7. It's important to wrap all our application logic in the `uncancelable`. Without it when the graceful shutdown will be started `run` method will be just interrupted, and `stoppedDeferred` will be never resolved.
8. Here we started our custom closing logic.
9. If our main app logic failed with an error, we should not start a graceful shutdown, we should close consumer regularly. We may also handle an error somehow, for example, log an error.
10. If there were no errors during application work, we may start a graceful shutdown.
11. Stopping our consumer. After this call stream inside the `run` function will receive only already fetched records and after that finish.
12. Waiting until the `run` function finished with some result and resolved `stoppedDeferred`.
13. Let's add a timeout for our graceful shutdown. This is not absolutely necessary, but if your processing contains many steps, a graceful shutdown may take a while.
14. When `stoppedDeferred` returns some result, we could somehow handle it. For example, we could handle an error case.
15. Don't forget to call the `closeConsumer` function.

For cats-effect 3 the example above will not work as in cats-effect 2, because in cats-effect 3 cancelation semantics [was changed](https://typelevel.org/cats-effect/docs/typeclasses/monadcancel). Here is the example of graceful shutdown for cats-effect 3:

```scala mdoc:silent
object WithGracefulShutdownExampleCE3 extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: CommittableConsumerRecord[IO, String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    def run(consumer: KafkaConsumer[IO, String, String]): IO[Unit] = {
      consumer.subscribeTo("topic") >> consumer
        .stream
        .evalMap { msg =>
          processRecord(msg).as(msg.offset)
        }
        .through(commitBatchWithin(100, 15.seconds))
        .compile
        .drain
    }

    KafkaConsumer
      .resource(consumerSettings)
      .use { consumer =>          // [1]
        IO.uncancelable { poll => // [2]
          for {
            runFiber <- run(consumer).start            // [3]
            _        <- poll(runFiber.join).onCancel { // [4]
                   for {
                     _               <- IO(println("Starting graceful shutdown"))
                     _               <- consumer.stopConsuming // [5]
                     shutdownOutcome <-
                       runFiber
                         .join
                         .timeoutTo[Outcome[IO, Throwable, Unit]]( // [6]
                           20.seconds,
                           IO.pure(
                             Outcome.Errored(new RuntimeException("Graceful shutdown timed out"))
                           )
                         )
                     _ <- shutdownOutcome match { // [7]
                            case Outcome.Succeeded(_) =>
                              IO(println("Succeeded in graceful shutdown"))
                            case Outcome.Canceled() =>
                              IO(println("Canceled in graceful shutdown")) >> runFiber.cancel
                            case Outcome.Errored(e) =>
                              IO(println("Failed to shutdown gracefully")) >> runFiber.cancel >> IO
                                .raiseError(e)
                          }
                   } yield ()
                 }
          } yield ()
        }
      }
  }

}
```

`processRecord` and `run` functions are the same. But resource handling part is changed:

1. Here we allocated `KafkaConsumer` as a resource. Unlike the cats-effect 2 example, you don't need to use `allocated`.
2. We created an `uncancelable` block. You can get more information about this in the [`MonadCancel` docs](https://typelevel.org/cats-effect/docs/typeclasses/monadcancel). But shortly — everything inside an `uncancelable` block ignores `cancel` signals. But if you want to create a _cancelable_ block inside, you can use `poll` for this. We will use it in our example.
3. We started our application's main logic by calling the `run` function. An important note here: we are starting in a separate fiber. It's because we want to manually control the lifecycle of this fiber.
4. We called the `join` method on `runFiber` to wait for fiber completion. This call is wrapped in the `poll` function to make this awaiting cancellable. Also, on this line, we are calling the `onCancel` callback to add custom logic for cancelation.
5. If `runFiber.join` call is canceled we have to call the `stopConsuming` to get a graceful shutdown.
6. We joined on `runFiber` one more time, this time after the `stopConsuming` call. After the `stopConsuming` call the `runFiber` will work until an internal `IO` is finished. And internal `IO` finishes when a consumer stream will be finished. That's the main graceful shutdown point. Also, we added the `timeoutTo` on `join` to not wait for too long.
7. We handled all possible outcomes after `runFiber.join`. In the case of `Outcome.Canceled` (note, that it means that `join` call is canceled, not the fiber itself) we have to cancel `runFiber` manually. In the case of `Outcome.Errored` we again have to cancel `runFiber`, and also it will be useful to re-raise an error.

You may notice, that actual graceful shutdown implementation requires a decent amount of low-level handwork. `stopConsuming` is just a building block for making your own graceful shutdown, not a ready-made solution for all needs. This design is intentional, because different applications may need different graceful shutdown logic. For example, what if your application has multiple consumers? Or some other components in your application may also need to participate in a graceful shutdown somehow? Because of that graceful shutdown with `stopConsuming` considered as a low level and advanced feature.

Also note, that even if you implement a graceful shutdown your application may fall with an error. And in this case, a graceful shutdown will not be invoked. It means that your application should be ready to an _at least once_ semantic even when a graceful shutdown is implemented. Or, if you need an _exactly once_ semantic, consider using [transactions](transactions.md).

### Graceful partition revoke

In addition to graceful shutdown of the whole consumer there is an option to configure your consumer to wait for the streams
to finish processing partition before "releasing" it. Behavior can be enabled via the following settings:

```scala mdoc:silent
object WithGracefulPartitionRevoke extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: CommittableConsumerRecord[IO, String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    def run(consumer: KafkaConsumer[IO, String, String]): IO[Unit] = {
      consumer.subscribeTo("topic") >> consumer
        .stream
        .evalMap { msg =>
          processRecord(msg).as(msg.offset)
        }
        .through(commitBatchWithin(100, 15.seconds))
        .compile
        .drain
    }

    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withRebalanceRevokeMode(RebalanceRevokeMode.Graceful)
        .withSessionTimeout(2.seconds)

    KafkaConsumer
      .resource(consumerSettings)
      .use { consumer =>
        run(consumer)
      }
  }

}
```

Please note that this setting does not guarantee that all the commits will be performed before partition is revoked and
that `session.timeout.ms` setting is set to lower value. Be aware that awaiting too long for partition processor
to finish will cause processing of the whole topic to be suspended.

Awaiting for commits to complete might be implemented in the future.

[commitrecovery-default]: @API_BASE_URL@/CommitRecovery$.html#Default:fs2.kafka.CommitRecovery
[committableconsumerrecord]: @API_BASE_URL@/CommittableConsumerRecord.html
[committableoffset]: @API_BASE_URL@/CommittableOffset.html
[committableoffsetbatch]: @API_BASE_URL@/CommittableOffsetBatch.html
[committableoffsetbatch$]: @API_BASE_URL@/CommittableOffsetBatch$.html
[consumerconfig]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/consumer/ConsumerConfig.html
[consumerrecord]: @API_BASE_URL@/ConsumerRecord.html
[consumersettings]: @API_BASE_URL@/ConsumerSettings.html
[deserializer]: @API_BASE_URL@/Deserializer.html
[deserializer$]: @API_BASE_URL@/Deserializer$.html
[headers]: @API_BASE_URL@/Headers.html
[java-kafka-consumer]: @KAFKA_API_BASE_URL@/?org/apache/kafka/clients/consumer/KafkaConsumer.html
[kafkaconsumer]: @API_BASE_URL@/KafkaConsumer.html
[deserializationexception]: @API_BASE_URL@/DeserializationException.html
[unexpectedtopicexception]: @API_BASE_URL@/UnexpectedTopicException.html
