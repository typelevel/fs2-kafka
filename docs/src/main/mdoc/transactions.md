---
id: transactions
title: Transactions
---

Kafka transactions are supported through a [`KafkaProducer`][transactionalkafkaproducer] instantiated via one of the `KafkaProducer.transactional` or `KafkaProducer.transactionalStream` methods.

For details on [consumers](consumers.md) and [producers](producers.md), see the respective sections.

The following imports are assumed throughout this page.

```scala mdoc:silent
import scala.concurrent.duration._

import cats.effect.{IO, IOApp}
import fs2.kafka._
import fs2.Stream

import org.apache.kafka.common.TopicPartition
```

## Producing and Committing Offsets

To produce data and commit offsets transactionally, we first need to create a `KafkaConsumer` and `ProducerSettings`. The `ProducerSettings` must have idempotence enabled, retries set to a value greater than zero, and a transactional ID configured.

```scala mdoc:silent
val consumerSettings =
  ConsumerSettings[IO, String, String]
    .withIsolationLevel(IsolationLevel.ReadCommitted)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")

val producerSettings =
  ProducerSettings[IO, String, String]
    .withBootstrapServers("localhost:9092")
    .withEnableIdempotence(true)
    .withRetries(10)
    .withTransactionId("transactional-id")
```

Once the settings are created, we can instantiate a `KafkaProducer` using `KafkaProducer.transactional` or `KafkaProducer.transactionalStream`. We then create `CommittableProducerRecords`, wrap them in `TransactionalProducerRecords`, and use the `produceAndCommitTransactionally` method.

> Note that calls to `produceAndCommitTransactionally` are sequenced in the `KafkaProducer` to ensure that, when used concurrently, transactions don't run into each other resulting in an invalid transaction transition exception.
>
> Because the `KafkaProducer` waits for the record batch to be flushed and the transaction committed on the broker, this could lead to performance bottlenecks where a single producer is shared among many threads. To ensure the performance of `KafkaProducer` aligns with your performance expectations when used concurrently, it is recommended you create a pool of transactional producers.

Following is an example where transactions are used to consume, process, produce, and commit.

```scala mdoc
object Main extends IOApp.Simple {

  val run: IO[Unit] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[(String, String)] =
      IO.pure(record.key -> record.value)

    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withIsolationLevel(IsolationLevel.ReadCommitted)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")

    def producerSettings(partition: TopicPartition) =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:9092")
        .withEnableIdempotence(true)
        .withRetries(10)
        .withTransactionId(s"transactional-id-$partition")

    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo("topic")
      .flatMap(_.partitionsMapStream)
      .map(
        _.map { case (partition, stream) =>
          KafkaProducer
            .transactionalStream(producerSettings(partition))
            .flatMap { producer =>
              stream
                .mapAsync(25) { committable =>
                  processRecord(committable.record).map { case (key, value) =>
                    val record = ProducerRecord("topic", key, value)
                    CommittableProducerRecords.one(record, committable.offset)
                  }
                }
                .groupWithin(500, 15.seconds)
                .evalMap(producer.produceAndCommitTransactionally)
            }
        }
      )
      .flatMap { partitionsMap =>
        Stream.emits(partitionsMap.toVector).parJoinUnbounded
      }
      .compile
      .drain
  }

}
```

## Producing Only Data

To produce only data transactionally, without committing offsets, we can use the `produceTransactionally` method. Similar to producing and committing offsets, we need a transactional `KafkaProducer` and `TransactionalProducerRecords`.

## Fine-grained Control

The following methods provide more control over the transaction lifecycle:

- `initializeTransactions`: initializes the underlying Kafka Producer transaction mechanism.
- `transaction`: a resource that represents a transaction, handling commit and abort automatically.
- `commitOffsets`: allows committing offsets using the producer within a transaction.
- `produce`: can be used within a transaction to produce records.

[transactionalkafkaproducer]: @API_BASE_URL@/TransactionalKafkaProducer.html
