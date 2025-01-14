---
id: quick-example
title: Quick Example
---

Following is an example showing how to:

- use `KafkaConsumer#records` in order to stream records from Kafka,
- use `produce` to produce newly created records to Kafka,
- use `commitBatchWithin` to commit consumed offsets in batches.

```scala mdoc
import cats.effect.{IO, IOApp}
import fs2._
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow

object Main extends IOApp.Simple {

  val run: IO[Unit] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings[IO, String, String].withBootstrapServers("localhost:9092")

    def processRecords(
      producer: KafkaProducer[IO, String, String]
    )(records: Chunk[ConsumerRecord[String, String]]): IO[CommitNow] = {
      val producerRecords = records.map(consumerRecord =>
        ProducerRecord("topic", consumerRecord.key, consumerRecord.value)
      )
      producer.produce(producerRecords).flatten.as(CommitNow)
    }

    val stream =
      KafkaProducer
        .stream(producerSettings)
        .evalMap { producer =>
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo("topic")
            .consumeChunk(chunk => processRecords(producer)(chunk))
        }

    stream.compile.drain
  }

}
```
