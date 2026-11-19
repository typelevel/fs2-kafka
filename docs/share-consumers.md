# Share Consumers

Kafka 4.x adds share groups ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)): records of a topic are handed out in a competing consumers style, like a queue, instead of by partition. Each acquired record is locked for a limited time, and the consumer decides its fate: accept, release (deliver again), reject, or renew the lock.

Share consumers are built on the [Java share consumer][java-share-consumer].

> **Experimental.** The module targets Kafka's share groups, introduced by KIP-932.

Share consumers are in a separate module.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@SHARE_MODULE_NAME@" % "@VERSION@"
```

## Prerequisites

- **Share groups have to be enabled on the brokers.** The `share.version` feature turns them on, and it may be off on a new cluster: `kafka-features.sh --bootstrap-server <broker> upgrade --feature share.version=1`. Renewing locks also needs a broker with [KIP-1222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222%3A+Acquisition+lock+timeout+renewal+for+share+consumers).

- **A new share group starts at the latest offset.** To read a topic from the beginning, set `share.auto.offset.reset` to `earliest` for the group: `kafka-configs.sh --bootstrap-server <broker> --alter --entity-type groups --entity-name share-workers --add-config share.auto.offset.reset=earliest`.

The following imports are assumed throughout this page.

```scala mdoc:silent
import scala.concurrent.duration._

import cats.effect._
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2._
import fs2.kafka._
```

## When to Use Share Consumers

Share consumers fit best when:

- **records are independent jobs**, and their order does not matter,

- **more consumers are needed than there are partitions**,

- **processing time varies**, as a slow record holds back only its own consumer, and

- **records are retried one at a time**, without affecting the others.

A regular [`KafkaConsumer`](consumers.md) fits better when:

- **order matters**, as share groups give no ordering guarantee, not even per partition,

- **exactly-once processing is needed**, as share groups do not take part in transactions,

- **processing depends on the partition**, such as state per key or seeking, or

- **processing is fast and uniform**, as committing one offset per partition is less work than acknowledging every record.

## Settings

We create @:api(fs2.kafka.ShareConsumerSettings) like `ConsumerSettings`, with the key and value deserializers.

```scala mdoc:silent
val settings = ShareConsumerSettings[IO, String, String]
  .withBootstrapServers("localhost:9092")
  .withGroupId("share-workers")
  .withMaxPollRecords(100)
```

Two defaults differ from Java's: the acknowledgement mode is `explicit`, and the acquire mode is `record_limit`. See [acknowledgement and acquire modes](#acknowledgement-and-acquire-modes).

These settings are specific to the library:

- `withAutoRenewal` renews unacknowledged records before their lock expires. Default is `false`. See [renewal](#renewal).

- `withRenewalMargin` sets how long before the lock ends a record is renewed. Default is 5 seconds.

- `withMaxProcessingTime` limits how long a record can stay unacknowledged. No limit by default. See [renewal](#renewal).

- `withFailureAcknowledgement` sets what happens to records the consumer has to acknowledge itself: `Release` or `Reject`. Default is `Release`. See [consuming](#consuming).

- `withMaxAcknowledgementDelay` sets how long an acknowledgement can wait before it is sent. Default is 1 second. See [acknowledging](#acknowledging).

- `withPauseWhen` stops polling while a `Signal[F, Boolean]` is `true`. See [pausing](#pausing).

- `withCommitTimeout` sets how long stopping waits for the broker to confirm the last acknowledgements. Default is 15 seconds.

- `withCloseTimeout`, `withMaxPollRecords` and `withPollTimeout` work as in `ConsumerSettings`. Their defaults are 20 seconds, 500 and 50 milliseconds.

## Consuming

A @:api(fs2.kafka.KafkaShareConsumer) runs the same loop you would write with the Java consumer. It polls, passes the records to your `process` function, and polls again when `process` returns. `consume` runs this loop until `stopConsuming` is called.

Each record is an @:api(fs2.kafka.AcknowledgeableConsumerRecord), with an @:api(fs2.kafka.AcknowledgeableOffset) to acknowledge it. We acknowledge a record once it is processed. The records of one poll can be processed in parallel.

```scala mdoc:silent
def process(record: ConsumerRecord[String, String]): IO[Unit] =
  IO.println(s"${record.key}: ${record.value}")

val program: IO[Unit] =
  KafkaShareConsumer.resource(settings).use { consumer =>
    consumer.subscribeTo("jobs") *>
      consumer.consume { records =>
        records.parTraverseN(32) { record =>
          process(record.record) *> record.accept
        }.void
      }
  }
```

| `AcknowledgeType` | Effect |
| --- | --- |
| `Accept` | The record was processed |
| `Release` | The record is delivered again |
| `Reject` | The record is not delivered again |
| `Renew` | The record's acquisition lock is extended; it needs another acknowledgement later |

Delivery is at-least-once, so processing has to tolerate duplicates. We can release a failed record to try it again, or reject it if it can never succeed.

```scala mdoc:silent
def processOrRelease(record: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(record.record).attempt.flatMap {
    case Right(_) => record.accept
    case Left(_)  => record.release
  }
```

When `process` returns or fails, some records may still be unacknowledged. The consumer acknowledges them itself:

- By default they are released. With `withFailureAcknowledgement(AcknowledgeType.Reject)`, they are rejected.

- If `process` fails, `consume` fails with the same error.

- If `process` is canceled, its records are always released.

- A failing `process` affects every unacknowledged record of the poll. With `Reject`, handle failures per record instead.

Acknowledging a record after `process` returned fails with a `RecordNotInFlightException`.

## Acknowledging

There are two ways to acknowledge:

- `record.accept`, `release`, `reject` and `renew` acknowledge one record, as does `acknowledge(AcknowledgeType)`. They are available on the record and on its `offset`.

- `acknowledgeChunks` is a pipe which acknowledges a whole chunk at once, with one call per run of records of the same consumer. For example, to accept a whole poll: `Stream.chunk(records).map(_.offset -> AcknowledgeType.Accept).through(acknowledgeChunks[IO])`.

As in Java, an acknowledgement is first only recorded in the consumer. There is no commit to call. The consumer sends the pending acknowledgements:

- with the next poll,

- when the oldest one has waited for `withMaxAcknowledgementDelay`,

- immediately while paused, and

- when `consume` ends.

Until it is sent, a record can be acknowledged again, and the last acknowledgement wins. After that, acknowledging it fails with a `RecordNotInFlightException`. The same happens for a record the consumer no longer holds. `acknowledgeChunks` skips these failures.

The broker only sees an acknowledgement when it is sent. If the record's lock has expired by then, the broker rejects it and delivers the record again.

## Deserialization Failures

If a record fails to deserialize, `consume` fails, and the poll's records get the failure acknowledgement. The same record then fails the next consumer too, until the broker archives it at the delivery count limit.

To avoid that, we can use `attempt` deserializers. They return failures as values, so we can reject the bad records.

```scala mdoc:silent
val attemptSettings =
  ShareConsumerSettings(
    keyDeserializer = Deserializer[IO, String],
    valueDeserializer = Deserializer[IO, String].attempt
  ).withBootstrapServers("localhost:9092")
    .withGroupId("share-workers")

def processOrReject(
  record: AcknowledgeableConsumerRecord[IO, String, Either[Throwable, String]]
): IO[Unit] =
  record.record.value match {
    case Right(value) =>
      IO.println(value) *> record.accept
    case Left(_) =>
      record.reject
  }
```

## Renewal

A record has to be acknowledged before its acquisition lock expires, which is 30 seconds by default. Otherwise it is delivered again, possibly to another consumer.

As in Java, locks are not renewed automatically. We can renew a record with `renew`, as often as needed, before we acknowledge it. A renewal covers every unacknowledged record of the poll, and it does not wait for the broker. Records acknowledged in the meantime keep their acknowledgement, and no new records are polled until the renewal is done. If the broker rejects a renewal, the records are delivered again.

With `withAutoRenewal(true)`, the consumer renews unacknowledged records itself. It does so `withRenewalMargin` before the lock ends, and at least every half of `max.poll.interval.ms`. Enable it when processing can take longer than the lock.

- **Without auto-renewal**, keep processing within the lock and `max.poll.interval.ms`, as in Java.

- **With auto-renewal**, a record which hangs is renewed for as long as `process` runs. Set `withMaxProcessingTime` to limit that. A record held longer gets the failure acknowledgement, and acknowledging it later fails with a `RecordNotInFlightException`. The time counts from the poll, so set it well above the time needed for a whole poll.

```scala mdoc:silent
val renewing: IO[Unit] =
  KafkaShareConsumer
    .resource(settings.withAutoRenewal(true).withMaxProcessingTime(10.minutes))
    .use { consumer =>
      consumer.subscribeTo("jobs") *>
        consumer.consume { records =>
          records.parTraverseN(32)(processOrRelease).void
        }
    }
```

## Pausing

Sometimes processing cannot succeed for a while, for example when a service it needs is down. If we kept polling, every record would fail, and its delivery count would grow until the broker archives it.

With `withPauseWhen(signal)`, the consumer stops polling for new records while the signal is `true`.

```scala mdoc:silent
import fs2.concurrent.SignallingRef

// Retries until the service is back, while auto-renewal holds the record's lock.
def processRetrying(record: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(record.record).handleErrorWith(_ => IO.sleep(5.seconds) *> processRetrying(record))

def pausable(paused: SignallingRef[IO, Boolean]): IO[Unit] =
  KafkaShareConsumer
    .resource(settings.withAutoRenewal(true).withPauseWhen(paused))
    .use { consumer =>
      consumer.subscribeTo("jobs") *>
        consumer.consume { records =>
          records.parTraverseN(32)(record => processRetrying(record) *> record.accept).void
        }
    }
```

When to pause is up to us. A circuit breaker or a health check can set the signal. While paused:

- Records already polled are processed and acknowledged as usual. Their acknowledgements are sent immediately, and auto-renewal keeps working.

- If no records are held and the pause is longer than `max.poll.interval.ms`, Java leaves the share group. It rejoins with the next poll.

- If the last poll before the pause returned nothing, Java may still acquire records once. These records are delivered after the pause, with a delivery count one higher.

## Acknowledgement and Acquire Modes

| Property | Value | Behaviour |
| --- | --- | --- |
| `share.acknowledgement.mode` | `explicit` (default here) | Every record needs an acknowledgement before the next poll; use `consume` |
| | `implicit` (Java's default) | A poll is accepted once `process` returned; use `autoConsume` |
| `share.acquire.mode` | `record_limit` (default here) | A poll returns at most `max.poll.records` records |
| | `batch_optimized` (Java's default) | A poll may return more, to align with the broker's record batches |

### Implicit Acknowledgement

In implicit mode, records cannot be acknowledged one by one, so `consume` fails with an `ImplicitAcknowledgementException`. We use `autoConsume` instead. It passes plain `ConsumerRecord`s to `process`, and accepts the whole poll when `process` returns.

If `process` fails or is canceled, the poll is not accepted. The consumer then refuses to consume again, because Java would accept the records with its next poll. Close it to release the records, and create a new one.

```scala mdoc:silent
val implicitMode: IO[Unit] =
  KafkaShareConsumer
    .resource(settings.withAcknowledgementMode(ShareAcknowledgementMode.Implicit))
    .use { consumer =>
      consumer.subscribeTo("jobs") *>
        consumer.autoConsume { records =>
          records.parTraverseN(32)(process).void
        }
    }
```

### Acquisition

With `batch_optimized`, a poll can return up to one broker record batch more than `max.poll.records`. Java may also buffer records before we poll them, and their locks are already running then.

Larger polls can improve throughput when processing is fast and uniform. But a slow record then holds back more records, and more renewals are needed.

## Several Consumers

One consumer processes one poll at a time, so a slow record delays the next poll. To avoid that, we can run several consumers in the same share group. Each has its own Java consumer and its own loop.

```scala mdoc:silent
val parallelProgram: IO[Unit] =
  KafkaShareConsumers.resource(settings, consumers = 4).use { consumers =>
    consumers.subscribeTo("jobs") *>
      consumers.consume { records =>
        records.parTraverseN(8)(processOrRelease).void
      }
  }
```

`consume` runs on every consumer until `stopConsuming`. If one consumer fails, the others are canceled, their records are released, and the error is raised.

If `client.id` is set and there are several consumers, each gets its index as a suffix, starting from 0: `client.id-0`, `client.id-1`, and so on. `metrics` returns the metrics of each consumer by its client id.

## Stopping and Shutdown

- `stopConsuming` lets the current poll finish and sends the last acknowledgements with a final commit. Then `consume` returns. If the broker rejects these acknowledgements, `consume` fails with the broker's error. If the broker does not answer within the commit timeout, it fails with a `ShareCommitTimeoutException`. Stopping is final: to consume again, create a new consumer.

- Releasing the resource closes the Java consumer. It sends the pending acknowledgements and releases the records that are still unacknowledged.

[java-share-consumer]: @KAFKA_API_BASE_URL@/org/apache/kafka/clients/consumer/KafkaShareConsumer.html
