---
id: otel4s-trace
title: OTel4s Trace
---

The `@OTEL4S_TRACE_MODULE_NAME@` module adds tracing support for `fs2-kafka` using
[otel4s](https://typelevel.org/otel4s).

Add it to your project in `build.sbt`:

```scala
libraryDependencies += "org.typelevel" %% "@OTEL4S_TRACE_MODULE_NAME@" % "@LATEST_VERSION@"
```

The following imports are assumed throughout this page.

```scala mdoc:silent
import cats.Parallel
import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.kafka._
import fs2.kafka.otel4s.trace._
import fs2.kafka.otel4s.trace.syntax._
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.TracerProvider
```

## Overview

This module is explicit:

- it does not auto-instrument `KafkaProducer` or `KafkaConsumer`
- it does not trace message processing unless you mark that boundary yourself
- producer and consumer tracing use different APIs

It models three operations:

- `send` for producer sends
- `receive` for delivery of records to application code
- `process` for handling an individual record

## Basic Usage

Create `KafkaTracer` from the ambient `TracerProvider`, then bind it to a producer or consumer.

```scala mdoc:silent
def bindProducer[F[_]: Async: Parallel: TracerProvider](
  producer: KafkaProducer[F, String, String]
): F[TracedKafkaProducer[F, String, String]] =
  KafkaTracer.create[F](KafkaTracer.Config.default).map(_.producer(producer))

def bindConsumer[F[_]: Async: Parallel: TracerProvider](
  consumer: KafkaConsumer[F, String, String]
): F[TracedKafkaConsumer[F, String, String]] =
  KafkaTracer.create[F](KafkaTracer.Config.default).map(_.consumer(consumer))
```

Create the traced handle once per Kafka client and reuse it.

The producer and consumer APIs are different:

- `TracedKafkaProducer` is an explicit producer-side tracing handle
- `TracedKafkaConsumer` is a consumer-bound tracing handle with explicit `receive` and `process`

## Producer Usage

Bind a traced producer once, then use the awaited send helpers.

```scala mdoc:silent
def tracedProducerResource[F[_]: Async: Parallel: TracerProvider](
  settings: ProducerSettings[F, String, String]
): Resource[F, TracedKafkaProducer[F, String, String]] =
  for {
    producer    <- KafkaProducer.resource(settings)
    kafkaTracer <- Resource.eval(KafkaTracer.create[F](KafkaTracer.Config.default))
  } yield kafkaTracer.producer(producer)

def publishOne[F[_]: Async: Parallel: TracerProvider](
  settings: ProducerSettings[F, String, String]
): Resource[F, F[Unit]] =
  tracedProducerResource(settings).map(
    _.produceOneAwaited(ProducerRecord("topic", "key", "value")).void
  )
```

The main producer operations are:

- `injectHeaders` to propagate trace context without sending
- `produceAwaited` to send a batch and await completion
- `produceOneAwaited` to send one record and await completion
- `produceTransactionally` and `produceAndCommitTransactionally` for transactional flows
- `underlying` to access the original `KafkaProducer` when needed

### Propagation Without Sending

```scala mdoc:silent
def prepareRecord[F[_]](
  record: ProducerRecord[String, String]
)(implicit tracedProducer: TracedKafkaProducer[F, String, String]): F[ProducerRecord[String, String]] =
  tracedProducer.injectHeaders(record)
```

## Consumer Usage

Consumer tracing is explicit because processing happens in application code.

### `process` Only

If you only want per-record spans, wrap the business logic with `process`.

```scala mdoc:silent
def consumeWithProcessOnly[F[_]: Async: Parallel: TracerProvider](
  settings: ConsumerSettings[F, String, String]
): Stream[F, Unit] =
  for {
    consumer    <- KafkaConsumer.stream(settings)
    kafkaTracer <- Stream.eval(KafkaTracer.create[F](KafkaTracer.Config.default))
    _           <- Stream.eval(consumer.subscribeTo("topic"))
    tracedConsumer = kafkaTracer.consumer(consumer)
    _ <- tracedConsumer.records.evalMap { record =>
           tracedConsumer.process(record)(Async[F].unit)
         }
  } yield ()
```

### `receive` Then `process`

Use `receive` when you want a delivery span separate from the processing span.

```scala mdoc:silent
def consumeWithExplicitReceive[F[_]: Async: Parallel: TracerProvider](
  settings: ConsumerSettings[F, String, String]
): Stream[F, Unit] =
  for {
    consumer    <- KafkaConsumer.stream(settings)
    kafkaTracer <- Stream.eval(KafkaTracer.create[F](KafkaTracer.Config.default))
    _           <- Stream.eval(consumer.subscribeTo("topic"))
    tracedConsumer = kafkaTracer.consumer(consumer)
    partition <- consumer.partitionedStream
    _ <- partition.chunks.evalMap { chunk =>
           tracedConsumer.receiveCommittable(chunk) {
             chunk.traverse_(record => tracedConsumer.process(record)(Async[F].unit))
           }
         }
  } yield ()
```

### `recordsWithProcess`

For the common stream shape, use `recordsWithProcess`.

```scala mdoc:silent
def consumeWithHelper[F[_]: Async: Parallel: TracerProvider](
  settings: ConsumerSettings[F, String, String]
): Stream[F, Unit] =
  for {
    consumer    <- KafkaConsumer.stream(settings)
    kafkaTracer <- Stream.eval(KafkaTracer.create[F](KafkaTracer.Config.default))
    _           <- Stream.eval(consumer.subscribeTo("topic"))
    tracedConsumer = kafkaTracer.consumer(consumer)
    _ <- tracedConsumer.recordsWithProcess(_ => Async[F].unit)
  } yield ()
```

`recordsWithProcess` does two things:

- `receive` covers handoff of a delivered chunk to application code
- `process` covers the per-record business logic
- the chunk-level `receive` span ends before the per-record processing work begins

If you intentionally want only per-record `process` spans, use `recordsWithProcessOnly`.

## Syntax Imports

`syntax._` adds convenience extensions for already-bound traced handles.

```scala mdoc:silent
def injectViaSyntax[F[_]](
  record: ProducerRecord[String, String]
)(implicit tracedProducer: TracedKafkaProducer[F, String, String]): F[ProducerRecord[String, String]] =
  record.injectTracingHeaders

def traceReceiveViaSyntax[F[_]](
  records: Chunk[ConsumerRecord[String, String]]
)(fa: F[Unit])(implicit tracedConsumer: TracedKafkaConsumer[F, String, String]): F[Unit] =
  records.traceReceive(fa)

def traceProcessViaSyntax[F[_]](
  record: ConsumerRecord[String, String]
)(fa: F[Unit])(implicit tracedConsumer: TracedKafkaConsumer[F, String, String]): F[Unit] =
  record.traceProcess(fa)
```

Use these helpers when they make the call site shorter. If the direct methods are clearer, use
those instead.

## Configuration

`KafkaTracer.Config` customizes the emitted spans.

```scala mdoc:silent
val kafkaTracingConfig =
  KafkaTracer
    .Config
    .default
    .withServerAddress("kafka.internal", Some(9092))
    .addConstAttributes(
      Attribute("module.component", "payments")
    )
```

The main configuration hooks are:

- `withConstAttributes` and `addConstAttributes` for attributes added to every emitted span
- `withServerAddress` for logical Kafka endpoint attributes
- `withSendSpanSetup` for producer-side `send` span naming and extra attributes
- `withReceiveSpanSetup` for consumer-side `receive` span naming and extra attributes
- `withProcessSpanSetup` for consumer-side `process` span naming and extra attributes

Attribute precedence is:

1. semantic-convention attributes derived from the traced records
2. configured constant attributes
3. attributes from the specific span setup

Later layers override earlier ones when keys overlap.

## What Spans Are Emitted

The module emits:

- producer `send` spans
- per-message `create` spans for multi-record sends when needed to preserve message creation
  context
- consumer `receive` spans
- consumer `process` spans

Current limitations:

- transactional offset commit / settlement is not modeled as a separate settlement span
- `records` on its own is not traced; only explicit `receive` or `process` boundaries are traced

## Notes

- Bind traced producers and traced consumers once per Kafka client. Recreating them per record or
  per chunk adds noise and unnecessary overhead.
- `receive` and `process` represent different boundaries. Use `receive` for delivery and `process`
  for actual business logic.
- Consumer-side spans link to propagated message creation context by default. They do not adopt the
  producer span as a normal parent-child relationship.
- Logical broker endpoint identity is not auto-discovered from `fs2-kafka`. If you want
  `server.address` and `server.port`, configure them explicitly.
- `messaging.kafka.message.key` is emitted only when the key has a stable string representation.
  Custom key types can provide `KafkaMessageKey[K]`.
- `messaging.kafka.message.tombstone=true` is emitted when a traced record value is `null`.
- `injectHeaders` can be used on its own when you want propagation without emitting a traced send.
