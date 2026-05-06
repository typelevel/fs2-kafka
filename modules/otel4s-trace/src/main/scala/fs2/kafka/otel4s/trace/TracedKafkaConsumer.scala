/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import scala.util.chaining._

import cats.effect.Concurrent
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord, KafkaConsumer}
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv

import org.typelevel.otel4s.trace.{SpanContext, SpanKind, Tracer}
import org.typelevel.otel4s.Attributes

/**
  * A consumer-bound tracing handle for `fs2-kafka`.
  *
  * Unlike producer tracing, consumer tracing cannot be a fully transparent drop-in replacement,
  * because the important `process` boundary lives in user code. This handle keeps the association
  * with a specific [[fs2.kafka.KafkaConsumer]] while still making `receive` and `process` explicit.
  */
trait TracedKafkaConsumer[F[_], K, V] {

  /**
    * The underlying Kafka consumer this tracing handle is bound to.
    */
  def underlying: KafkaConsumer[F, K, V]

  /**
    * Delegates to [[underlying.records]].
    *
    * Record emission itself is not treated as processing. Wrap the actual business logic with
    * [[process]] or use [[recordsWithProcess]] for the common `evalMap` shape.
    */
  def records: Stream[F, CommittableConsumerRecord[F, K, V]]

  /**
    * Evaluates `fa` inside a `poll` / `receive` span representing delivery of a non-committable
    * chunk of records to application code.
    */
  def receive[A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /**
    * Evaluates `fa` inside a `poll` / `receive` span representing delivery of a committable chunk
    * of records to application code.
    *
    * The commit handle is preserved, but span attributes are derived from the wrapped
    * [[CommittableConsumerRecord.record]] values.
    */
  def receiveCommittable[A](records: Chunk[CommittableConsumerRecord[F, K, V]])(fa: F[A]): F[A]

  /**
    * Evaluates `fa` inside a `process` span using trace context extracted from the record headers
    * when available.
    */
  def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A]

  /**
    * Evaluates `fa` inside a `process` span using trace context extracted from the record headers
    * when available.
    */
  def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A]

  /**
    * Convenience stream for the common traced-consumption shape: a chunk-level `receive` span
    * around delivery, plus a per-record `process` span for each record in that chunk.
    *
    * This helper intentionally derives `receive` spans from the underlying per-partition delivery
    * chunks rather than from arbitrary chunking of a flattened `records` stream. The `receive` span
    * ends once the chunk has been handed off; user business logic then runs only inside the
    * per-record `process` spans.
    */
  def recordsWithProcess[A](
    f: CommittableConsumerRecord[F, K, V] => F[A]
  ): Stream[F, A]

  /**
    * Convenience stream for the narrower `records.evalMap(record => process(record)(...))` shape.
    *
    * This intentionally omits the chunk-level `receive` span. Prefer [[recordsWithProcess]] unless
    * you specifically want only per-record processing spans.
    */
  def recordsWithProcessOnly[A](f: CommittableConsumerRecord[F, K, V] => F[A]): Stream[F, A]

}

object TracedKafkaConsumer {

  final private[otel4s] class Impl[F[_]: Concurrent: Tracer, K: KafkaMessageKey, V](
    override val underlying: KafkaConsumer[F, K, V],
    config: KafkaTracer.Config
  ) extends TracedKafkaConsumer[F, K, V] {

    override def records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      underlying.records

    override def receive[A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      if (records.isEmpty) fa
      else {
        val spanContext = Semconv.receiveSpanContext(underlying.metadata, records)
        val spanSetup   = config.receiveSpanSetup(spanContext)
        creationContextLinks(records.toList).flatMap { links =>
          Tracer[F]
            .spanBuilder(spanSetup.spanName)
            .withSpanKind(SpanKind.Client)
            .withFinalizationStrategy(spanSetup.finalizationStrategy)
            .addAttributes(
              Semconv.receiveAttributes(spanContext, records) ++
                config.constAttributes ++
                spanSetup.attributes
            )
            .pipe { builder =>
              links.foldLeft(builder) { case (acc, (ctx, attributes)) =>
                acc.addLink(ctx, attributes)
              }
            }
            .build
            .surround(fa)
        }
      }

    override def receiveCommittable[A](
      records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] =
      receive(records.map(_.record))(fa)

    override def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] = {
      val spanContext = Semconv.processSpanContext(underlying.metadata, record)
      val spanSetup   = config.processSpanSetup(spanContext)
      creationContextLinks(record :: Nil).flatMap { links =>
        Tracer[F]
          .spanBuilder(spanSetup.spanName)
          .withSpanKind(SpanKind.Consumer)
          .withFinalizationStrategy(spanSetup.finalizationStrategy)
          .addAttributes(
            Semconv.processAttributes(spanContext, record) ++
              config.constAttributes ++
              spanSetup.attributes
          )
          .pipe { builder =>
            links.foldLeft(builder) { case (acc, (ctx, attributes)) =>
              acc.addLink(ctx, attributes)
            }
          }
          .build
          .surround(fa)
      }
    }

    override def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] =
      process(record.record)(fa)

    override def recordsWithProcess[A](
      f: CommittableConsumerRecord[F, K, V] => F[A]
    ): Stream[F, A] =
      underlying
        .partitionedStream
        .map(
          _.chunks
            .flatMap { chunk =>
              Stream
                .eval(receiveCommittable(chunk)(Concurrent[F].pure(chunk)))
                .flatMap(records =>
                  Stream.chunk(records).evalMap(record => process(record)(f(record)))
                )
            }
        )
        .parJoinUnbounded

    override def recordsWithProcessOnly[A](
      f: CommittableConsumerRecord[F, K, V] => F[A]
    ): Stream[F, A] =
      underlying.records.evalMap(record => process(record)(f(record)))

    private def creationContextLinks(
      records: Iterable[ConsumerRecord[K, V]]
    ): F[List[(SpanContext, Attributes)]] =
      records
        .toList
        .flatTraverse { record =>
          for {
            ctx <- Tracer[F].joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
          } yield ctx.tupleRight(Semconv.receiveLinkAttributes(record)).toList
        }

  }

}
