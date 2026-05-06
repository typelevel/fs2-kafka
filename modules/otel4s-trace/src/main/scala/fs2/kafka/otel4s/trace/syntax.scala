/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import fs2.kafka._
import fs2.Chunk

/**
  * Producer-side syntax for the `fs2-kafka-otel4s` module.
  *
  * This syntax operates on an implicit [[TracedKafkaProducer]].
  *
  * It is intended for cases where a traced producer has already been bound through
  * [[fs2.kafka.otel4s.KafkaTracer#producer]], and you want compact syntax for header propagation or
  * traced sends at the call site.
  *
  * See the OpenTelemetry messaging and Kafka semantic conventions:
  *
  *   - https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
  *   - https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
  */
trait ProducerTracingSyntax {

  implicit final class ProducerRecordTracingOps[K, V](
    private val record: ProducerRecord[K, V]
  ) {

    def injectTracingHeaders[F[_]](implicit
      tracedProducer: TracedKafkaProducer[F, K, V]
    ): F[ProducerRecord[K, V]] =
      tracedProducer.injectHeaders(record)

  }

  implicit final class ProducerRecordsTracingOps[K, V](
    private val records: ProducerRecords[K, V]
  ) {

    def injectTracingHeaders[F[_]](implicit
      tracedProducer: TracedKafkaProducer[F, K, V]
    ): F[ProducerRecords[K, V]] =
      tracedProducer.injectHeaders(records)

  }

  implicit final class TracedKafkaProducerTracingOps[F[_], K, V](
    private val producer: TracedKafkaProducer[F, K, V]
  ) {

    /**
      * Produces records with tracing and awaits Kafka completion immediately.
      *
      * Prefer this over the underlying producer's two-stage `produce` when you want `send` span
      * timing to match the actual Kafka send.
      */
    def produceTraced(
      records: ProducerRecords[K, V]
    ): F[ProducerResult[K, V]] =
      producer.produceAwaited(records)

    /**
      * Produces one record with tracing and awaits Kafka completion immediately.
      */
    def produceOneTraced(
      record: ProducerRecord[K, V]
    ): F[ProducerResult[K, V]] =
      producer.produceOneAwaited(record)

  }

}

/**
  * Consumer-side syntax for the `fs2-kafka-otel4s` module.
  *
  * This syntax operates on an implicit [[TracedKafkaConsumer]] that should normally be derived once
  * from [[fs2.kafka.otel4s.KafkaTracer#consumer]] and then reused at record or chunk boundaries.
  *
  * Prefer the methods on [[TracedKafkaConsumer]] directly when that reads more clearly at the call
  * site. The syntax is mainly intended for stream code where `traceReceive` and `traceProcess`
  * improve local readability.
  *
  * See the OpenTelemetry messaging and Kafka semantic conventions:
  *
  *   - https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
  *   - https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
  */
trait ConsumerTracingSyntax {

  implicit final class ConsumerRecordChunkTracingOps[K, V](
    private val records: Chunk[ConsumerRecord[K, V]]
  ) {

    def traceReceive[F[_], A](fa: F[A])(implicit
      tracedConsumer: TracedKafkaConsumer[F, K, V]
    ): F[A] =
      tracedConsumer.receive(records)(fa)

  }

  implicit final class ConsumerChunkTracingOps[F[_], K, V](
    private val records: Chunk[CommittableConsumerRecord[F, K, V]]
  ) {

    def traceReceive[A](fa: F[A])(implicit tracedConsumer: TracedKafkaConsumer[F, K, V]): F[A] =
      tracedConsumer.receiveCommittable(records)(fa)

  }

  implicit final class ConsumerRecordTracingOps[K, V](
    private val record: ConsumerRecord[K, V]
  ) {

    def traceProcess[F[_], A](fa: F[A])(implicit
      tracedConsumer: TracedKafkaConsumer[F, K, V]
    ): F[A] =
      tracedConsumer.process(record)(fa)

  }

  implicit final class CommittableConsumerRecordTracingOps[F[_], K, V](
    private val record: CommittableConsumerRecord[F, K, V]
  ) {

    def traceProcess[A](fa: F[A])(implicit tracedConsumer: TracedKafkaConsumer[F, K, V]): F[A] =
      tracedConsumer.process(record)(fa)

  }

}

/**
  * Syntax imports for traced producer helpers and consumer-bound tracing helpers.
  */
object syntax extends ProducerTracingSyntax with ConsumerTracingSyntax
