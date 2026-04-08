/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.annotation.nowarn
import cats.effect.Async
import cats.syntax.all.*
import cats.effect.Resource
import cats.Parallel
import fs2.kafka.producer.MkProducer
import fs2.Chunk
import fs2.Stream
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName

/**
  * Represents a producer of Kafka records specialized for 'read-process-write' streams, with the
  * ability to atomically produce `ProducerRecord`s and commit corresponding [[CommittableOffset]]s
  * using [[produce]].<br><br>
  *
  * Records are wrapped in [[TransactionalProducerRecords]], which is a chunk of
  * [[CommittableProducerRecord]] which wrap zero or more records together with a
  * [[CommittableOffset]].
  */
abstract class TransactionalKafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord`s in the specified [[TransactionalProducerRecords]] in four
    * steps: first a transaction is initialized, then the records are placed in the buffer of the
    * producer, then the offsets of the records are sent to the transaction, and lastly the
    * transaction is committed. If errors or cancellation occurs, the transaction is aborted. The
    * returned effect succeeds if the whole transaction completes successfully.
    */
  def produce(
    records: TransactionalProducerRecords[F, K, V]
  ): F[ProducerResult[K, V]]

  /**
    * Produces the `ProducerRecord`s in the specified [[ProducerRecords]] in three steps: first a
    * transaction is initialized, then the records are placed in the buffer of the producer, and
    * lastly the transaction is committed. If errors or cancellation occurs, the transaction is
    * aborted. The returned effect succeeds if the whole transaction completes successfully.
    */
  def produceWithoutOffsets(records: ProducerRecords[K, V]): F[ProducerResult[K, V]]

  /**
    * Returns producer metrics.
    *
    * @see
    *   org.apache.kafka.clients.producer.KafkaProducer#metrics
    */
  def metrics: F[Map[MetricName, Metric]]

}

object TransactionalKafkaProducer {

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Resource` context, using the specified
    * [[TransactionalProducerSettings]]. Note that there is another version where `F[_]` is
    * specified explicitly and the key and value type can be inferred, which allows you to use the
    * following syntax.
    *
    * {{{
    * TransactionalKafkaProducer.resource[F].using(settings)
    * }}}
    */
  def resource[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkProducer[F],
    p: Parallel[F]
  ): Resource[F, TransactionalKafkaProducer[F, K, V]] = {
    for {
      producer <- LowLevelKafkaProducer.resource[F, K, V](settings)(F, mk)
    } yield new TransactionalKafkaProducer[F, K, V] {

      override def produce(r: TransactionalProducerRecords[F, K, V]): F[ProducerResult[K, V]] = {
        val records: Chunk[CommittableProducerRecords[F, K, V]] = r
        if (records.isEmpty) F.pure(Chunk.empty)
        else {
          val batch   = CommittableOffsetBatch.fromFoldableMap(records)(_.offset)
          val offsets = Chunk.from(batch.offsets.toVector)
          producer
            .transaction
            .use { _ =>
              offsets.parFlatTraverse { case (committer, offsets) =>
                for {
                  metadata       <- committer.metadata
                  producerRecords = records.filter(_.offset.committer == committer).flatMap(_.records)
                  result <- producer.produce(producerRecords)
                  _              <- producer.sendOffsetsToTransaction(offsets, metadata)
                } yield result
              }
            }
        }
      }

      override def produceWithoutOffsets(
        records: ProducerRecords[K, V]
      ): F[ProducerResult[K, V]] =
        producer
          .transaction
          .use { _ =>
            producer.produce(records)
          }

      override def metrics: F[Map[MetricName, Metric]] = producer.metrics

      override def toString: String =
        "TransactionalKafkaProducer$" + System.identityHashCode(this)

    }
  }

  /**
    * Creates a new [[TransactionalKafkaProducer]] in the `Stream` context, using the specified
    * [[TransactionalProducerSettings]]. Note that there is another version where `F[_]` is
    * specified explicitly and the key and value type can be inferred, which allows you to use the
    * following syntax.
    *
    * {{{
    * TransactionalKafkaProducer.stream[F].using(settings)
    * }}}
    */
  def stream[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkProducer[F],
    p: Parallel[F]
  ): Stream[F, TransactionalKafkaProducer[F, K, V]] =
    Stream.resource(resource(settings)(F, mk, p))

  def apply[F[_]]: TransactionalProducerPartiallyApplied[F] =
    new TransactionalProducerPartiallyApplied

  final private[kafka] class TransactionalProducerPartiallyApplied[F[_]](val dummy: Boolean = true)
      extends AnyVal {

    /**
      * Alternative version of `resource` where the `F[_]` is specified explicitly, and where the
      * key and value type can be inferred from the [[TransactionalProducerSettings]]. This allows
      * you to use the following syntax.
      *
      * {{{
      * KafkaProducer[F].resource(settings)
      * }}}
      */
    def resource[K, V](settings: TransactionalProducerSettings[F, K, V])(implicit
      F: Async[F],
      mk: MkProducer[F],
      p: Parallel[F]
    ): Resource[F, TransactionalKafkaProducer[F, K, V]] =
      TransactionalKafkaProducer.resource(settings)(F, mk, p)

    /**
      * Alternative version of `stream` where the `F[_]` is specified explicitly, and where the key
      * and value type can be inferred from the [[TransactionalProducerSettings]]. This allows you
      * to use the following syntax.
      *
      * {{{
      * KafkaProducer[F].stream(settings)
      * }}}
      */
    def stream[K, V](settings: TransactionalProducerSettings[F, K, V])(implicit
      F: Async[F],
      mk: MkProducer[F],
      p: Parallel[F]
    ): Stream[F, TransactionalKafkaProducer[F, K, V]] =
      TransactionalKafkaProducer.stream(settings)(F, mk, p)

    override def toString: String =
      "TransactionalProducerPartiallyApplied$" + System.identityHashCode(this)

  }

  /*
   * Prevents the default `MkProducer` instance from being implicitly available
   * to code defined in this object, ensuring factory methods require an instance
   * to be provided at the call site.
   */
  @nowarn("msg=never used")
  implicit private def mkAmbig1[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")

  @nowarn("msg=never used")
  implicit private def mkAmbig2[F[_]]: MkProducer[F] =
    throw new AssertionError("should not be used")

}
