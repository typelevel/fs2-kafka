/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.annotation.nowarn
import scala.concurrent.Promise

import cats.effect.{Async, Outcome, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.internal.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.producer.MkProducer

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName}

/**
  * Represents a producer of Kafka records specialized for 'read-process-write' streams, with the
  * ability to atomically produce `ProducerRecord` s and commit corresponding [[CommittableOffset]]s
  * using [[produce]].<br><br>
  *
  * Records are wrapped in [[TransactionalProducerRecords]], which is a chunk of
  * [[CommittableProducerRecord]] which wrap zero or more records together with a
  * [[CommittableOffset]].
  */
abstract class TransactionalKafkaProducer[F[_], K, V] {

  /**
    * Produces the `ProducerRecord` s in the specified [[TransactionalProducerRecords]] in four
    * steps: first a transaction is initialized, then the records are placed in the buffer of the
    * producer, then the offsets of the records are sent to the transaction, and lastly the
    * transaction is committed. If errors or cancellation occurs, the transaction is aborted. The
    * returned effect succeeds if the whole transaction completes successfully.
    */
  def produce(
    records: TransactionalProducerRecords[F, K, V]
  ): F[ProducerResult[K, V]]

}

object TransactionalKafkaProducer {

  /**
    * [[TransactionalKafkaProducer.Metrics]] extends [[TransactionalKafkaProducer]] to provide
    * access to the underlying producer metrics.
    */
  abstract class Metrics[F[_], K, V] extends TransactionalKafkaProducer[F, K, V] {

    /**
      * Returns producer metrics.
      *
      * @see
      *   org.apache.kafka.clients.producer.KafkaProducer#metrics
      */
    def metrics: F[Map[MetricName, Metric]]
  }

  /**
    * [[TransactionalKafkaProducer.WithoutOffsets]] extends [[TransactionalKafkaProducer.Metrics]]
    * to allow producing of records without corresponding upstream offsets.
    */
  abstract class WithoutOffsets[F[_], K, V] extends Metrics[F, K, V] {

    /**
      * Produces the `ProducerRecord` s in the specified [[ProducerRecords]] in three steps: first a
      * transaction is initialized, then the records are placed in the buffer of the producer, and
      * lastly the transaction is committed. If errors or cancellation occurs, the transaction is
      * aborted. The returned effect succeeds if the whole transaction completes successfully.
      */
    def produceWithoutOffsets(records: ProducerRecords[K, V]): F[ProducerResult[K, V]]
  }

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
    mk: MkProducer[F]
  ): Resource[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    (
      settings.producerSettings.keySerializer,
      settings.producerSettings.valueSerializer,
      WithTransactionalProducer(mk, settings)
    ).mapN { (keySerializer, valueSerializer, withProducer) =>
      new TransactionalKafkaProducer.WithoutOffsets[F, K, V] {

        override def produce(
          records: TransactionalProducerRecords[F, K, V]
        ): F[ProducerResult[K, V]] =
          produceTransactionWithOffsets(records)

        private[this] def produceTransactionWithOffsets(
          records: Chunk[CommittableProducerRecords[F, K, V]]
        ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] =
          if (records.isEmpty) F.pure(Chunk.empty)
          else {
            val batch =
              CommittableOffsetBatch.fromFoldableMap(records)(_.offset)

            val consumerGroupId =
              if (batch.consumerGroupIdsMissing || batch.consumerGroupIds.size != 1)
                F.raiseError(ConsumerGroupException(batch.consumerGroupIds))
              else F.pure(batch.consumerGroupIds.head)

            consumerGroupId.flatMap { groupId =>
              val sendOffsets: (KafkaByteProducer, Blocking[F]) => F[Unit] = (producer, blocking) =>
                blocking {
                  producer.sendOffsetsToTransaction(
                    batch.offsets.asJava,
                    new ConsumerGroupMetadata(groupId)
                  )
                }

              produceTransaction(records.flatMap(_.records), Some(sendOffsets))
            }
          }

        override def produceWithoutOffsets(
          records: ProducerRecords[K, V]
        ): F[ProducerResult[K, V]] =
          produceTransaction(records, None)

        private[this] def produceTransaction(
          records: Chunk[ProducerRecord[K, V]],
          sendOffsets: Option[(KafkaByteProducer, Blocking[F]) => F[Unit]]
        ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] =
          withProducer.exclusiveAccess { (producer, blocking) =>
            blocking(producer.beginTransaction()).bracketCase { _ =>
              def produceRecords(produceRecordError: Option[Promise[Throwable]]) =
                records
                  .traverse(
                    KafkaProducer.produceRecord(
                      keySerializer,
                      valueSerializer,
                      producer,
                      blocking,
                      produceRecordError
                    )
                  )
                  .flatMap(_.sequence)

              val produce =
                if (settings.producerSettings.failFastProduce)
                  Async[F]
                    .delay(Promise[Throwable]())
                    .flatMap { produceRecordError =>
                      Async[F]
                        .race(
                          Async[F].fromFutureCancelable(
                            Async[F].delay((produceRecordError.future, Async[F].unit))
                          ),
                          produceRecords(produceRecordError.some)
                        )
                        .rethrow
                    }
                else
                  produceRecords(None)

              sendOffsets.fold(produce)(f => produce.flatTap(_ => f(producer, blocking)))
            } {
              case (_, Outcome.Succeeded(_)) =>
                blocking(producer.commitTransaction())
              case (_, Outcome.Canceled() | Outcome.Errored(_)) =>
                blocking(producer.abortTransaction())
            }
          }

        override def metrics: F[Map[MetricName, Metric]] =
          withProducer.blocking(_.metrics().asScala.toMap)

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
    mk: MkProducer[F]
  ): Stream[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
    Stream.resource(resource(settings)(F, mk))

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
      mk: MkProducer[F]
    ): Resource[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
      TransactionalKafkaProducer.resource(settings)(F, mk)

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
      mk: MkProducer[F]
    ): Stream[F, TransactionalKafkaProducer.WithoutOffsets[F, K, V]] =
      TransactionalKafkaProducer.stream(settings)(F, mk)

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
