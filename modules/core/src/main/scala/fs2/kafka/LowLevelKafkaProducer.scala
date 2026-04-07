/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.annotation.nowarn
import scala.concurrent.{Promise}

import cats.effect.{Async, Resource}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk}
import fs2.kafka.internal.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.producer.MkProducer

import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

/**
  * Represents a producer of Kafka records specialized for 'read-process-write' streams, with the
  * ability to atomically produce `ProducerRecord`s and commit corresponding [[CommittableOffset]]s
  * using [[produce]].<br><br>
  *
  * Records are wrapped in [[TransactionalProducerRecords]], which is a chunk of
  * [[CommittableProducerRecord]] which wrap zero or more records together with a
  * [[CommittableOffset]].
  */
abstract class LowLevelKafkaProducer[F[_], K, V] {

  def produce(records: ProducerRecords[K, V]): F[ProducerResult[K, V]]

  def sendOffsetsToTransaction(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    groupMetadata: ConsumerGroupMetadata
  ): F[Unit]

  def transaction: Resource[F, Unit]

  /**
    * Returns producer metrics.
    *
    * @see
    *   org.apache.kafka.clients.producer.KafkaProducer#metrics
    */
  def metrics: F[Map[MetricName, Metric]]

}

object LowLevelKafkaProducer {

  def resource[F[_], K, V](
    settings: TransactionalProducerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkProducer[F]): Resource[F, LowLevelKafkaProducer[F, K, V]] = for {
    keySerializer   <- settings.producerSettings.keySerializer
    valueSerializer <- settings.producerSettings.valueSerializer
    withProducer    <- WithTransactionalProducer(mk, settings)
    _               <- withProducer.blocking(withProducer.producer.initTransactions()).toResource
  } yield new LowLevelKafkaProducer[F, K, V] {

    override def transaction: Resource[F, Unit] = {
      val acquireTx = withProducer.blocking(withProducer.producer.beginTransaction())
      val commitTx  = withProducer.blocking(withProducer.producer.commitTransaction())
      val abortTx   = withProducer.blocking(withProducer.producer.abortTransaction())
      for {
        _ <- withProducer.exclusiveAccess
        _ <- Resource.makeCase(acquireTx) {
               case (_, ExitCase.Succeeded)                      => commitTx
               case (_, ExitCase.Canceled | ExitCase.Errored(_)) => abortTx
             }
      } yield ()
    }

    override def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      groupMetadata: ConsumerGroupMetadata
    ): F[Unit] =
      withProducer.blocking(
        withProducer.producer.sendOffsetsToTransaction(offsets.asJava, groupMetadata)
      )

    private def sendRecords(
      produceRecordError: Option[Promise[Throwable]],
      records: Chunk[ProducerRecord[K, V]]
    ): F[Chunk[(ProducerRecord[K, V], RecordMetadata)]] = {
      records
        .traverse(
          KafkaProducer.produceRecord(
            keySerializer,
            valueSerializer,
            withProducer.producer,
            withProducer.blocking,
            produceRecordError
          )
        )
        .flatMap(_.sequence)
    }

    override def produce(records: ProducerRecords[K, V]): F[ProducerResult[K, V]] = {
      if (settings.producerSettings.failFastProduce)
        for {
          produceRecordError <- Async[F].delay(Promise[Throwable]())
          waitErrorF          = Async[F].fromFutureCancelable(
                         Async[F].delay((produceRecordError.future, Async[F].unit))
                       )
          recordsToProduce = sendRecords(produceRecordError.some, records)
          result          <- waitErrorF.race(recordsToProduce).rethrow
        } yield result
      else sendRecords(None, records)
    }

    /**
      * Returns producer metrics.
      *
      * @see
      *   org.apache.kafka.clients.producer.KafkaProducer#metrics
      */
    override def metrics: F[Map[MetricName, Metric]] = withProducer
      .blocking(withProducer.producer.metrics())
      .map(_.asScala.toMap)
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
