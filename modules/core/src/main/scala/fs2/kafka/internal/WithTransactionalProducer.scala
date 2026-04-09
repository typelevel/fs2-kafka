/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Async, MonadCancelThrow, Resource}
import cats.effect.std.Semaphore
import cats.effect.syntax.all.*
import fs2.kafka.{KafkaByteProducer, TransactionalProducerSettings}
import fs2.kafka.internal.syntax.*
import fs2.kafka.producer.MkProducer

sealed abstract private[kafka] class WithTransactionalProducer[F[_]] {

  def blocking: Blocking[F]
  def exclusiveAccess: Resource[F, Unit]
  def producer: KafkaByteProducer

}

private[kafka] object WithTransactionalProducer {

  def apply[F[_], K, V](
    mk: MkProducer[F],
    settings: TransactionalProducerSettings[F, K, V]
  )(implicit
    F: Async[F]
  ): Resource[F, WithTransactionalProducer[F]] = {
    /*
     * Deliberately does not use the exclusive access functionality to close the producer. The close method on
     * the underlying client waits until the buffer has been flushed to the broker or the timeout is exceeded.
     * Because the transactional producer _always_ waits until the buffer is flushed and the transaction
     * committed on the broker before proceeding, upon gaining exclusive access to the producer the buffer will
     * always be empty. Therefore if we used exclusive access to close the underlying producer, the buffer
     * would already be empty and the close timeout setting would be redundant.
     *
     * TLDR: not using exclusive access here preserves the behaviour of the underlying close method and timeout
     * setting
     */
    for {
      producer  <- mk(settings.producerSettings).toResource
      semaphore <- Semaphore(1).toResource
      blocking   = settings
                   .producerSettings
                   .customBlockingContext
                   .fold(Blocking.fromSync[F])(Blocking.fromExecutionContext)
      withProducer = create(producer, blocking, semaphore)
      close        = blocking(producer.close(settings.producerSettings.closeTimeout.toJava))
      _           <- Resource.onFinalize(close)
    } yield withProducer
  }

  private def create[F[_]: MonadCancelThrow](
    _producer: KafkaByteProducer,
    _blocking: Blocking[F],
    transactionSemaphore: Semaphore[F]
  ): WithTransactionalProducer[F] = new WithTransactionalProducer[F] {

    override def blocking: Blocking[F] = _blocking

    override def exclusiveAccess: Resource[F, Unit] =
      Resource.make(transactionSemaphore.acquire)(_ => transactionSemaphore.release)

    override def producer: KafkaByteProducer = _producer
  }

}
