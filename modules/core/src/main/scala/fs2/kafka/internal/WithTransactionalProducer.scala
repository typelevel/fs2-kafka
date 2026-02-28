/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Async, MonadCancelThrow, Resource}
import cats.effect.std.Semaphore
import cats.implicits.*
import fs2.kafka.{KafkaByteProducer, TransactionalProducerSettings}
import fs2.kafka.internal.syntax.*
import fs2.kafka.producer.MkProducer

sealed abstract private[kafka] class WithTransactionalProducer[F[_]] {

  def apply[A](f: (KafkaByteProducer, Blocking[F], ExclusiveAccess[F, A]) => F[A]): F[A]

  def exclusiveAccess[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A] = apply {
    case (producer, blocking, exclusive) => exclusive(f(producer, blocking))
  }

  def blocking[A](f: KafkaByteProducer => A): F[A] = apply { case (producer, blocking, _) =>
    blocking(f(producer))
  }

}

private[kafka] object WithTransactionalProducer {

  def apply[F[_], K, V](
    mk: MkProducer[F],
    settings: TransactionalProducerSettings[F, K, V]
  )(implicit
    F: Async[F]
  ): Resource[F, WithTransactionalProducer[F]] =
    Resource[F, WithTransactionalProducer[F]] {
      (mk(settings.producerSettings), Semaphore(1))
        .tupled
        .flatMap { case (producer, semaphore) =>
          val blocking = settings
            .producerSettings
            .customBlockingContext
            .fold(Blocking.fromSync[F])(Blocking.fromExecutionContext)

          val withProducer = create(producer, blocking, semaphore)

          val initTransactions = withProducer.blocking(_.initTransactions())

          /*
          Deliberately does not use the exclusive access functionality to close the producer. The close method on
          the underlying client waits until the buffer has been flushed to the broker or the timeout is exceeded.
          Because the transactional producer _always_ waits until the buffer is flushed and the transaction
          committed on the broker before proceeding, upon gaining exclusive access to the producer the buffer will
          always be empty. Therefore if we used exclusive access to close the underlying producer, the buffer
          would already be empty and the close timeout setting would be redundant.

          TLDR: not using exclusive access here preserves the behaviour of the underlying close method and timeout
          setting
           */
          val close = withProducer.blocking {
            _.close(settings.producerSettings.closeTimeout.toJava)
          }

          initTransactions.as((withProducer, close))
        }
    }

  private def create[F[_]: MonadCancelThrow](
    producer: KafkaByteProducer,
    _blocking: Blocking[F],
    transactionSemaphore: Semaphore[F]
  ): WithTransactionalProducer[F] = new WithTransactionalProducer[F] {

    override def apply[A](
      f: (KafkaByteProducer, Blocking[F], ExclusiveAccess[F, A]) => F[A]
    ): F[A] =
      f(producer, _blocking, transactionSemaphore.permit.surround)

  }

}
