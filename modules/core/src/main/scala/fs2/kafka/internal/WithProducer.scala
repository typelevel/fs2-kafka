/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Async, Resource}
import fs2.kafka.{KafkaByteProducer, ProducerSettings}
import fs2.kafka.internal.syntax.*
import fs2.kafka.producer.MkProducer

sealed abstract private[kafka] class WithProducer[F[_]] {

  def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A]

  def blocking[A](f: KafkaByteProducer => A): F[A] = apply { case (producer, blocking) =>
    blocking(f(producer))
  }

}

private[kafka] object WithProducer {

  def apply[F[_], G[_]](
    mk: MkProducer[F],
    settings: ProducerSettings[G, ?, ?]
  )(implicit
    F: Async[F],
    G: Async[G]
  ): Resource[F, WithProducer[G]] = {
    val blockingF =
      settings.customBlockingContext.fold(Blocking.fromSync[F])(Blocking.fromExecutionContext[F])
    val blockingG =
      settings.customBlockingContext.fold(Blocking.fromSync[G])(Blocking.fromExecutionContext[G])

    Resource
      .make(
        mk(settings)
      )(producer => blockingF(producer.close(settings.closeTimeout.toJava)))
      .map(create(_, blockingG))
  }

  private def create[F[_]](
    producer: KafkaByteProducer,
    _blocking: Blocking[F]
  ): WithProducer[F] = new WithProducer[F] {

    override def apply[A](f: (KafkaByteProducer, Blocking[F]) => F[A]): F[A] =
      f(producer, _blocking)

  }

}
