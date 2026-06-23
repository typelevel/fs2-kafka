/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.kafka.{ConsumerSettings, KafkaByteConsumer}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.CloseOptions

sealed abstract private[kafka] class WithConsumer[F[_]] {

  def blocking[A](f: KafkaByteConsumer => A): F[A]

  /**
    * Runs `f` on the current thread, directly on the underlying consumer, bypassing the
    * single-threaded blocking context used by [[blocking]].
    *
    * This is ONLY safe to call from within a `ConsumerRebalanceListener` callback. Those callbacks
    * are invoked by Kafka on the consumer's polling thread (inside `poll`), so the consumer may be
    * accessed reentrantly from there, and routing through [[blocking]] would instead deadlock by
    * submitting to the single-threaded context that is already occupied by the in-progress `poll`.
    */
  def synchronouslyDuringRebalance[A](f: KafkaByteConsumer => A): A

}

private[kafka] object WithConsumer {

  def apply[F[_]: Async, K, V](
    mk: MkConsumer[F],
    settings: ConsumerSettings[F, K, V]
  ): Resource[F, WithConsumer[F]] = {
    val blocking: Resource[F, Blocking[F]] = settings.customBlockingContext match {
      case None     => Blocking.singleThreaded[F]("fs2-kafka-consumer")
      case Some(ec) => Resource.pure(Blocking.fromExecutionContext(ec))
    }

    blocking.flatMap { b =>
      Resource.make {
        mk(settings).map { consumer =>
          new WithConsumer[F] {

            override def blocking[A](f: KafkaByteConsumer => A): F[A] =
              b(f(consumer))

            override def synchronouslyDuringRebalance[A](f: KafkaByteConsumer => A): A =
              f(consumer)

          }
        }
      }(_.blocking(_.close(CloseOptions.timeout(settings.closeTimeout.toJava))))
    }
  }

}
