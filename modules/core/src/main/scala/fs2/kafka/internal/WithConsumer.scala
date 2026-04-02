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

          }
        }
      }(_.blocking(_.close(CloseOptions.timeout(settings.closeTimeout.toJava))))
    }
  }

}
