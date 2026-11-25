/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all.*
import fs2.kafka.KafkaByteShareConsumer
import fs2.kafka.ShareConsumerSettings
import fs2.kafka.consumer.MkShareConsumer
import fs2.kafka.internal.syntax.*

sealed abstract private[kafka] class WithShareConsumer[F[_]] {

  def blocking[A](f: KafkaByteShareConsumer => A): F[A]

}

private[kafka] object WithShareConsumer {

  def apply[F[_]: Async, K, V](
    mk: MkShareConsumer[F],
    settings: ShareConsumerSettings[F, K, V]
  ): Resource[F, WithShareConsumer[F]] =
    Blocking
      .singleThreaded[F]("fs2-kafka-share-consumer")
      .flatMap { b =>
        Resource.make {
          mk(settings).map { consumer =>
            new WithShareConsumer[F] {

              override def blocking[A](f: KafkaByteShareConsumer => A): F[A] =
                b(f(consumer))

            }
          }
        }(_.blocking(_.close(settings.closeTimeout.toJava)))
      }

}
