/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import cats.effect.Sync
import fs2.kafka.KafkaByteShareConsumer
import fs2.kafka.ShareConsumerSettings
import fs2.kafka.internal.converters.collection.*
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
  * A capability trait representing the ability to instantiate the Java `ShareConsumer` underlying
  * the fs2 [[KafkaShareConsumer]]. This is needed in order to instantiate
  * [[KafkaShareConsumer]].<br><br>
  *
  * By default, the instance provided by [[MkShareConsumer.mkShareConsumerForSync]] will be used.
  * However, this behaviour can be overridden, e.g. for testing purposes, by placing an alternative
  * implicit instance in lexical scope.
  */
trait MkShareConsumer[F[_]] {
  def apply[G[_]](settings: ShareConsumerSettings[G, ?, ?]): F[KafkaByteShareConsumer]
}

object MkShareConsumer {

  implicit def mkShareConsumerForSync[F[_]](implicit F: Sync[F]): MkShareConsumer[F] =
    new MkShareConsumer[F] {

      def apply[G[_]](settings: ShareConsumerSettings[G, ?, ?]): F[KafkaByteShareConsumer] =
        F.blocking {
          val byteArrayDeserializer = new ByteArrayDeserializer
          new org.apache.kafka.clients.consumer.KafkaShareConsumer(
            (settings.properties: Map[String, AnyRef]).asJava,
            byteArrayDeserializer,
            byteArrayDeserializer
          )
        }

    }

}
