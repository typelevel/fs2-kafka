/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.producer

import cats.effect.Sync
import fs2.kafka.{KafkaByteProducer, ProducerSettings}
import fs2.kafka.internal.converters.collection.*

import org.apache.kafka.common.serialization.ByteArraySerializer

/**
  * A capability trait representing the ability to instantiate the Java `Producer` that underlies
  * the fs2-kafka `KafkaProducer`. This is needed in order to instantiate
  * [[fs2.kafka.KafkaProducer]] and [[fs2.kafka.TransactionalKafkaProducer]].<br><br>
  *
  * By default, the instance provided by [[MkProducer.mkProducerForSync]] will be used. However,
  * this behaviour can be overridden, e.g. for testing purposes, by placing an alternative implicit
  * instance in lexical scope.
  */
trait MkProducer[F[_]] {
  def apply[G[_]](settings: ProducerSettings[G, ?, ?]): F[KafkaByteProducer]
}

object MkProducer {

  implicit def mkProducerForSync[F[_]](implicit F: Sync[F]): MkProducer[F] =
    new MkProducer[F] {

      def apply[G[_]](settings: ProducerSettings[G, ?, ?]): F[KafkaByteProducer] = F.delay {
        val byteArraySerializer = new ByteArraySerializer
        new org.apache.kafka.clients.producer.KafkaProducer(
          (settings.properties: Map[String, AnyRef]).asJava,
          byteArraySerializer,
          byteArraySerializer
        )
      }

    }

}
