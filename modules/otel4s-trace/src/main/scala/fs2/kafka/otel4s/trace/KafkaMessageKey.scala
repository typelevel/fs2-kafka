/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import java.util.UUID

/**
  * Typeclass describing how to derive a canonical Kafka message-key string for semantic
  * conventions.
  *
  * Implementations should return `Some(...)` only when the key has an unambiguous canonical string
  * representation suitable for `messaging.kafka.message.key`. Returning `None` omits the attribute.
  */
trait KafkaMessageKey[-K] {

  def toMessageKey(value: K): Option[String]

  def contramap[K0](f: K0 => K): KafkaMessageKey[K0] =
    KafkaMessageKey.instance(value => toMessageKey(f(value)))

}

object KafkaMessageKey extends KafkaMessageKeyLowPriority {

  def apply[K](implicit instance: KafkaMessageKey[K]): KafkaMessageKey[K] =
    instance

  def instance[K](f: K => Option[String]): KafkaMessageKey[K] =
    new KafkaMessageKey[K] {
      override def toMessageKey(value: K): Option[String] =
        f(value)
    }

  implicit val stringKafkaMessageKey: KafkaMessageKey[String] =
    instance(Option(_))

  implicit val intKafkaMessageKey: KafkaMessageKey[Int] =
    instance(value => Option(value).map(_.toString))

  implicit val longKafkaMessageKey: KafkaMessageKey[Long] =
    instance(value => Option(value).map(_.toString))

  implicit val uuidKafkaMessageKey: KafkaMessageKey[UUID] =
    instance(value => Option(value).map(_.toString))

}

trait KafkaMessageKeyLowPriority {

  implicit def fallbackKafkaMessageKey[K]: KafkaMessageKey[K] =
    KafkaMessageKey.instance(_ => None)

}
