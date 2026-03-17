/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Eq, Show}
import cats.instances.string.*
import cats.syntax.eq.*

/**
  * [[Header]] represents a `String` key and `Array[Byte]` value which can be included as part of
  * [[Headers]] when creating a [[ProducerRecord]]. [[Headers]] are included together with a record
  * once produced, and can be used by consumers.<br><br>
  *
  * To create a new [[Header]], use [[Header#apply]].
  */
sealed abstract class Header extends org.apache.kafka.common.header.Header {

  /**
    * The header key.
    */
  override def key: String

  /**
    * The serialized header value.
    */
  override def value: Array[Byte]

  /**
    * Deserializes the [[value]] to the specified type.
    */
  final def as[A](implicit deserializer: HeaderDeserializer[A]): A =
    deserializer.deserialize(value)

  /**
    * Attempts to deserialize the [[value]] to the specified type.
    */
  final def attemptAs[A](implicit
    deserializer: HeaderDeserializer.Attempt[A]
  ): Either[Throwable, A] =
    deserializer.deserialize(value)

}

object Header {

  final private[this] class HeaderImpl(
    override val key: String,
    override val value: Array[Byte]
  ) extends Header {

    override def toString: String =
      s"Header($key -> ${java.util.Arrays.toString(value)})"

  }

  /**
    * Creates a new [[Header]] instance using the specified `String` key and value of type `V`,
    * which is going to be serialized with the implicit `HeaderSerializer`.
    */
  def apply[V](key: String, value: V)(implicit serializer: HeaderSerializer[V]): Header =
    new HeaderImpl(
      key = key,
      value = serializer.serialize(value)
    )

  def unapply(header: Header): Some[(String, Array[Byte])] =
    Some((header.key, header.value))

  implicit val headerShow: Show[Header] =
    Show.fromToString

  implicit val headerEq: Eq[Header] =
    Eq.instance { case (l, r) =>
      l.key === r.key && l.value.sameElements(r.value)
    }

}
