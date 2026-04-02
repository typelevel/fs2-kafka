/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Eq, Show}
import cats.data.{Chain, NonEmptyChain}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*

/**
  * [[Headers]] represent an immutable append-only collection of [[Header]]s. To create a new
  * [[Headers]] instance, you can use [[Headers#apply]] or [[Headers#empty]] and add an instance of
  * [[Header]] using `append`.
  */
sealed abstract class Headers {

  /**
    * Returns the first header with the specified key, wrapped in `Some`, or `None` if no such
    * header exists. Alias for [[withKey]].
    */
  final def apply(key: String): Option[Header] =
    withKey(key)

  /**
    * Returns the first header with the specified key, wrapped in `Some`, or `None` if no such
    * header exists. The [[apply]] function is an alias.
    */
  def withKey(key: String): Option[Header]

  /**
    * Creates a new [[Headers]] instance with the specified [[Header]] included.
    */
  def append(header: Header): Headers

  /**
    * Creates a new [[Headers]] instance including a [[Header]] with the specified key and value.
    */
  def append[V: HeaderSerializer](key: String, value: V): Headers

  /**
    * Returns `true` if a header with the specified key exists; otherwise `false`.
    */
  def exists(key: String): Boolean

  /**
    * Appends the specified [[Headers]] after these headers.
    */
  def concat(that: Headers): Headers

  /**
    * The included [[Header]]s as a `Chain`.
    */
  def toChain: Chain[Header]

  /**
    * `true` if at least one [[Header]] is included; otherwise `false`.
    */
  final def nonEmpty: Boolean = !isEmpty

  /**
    * `true` if no [[Header]]s are included; otherwise `false`.
    */
  def isEmpty: Boolean

  /**
    * The [[Headers]] as an immutable Java Kafka `Headers` instance.
    */
  def asJava: KafkaHeaders

}

object Headers {

  final private[this] class HeadersImpl(
    val headers: NonEmptyChain[Header]
  ) extends Headers {

    override def withKey(key: String): Option[Header] =
      headers.find(_.key == key)

    override def append(header: Header): Headers =
      new HeadersImpl(headers.append(header))

    override def append[V: HeaderSerializer](key: String, value: V): Headers =
      append(Header(key, value))

    override def exists(key: String): Boolean =
      headers.exists(_.key == key)

    override def concat(that: Headers): Headers =
      if (that.isEmpty) this
      else new HeadersImpl(headers.appendChain(that.toChain))

    override def toChain: Chain[Header] =
      headers.toChain

    override val isEmpty: Boolean =
      false

    override def asJava: KafkaHeaders = {
      val array: Array[KafkaHeader] =
        toChain.iterator.toArray

      new KafkaHeaders {

        override def add(header: KafkaHeader): KafkaHeaders =
          throw new IllegalStateException("Headers#asJava is immutable")

        override def add(key: String, value: Array[Byte]): KafkaHeaders =
          throw new IllegalStateException("Headers#asJava is immutable")

        override def remove(key: String): KafkaHeaders =
          throw new IllegalStateException("Headers#asJava is immutable")

        override def lastHeader(key: String): KafkaHeader = {
          val index = array.lastIndexWhere(_.key == key)
          if (index != -1) array(index) else null
        }

        override def headers(key: String): java.lang.Iterable[KafkaHeader] =
          () => array.iterator.filter(_.key == key).asJava

        override val toArray: Array[KafkaHeader] =
          array

        override def iterator(): java.util.Iterator[KafkaHeader] =
          array.iterator.asJava

      }
    }

    override def toString: String =
      headers.mkStringAppend { (append, header) =>
        append(header.key)
        append(" -> ")
        append(java.util.Arrays.toString(header.value))
      }(
        start = "Headers(",
        sep = ", ",
        end = ")"
      )

  }

  /**
    * Creates a new [[Headers]] instance from the specified [[Header]]s.
    */
  def apply(headers: Header*): Headers =
    if (headers.isEmpty) empty
    else new HeadersImpl(NonEmptyChain.fromChainUnsafe(Chain(headers*)))

  /**
    * Creates a new [[Headers]] instance from the specified `Chain` of [[Header]]s.
    */
  def fromChain(headers: Chain[Header]): Headers =
    if (headers.isEmpty) empty
    else new HeadersImpl(NonEmptyChain.fromChainUnsafe(headers))

  /**
    * Creates a new [[Headers]] instance from the specified `Seq` of [[Header]]s.
    */
  def fromSeq(headers: Seq[Header]): Headers =
    fromChain(Chain.fromSeq(headers))

  /**
    * Creates a new [[Headers]] instance from the specified `Iterable` of [[Header]]s.
    */
  def fromIterable(headers: Iterable[Header]): Headers =
    fromSeq(headers.toSeq)

  /**
    * The empty [[Headers]] instance without any [[Header]]s.
    */
  val empty: Headers =
    new Headers {

      override def withKey(key: String): Option[Header] =
        None

      override def append(header: Header): Headers =
        new HeadersImpl(NonEmptyChain.one(header))

      override def append[V: HeaderSerializer](key: String, value: V): Headers =
        append(Header(key, value))

      override def exists(key: String): Boolean =
        false

      override def concat(that: Headers): Headers =
        that

      override val toChain: Chain[Header] =
        Chain.empty

      override val isEmpty: Boolean =
        true

      override val asJava: KafkaHeaders =
        new KafkaHeaders {

          override def add(header: KafkaHeader): KafkaHeaders =
            throw new IllegalStateException("Headers#asJava is immutable")

          override def add(key: String, value: Array[Byte]): KafkaHeaders =
            throw new IllegalStateException("Headers#asJava is immutable")

          override def remove(key: String): KafkaHeaders =
            throw new IllegalStateException("Headers#asJava is immutable")

          override def lastHeader(key: String): KafkaHeader =
            null

          private[this] val emptyIterator: java.util.Iterator[KafkaHeader] =
            new java.util.Iterator[KafkaHeader] {

              override val hasNext: Boolean =
                false

              override def next(): KafkaHeader =
                throw new NoSuchElementException()

            }

          private[this] val emptyIterable: java.lang.Iterable[KafkaHeader] =
            new java.lang.Iterable[KafkaHeader] {

              override val iterator: java.util.Iterator[KafkaHeader] =
                emptyIterator

            }

          override def headers(key: String): java.lang.Iterable[KafkaHeader] =
            emptyIterable

          override val toArray: Array[KafkaHeader] =
            Array.empty

          override val iterator: java.util.Iterator[KafkaHeader] =
            emptyIterator

        }

      override def toString: String =
        "Headers()"

    }

  implicit val headersShow: Show[Headers] =
    Show.fromToString

  implicit val headersEq: Eq[Headers] =
    Eq.by(_.toChain)

}
