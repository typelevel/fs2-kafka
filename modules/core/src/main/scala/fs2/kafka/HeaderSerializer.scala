/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

import cats.Contravariant

/**
  * [[HeaderSerializer]] is a functional serializer for Kafka record header values. It's similar to
  * [[Serializer]], except it only has access to the value, and it does not interoperate with the
  * Kafka `Serializer` interface.
  */
sealed abstract class HeaderSerializer[A] {

  /**
    * Serializes the specified value of type `A` into bytes.
    */
  def serialize(a: A): Array[Byte]

  /**
    * Creates a new [[HeaderSerializer]] which applies the specified function `f` on a value of type
    * `B`, and then serializes the result with this [[HeaderSerializer]].
    */
  final def contramap[B](f: B => A): HeaderSerializer[B] =
    HeaderSerializer.instance { b =>
      serialize(f(b))
    }

  /**
    * Creates a new [[HeaderSerializer]] which applies the specified function `f` on the output
    * bytes of this [[HeaderSerializer]].
    */
  final def mapBytes(f: Array[Byte] => Array[Byte]): HeaderSerializer[A] =
    HeaderSerializer.instance { bytes =>
      f(serialize(bytes))
    }

  /**
    * Creates a new [[HeaderSerializer]] which serializes `Some` values using this
    * [[HeaderSerializer]], and serializes `None` as `null`.
    */
  final def option: HeaderSerializer[Option[A]] =
    HeaderSerializer.instance {
      case Some(a) => serialize(a)
      case None    => null
    }

}

object HeaderSerializer {

  def apply[A](implicit serializer: HeaderSerializer[A]): HeaderSerializer[A] = serializer

  /**
    * Creates a new [[HeaderSerializer]] which serializes all values of type `A` as `null`.
    */
  def asNull[A]: HeaderSerializer[A] =
    HeaderSerializer.const(null)

  /**
    * Creates a new [[HeaderSerializer]] which serializes all values of type `A` to the specified
    * `bytes`.
    */
  def const[A](bytes: Array[Byte]): HeaderSerializer[A] =
    HeaderSerializer.instance(_ => bytes)

  /**
    * Creates a new [[HeaderSerializer]] which delegates serialization to the specified Kafka
    * `Serializer`. Note that the `close` and `configure` functions won't be called for the
    * delegate. Also, the topic is an empty `String` and no headers are provided.
    */
  def delegate[A](serializer: KafkaSerializer[A]): HeaderSerializer[A] =
    HeaderSerializer.instance { a =>
      serializer.serialize("", a)
    }

  /**
    * Creates a new [[HeaderSerializer]] which serializes all values of type `A` as the empty
    * `Array[Byte]`.
    */
  def empty[A]: HeaderSerializer[A] =
    HeaderSerializer.const(Array.emptyByteArray)

  /**
    * Creates a new [[HeaderSerializer]] from the specified function.
    */
  def instance[A](f: A => Array[Byte]): HeaderSerializer[A] =
    new HeaderSerializer[A] {

      override def serialize(a: A): Array[Byte] =
        f(a)

      override def toString: String =
        "HeaderSerializer$" + System.identityHashCode(this)

    }

  /**
    * Creates a new [[HeaderSerializer]] which serializes `String` values using the specified
    * `Charset`. Note that the default `String` serializer uses `UTF-8`.
    */
  def string(charset: Charset): HeaderSerializer[String] =
    HeaderSerializer.instance(_.getBytes(charset))

  /**
    * Creates a new [[HeaderSerializer]] which serializes `UUID` values as `String` s with the
    * specified `Charset`. Note that the default `UUID` serializer uses `UTF-8.`
    */
  def uuid(charset: Charset): HeaderSerializer[UUID] =
    HeaderSerializer.string(charset).contramap(_.toString)

  /**
    * The identity [[HeaderSerializer]], which does not perform serialization, simply using the
    * input bytes as the output.
    */
  implicit val identity: HeaderSerializer[Array[Byte]] =
    HeaderSerializer.instance(bytes => bytes)

  /**
    * The option [[HeaderSerializer]] serializes `None` as `null`, and serializes `Some` values
    * using the serializer for type `A`.
    */
  implicit def option[A](implicit
    serializer: HeaderSerializer[A]
  ): HeaderSerializer[Option[A]] =
    serializer.option

  implicit val contravariant: Contravariant[HeaderSerializer] =
    new Contravariant[HeaderSerializer] {

      override def contramap[A, B](
        serializer: HeaderSerializer[A]
      )(f: B => A): HeaderSerializer[B] =
        serializer.contramap(f)

    }

  implicit val double: HeaderSerializer[Double] =
    HeaderSerializer.delegate {
      (new org.apache.kafka.common.serialization.DoubleSerializer).asInstanceOf[
        org.apache.kafka.common.serialization.Serializer[Double]
      ]
    }

  implicit val float: HeaderSerializer[Float] =
    HeaderSerializer.delegate {
      (new org.apache.kafka.common.serialization.FloatSerializer).asInstanceOf[
        org.apache.kafka.common.serialization.Serializer[Float]
      ]
    }

  implicit val int: HeaderSerializer[Int] =
    HeaderSerializer.delegate {
      (new org.apache.kafka.common.serialization.IntegerSerializer).asInstanceOf[
        org.apache.kafka.common.serialization.Serializer[Int]
      ]
    }

  implicit val long: HeaderSerializer[Long] =
    HeaderSerializer.delegate {
      (new org.apache.kafka.common.serialization.LongSerializer).asInstanceOf[
        org.apache.kafka.common.serialization.Serializer[Long]
      ]
    }

  implicit val short: HeaderSerializer[Short] =
    HeaderSerializer.delegate {
      (new org.apache.kafka.common.serialization.ShortSerializer).asInstanceOf[
        org.apache.kafka.common.serialization.Serializer[Short]
      ]
    }

  implicit val string: HeaderSerializer[String] =
    HeaderSerializer.string(StandardCharsets.UTF_8)

  implicit val unit: HeaderSerializer[Unit] =
    HeaderSerializer.const(null)

  implicit val uuid: HeaderSerializer[UUID] =
    HeaderSerializer.string.contramap(_.toString)

}
