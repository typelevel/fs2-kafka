/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

import scala.annotation.tailrec

import cats.{Eval, Monad}
import cats.syntax.either.*

/**
  * [[HeaderDeserializer]] is a functional deserializer for Kafka record header values. It's similar
  * to [[Deserializer]], except it only has access to the header bytes, and it does not interoperate
  * with the Kafka `Deserializer` interface.
  */
sealed abstract class HeaderDeserializer[A] {

  /**
    * Deserializes the header value bytes into a value of type `A`.
    */
  def deserialize(bytes: Array[Byte]): A

  /**
    * Creates a new [[HeaderDeserializer]] which applies the specified function `f` to the result of
    * this [[HeaderDeserializer]].
    */
  final def map[B](f: A => B): HeaderDeserializer[B] =
    HeaderDeserializer.instance { bytes =>
      f(deserialize(bytes))
    }

  /**
    * Creates a new [[HeaderDeserializer]] using the result of this [[HeaderDeserializer]] and the
    * specified function.
    */
  final def flatMap[B](f: A => HeaderDeserializer[B]): HeaderDeserializer[B] =
    HeaderDeserializer.instance { bytes =>
      f(deserialize(bytes)).deserialize(bytes)
    }

  /**
    * Creates a new [[HeaderDeserializer]] which deserializes both using this [[HeaderDeserializer]]
    * and that [[HeaderDeserializer]], and returns both results in a tuple.
    */
  final def product[B](that: HeaderDeserializer[B]): HeaderDeserializer[(A, B)] =
    HeaderDeserializer.instance { bytes =>
      val a = deserialize(bytes)
      val b = that.deserialize(bytes)
      (a, b)
    }

  /**
    * Creates a new [[HeaderDeserializer]] which does deserialization lazily by wrapping this
    * [[HeaderDeserializer]] in `Eval.always`.
    */
  final def delay: HeaderDeserializer[Eval[A]] =
    HeaderDeserializer.instance { bytes =>
      Eval.always(deserialize(bytes))
    }

  /**
    * Creates a new [[HeaderDeserializer]] which catches any non-fatal exceptions during
    * deserialization with this [[HeaderDeserializer]].
    */
  final def attempt: HeaderDeserializer.Attempt[A] =
    HeaderDeserializer.instance { bytes =>
      Either.catchNonFatal(deserialize(bytes))
    }

  /**
    * Creates a new [[HeaderDeserializer]] which returns `None` when the bytes are `null`, and
    * otherwise returns the result of this [[HeaderDeserializer]] wrapped in `Some`.
    */
  final def option: HeaderDeserializer[Option[A]] =
    HeaderDeserializer.instance { bytes =>
      if (bytes != null)
        Some(deserialize(bytes))
      else
        None
    }

}

object HeaderDeserializer {

  /**
    * Alias for `HeaderDeserializer[Either[Throwable, A]]`.
    */
  type Attempt[A] = HeaderDeserializer[Either[Throwable, A]]

  def apply[A](implicit
    deserializer: HeaderDeserializer[A]
  ): HeaderDeserializer[A] =
    deserializer

  def attempt[A](implicit
    deserializer: HeaderDeserializer.Attempt[A]
  ): HeaderDeserializer.Attempt[A] =
    deserializer

  /**
    * Creates a new [[HeaderDeserializer]] which deserializes all bytes to the specified value of
    * type `A`.
    */
  def const[A](a: A): HeaderDeserializer[A] =
    HeaderDeserializer.instance(_ => a)

  /**
    * Creates a new [[HeaderDeserializer]] which delegates deserialization to the specified Kafka
    * `Deserializer`. Please note that the `close` and `configure` functions won't be called for the
    * delegate. Also, the topic is an empty `String` and no headers are provided.
    */
  def delegate[A](deserializer: KafkaDeserializer[A]): HeaderDeserializer[A] =
    HeaderDeserializer.instance { bytes =>
      deserializer.deserialize("", bytes)
    }

  /**
    * Creates a new [[HeaderDeserializer]] from the specified function.
    */
  def instance[A](f: Array[Byte] => A): HeaderDeserializer[A] =
    new HeaderDeserializer[A] {

      override def deserialize(bytes: Array[Byte]): A =
        f(bytes)

      override def toString: String =
        "HeaderDeserializer$" + System.identityHashCode(this)

    }

  /**
    * Creates a new [[HeaderDeserializer]] which deserializes `String` values using the specified
    * `Charset`. Note that the default `String` deserializer uses `UTF-8`.
    */
  def string(charset: Charset): HeaderDeserializer[String] =
    HeaderDeserializer.instance(bytes => new String(bytes, charset))

  /**
    * Creates a new [[HeaderDeserializer]] which deserializes `String` values using the specified
    * `Charset` as `UUID` s. Note that the default `UUID` deserializer uses `UTF-8`.
    */
  def uuid(charset: Charset): HeaderDeserializer.Attempt[UUID] =
    HeaderDeserializer.string(charset).map(UUID.fromString).attempt

  /**
    * The identity [[HeaderDeserializer]], which does not perform any kind of deserialization,
    * simply using the input bytes as the output.
    */
  implicit val identity: HeaderDeserializer[Array[Byte]] =
    HeaderDeserializer.instance(bytes => bytes)

  /**
    * The option [[HeaderDeserializer]] returns `None` when the bytes are `null`, and otherwise
    * deserializes using the deserializer for the type `A`, wrapping the result in `Some`.
    */
  implicit def option[A](implicit
    deserializer: HeaderDeserializer[A]
  ): HeaderDeserializer[Option[A]] =
    deserializer.option

  implicit val monad: Monad[HeaderDeserializer] =
    new Monad[HeaderDeserializer] {

      override def pure[A](a: A): HeaderDeserializer[A] =
        HeaderDeserializer.const(a)

      override def map[A, B](
        deserializer: HeaderDeserializer[A]
      )(f: A => B): HeaderDeserializer[B] =
        deserializer.map(f)

      override def flatMap[A, B](
        deserializer: HeaderDeserializer[A]
      )(f: A => HeaderDeserializer[B]): HeaderDeserializer[B] =
        deserializer.flatMap(f)

      override def product[A, B](
        first: HeaderDeserializer[A],
        second: HeaderDeserializer[B]
      ): HeaderDeserializer[(A, B)] =
        first.product(second)

      override def tailRecM[A, B](a: A)(
        f: A => HeaderDeserializer[Either[A, B]]
      ): HeaderDeserializer[B] =
        HeaderDeserializer.instance { bytes =>
          @tailrec def go(deserializer: HeaderDeserializer[Either[A, B]]): B =
            deserializer.deserialize(bytes) match {
              case Right(_b) => _b
              case Left(_a)  => go(f(_a))
            }

          go(f(a))
        }

    }

  implicit val double: HeaderDeserializer.Attempt[Double] =
    HeaderDeserializer
      .delegate {
        (new org.apache.kafka.common.serialization.DoubleDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Double]
        ]
      }
      .attempt

  implicit val float: HeaderDeserializer.Attempt[Float] =
    HeaderDeserializer
      .delegate {
        (new org.apache.kafka.common.serialization.FloatDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Float]
        ]
      }
      .attempt

  implicit val int: HeaderDeserializer.Attempt[Int] =
    HeaderDeserializer
      .delegate {
        (new org.apache.kafka.common.serialization.IntegerDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Int]
        ]
      }
      .attempt

  implicit val long: HeaderDeserializer.Attempt[Long] =
    HeaderDeserializer
      .delegate {
        (new org.apache.kafka.common.serialization.LongDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Long]
        ]
      }
      .attempt

  implicit val short: HeaderDeserializer.Attempt[Short] =
    HeaderDeserializer
      .delegate {
        (new org.apache.kafka.common.serialization.ShortDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Short]
        ]
      }
      .attempt

  implicit val string: HeaderDeserializer[String] =
    HeaderDeserializer.string(StandardCharsets.UTF_8)

  implicit val unit: HeaderDeserializer[Unit] =
    HeaderDeserializer.const(())

  implicit val uuid: HeaderDeserializer.Attempt[UUID] =
    HeaderDeserializer.string.map(UUID.fromString).attempt

}
