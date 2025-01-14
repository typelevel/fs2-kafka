/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import cats.MonadError

/**
  * Functional composable Kafka key- and record deserializer with support for effect types.
  */
sealed abstract class GenericDeserializer[-T <: KeyOrValue, F[_], A] {

  /**
    * Attempts to deserialize the specified bytes into a value of type `A`. The Kafka topic name,
    * from which the serialized bytes came, and record headers are available.
    */
  def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): F[A]

  /**
    * Creates a new [[Deserializer]] which applies the specified function to the result of this
    * [[Deserializer]].
    */
  def map[B](f: A => B): GenericDeserializer[T, F, B]

  /**
    * Creates a new [[Deserializer]] by first deserializing with this [[Deserializer]] and then
    * using the result as input to the specified function.
    */
  def flatMap[T0 <: T, B](f: A => GenericDeserializer[T0, F, B]): GenericDeserializer[T0, F, B]

  /**
    * Creates a new [[Deserializer]] which deserializes both using this [[Deserializer]] and that
    * [[Deserializer]], and returns both results in a tuple.
    */
  def product[T0 <: T, B](that: GenericDeserializer[T0, F, B]): GenericDeserializer[T0, F, (A, B)]

  /**
    * Creates a new [[Deserializer]] which handles errors by turning them into `Either` values.
    */
  def attempt: GenericDeserializer[T, F, Either[Throwable, A]]

  /**
    * Creates a new [[Deserializer]] which returns `None` when the bytes are `null`, and otherwise
    * returns the result of this [[Deserializer]] wrapped in `Some`.
    */
  def option: Deserializer[F, Option[A]]

  /**
    * Creates a new [[Deserializer]] which suspends deserialization, capturing any impure behaviours
    * of this [[Deserializer]].
    */
  def suspend: Deserializer[F, A]

}

object GenericDeserializer {

  def apply[F[_], A](implicit deserializer: Deserializer[F, A]): Deserializer[F, A] = deserializer

  /**
    * Alias for [[Deserializer#identity]].
    */
  def apply[F[_]](implicit F: Sync[F]): Deserializer[F, Array[Byte]] = identity

  /**
    * Creates a new [[Deserializer]] which deserializes all bytes to the specified value of type
    * `A`.
    */
  def const[F[_], A](a: A)(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.lift(_ => F.pure(a))

  /**
    * Creates a new [[Deserializer]] which delegates deserialization to the specified Kafka
    * `Deserializer`. Note that the `close` and `configure` functions won't be called for the
    * delegate.<br><br>
    *
    * It is assumed the delegate `deserialize` function is pure. If it's not pure, then use
    * `suspend` after `delegate`, so the impure behaviours can be captured properly.
    */
  def delegate[F[_], A](
    deserializer: KafkaDeserializer[A]
  )(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      F.pure(deserializer.deserialize(topic, headers.asJava, bytes))
    }

  /**
    * Creates a new [[Deserializer]] which always fails deserialization with the specified exception
    * `e`.
    */
  def fail[F[_], A](e: Throwable)(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.lift(_ => F.raiseError(e))

  /**
    * Creates a new [[Deserializer]] which always fails deserialization with a
    * [[DeserializationException]] using the specified message.
    */
  def failWith[F[_], A](message: String)(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.fail(DeserializationException(message))

  /**
    * Creates a new [[Deserializer]] which can use different [[Deserializer]]s depending on the
    * record headers.
    */
  def headers[F[_], A](f: Headers => Deserializer[F, A])(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f(headers).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] from the specified function. Use [[lift]] instead if the
    * deserializer doesn't need access to the Kafka topic name or record headers.
    */
  def instance[F[_], A](
    f: (String, Headers, Array[Byte]) => F[A]
  )(implicit F: Sync[F]): Deserializer[F, A] =
    new Deserializer[F, A] {

      override def deserialize(topic: String, headers: Headers, bytes: Array[Byte]): F[A] =
        f(topic, headers, bytes)

      override def map[B](f: A => B): Deserializer[F, B] =
        Deserializer.instance { (topic, headers, bytes) =>
          deserialize(topic, headers, bytes).map(f)
        }

      override def flatMap[T0 <: KeyOrValue, B](
        f: A => GenericDeserializer[T0, F, B]
      ): GenericDeserializer[T0, F, B] =
        Deserializer.instance { (topic, headers, bytes) =>
          deserialize(topic, headers, bytes).flatMap { a =>
            f(a).deserialize(topic, headers, bytes)
          }
        }

      override def product[T0 <: KeyOrValue, B](
        that: GenericDeserializer[T0, F, B]
      ): Deserializer[F, (A, B)] =
        Deserializer.instance { (topic, headers, bytes) =>
          val a = deserialize(topic, headers, bytes)
          val b = that.deserialize(topic, headers, bytes)
          a.product(b)
        }

      override def attempt: Deserializer[F, Either[Throwable, A]] =
        Deserializer.instance { (topic, headers, bytes) =>
          deserialize(topic, headers, bytes).attempt
        }

      override def option: Deserializer[F, Option[A]] =
        Deserializer.instance { (topic, headers, bytes) =>
          if (bytes != null)
            deserialize(topic, headers, bytes).map(Some.apply)
          else
            F.pure(None)
        }

      override def suspend: Deserializer[F, A] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.defer(deserialize(topic, headers, bytes))
        }

      override def toString: String =
        "Deserializer$" + System.identityHashCode(this)

    }

  /**
    * Creates a new [[Deserializer]] from the specified function, ignoring from which Kafka topic
    * the bytes came and any record headers. Use [[instance]] instead if the deserializer needs
    * access to the Kafka topic name or the record headers.
    */
  def lift[F[_], A](f: Array[Byte] => F[A])(implicit F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance((_, _, bytes) => f(bytes))

  private[this] def unexpectedTopic[F[_], A](implicit F: Sync[F]): String => Deserializer[F, A] =
    topic => Deserializer.fail(UnexpectedTopicException(topic))

  /**
    * Creates a new [[Deserializer]] which can use different [[Deserializer]]s depending on the
    * Kafka topic name from which the serialized bytes came.
    */
  def topic[T <: KeyOrValue, F[_], A](
    f: PartialFunction[String, GenericDeserializer[T, F, A]]
  )(implicit F: Sync[F]): GenericDeserializer[T, F, A] =
    Deserializer.instance { (topic, headers, bytes) =>
      f.applyOrElse(topic, unexpectedTopic).deserialize(topic, headers, bytes)
    }

  /**
    * Creates a new [[Deserializer]] which deserializes `String` values using the specified
    * `Charset`. Note that the default `String` deserializer uses `UTF-8`.
    */
  def string[F[_]](charset: Charset)(implicit F: Sync[F]): Deserializer[F, String] =
    Deserializer.lift(bytes => F.catchNonFatal(new String(bytes, charset)))

  /**
    * Creates a new [[Deserializer]] which deserializes `String` values using the specified
    * `Charset` as `UUID` s. Note that the default `UUID` deserializer uses `UTF-8`.
    */
  def uuid[F[_]](charset: Charset)(implicit F: Sync[F]): Deserializer[F, UUID] =
    Deserializer.string[F](charset).map(UUID.fromString).suspend

  /**
    * The identity [[Deserializer]], which does not perform any kind of deserialization, simply
    * using the input bytes as the output.
    */
  implicit def identity[F[_]](implicit F: Sync[F]): Deserializer[F, Array[Byte]] =
    Deserializer.lift(bytes => F.pure(bytes))

  /**
    * The attempt [[Deserializer]] try to deserialize to type `A`, When it fails returns `Left`
    * containing the exception, otherwise returns `Right` with the value `A`
    */
  implicit def attempt[F[_], A](implicit
    deserializer: Deserializer[F, A]
  ): Deserializer[F, Either[Throwable, A]] =
    deserializer.attempt

  /**
    * The option [[Deserializer]] returns `None` when the bytes are `null`, and otherwise
    * deserializes using the deserializer for the type `A`, wrapping the result in `Some`.
    */
  implicit def option[F[_], A](implicit
    deserializer: Deserializer[F, A]
  ): Deserializer[F, Option[A]] =
    deserializer.option

  implicit def monadError[T <: KeyOrValue, F[_]](implicit
    F: Sync[F]
  ): MonadError[GenericDeserializer[T, F, *], Throwable] =
    new MonadError[GenericDeserializer[T, F, *], Throwable] {

      override def pure[A](a: A): GenericDeserializer[T, F, A] =
        Deserializer.const(a)

      override def map[A, B](
        deserializer: GenericDeserializer[T, F, A]
      )(f: A => B): GenericDeserializer[T, F, B] =
        deserializer.map(f)

      override def flatMap[A, B](
        deserializer: GenericDeserializer[T, F, A]
      )(f: A => GenericDeserializer[T, F, B]): GenericDeserializer[T, F, B] =
        deserializer.flatMap(f)

      override def product[A, B](
        first: GenericDeserializer[T, F, A],
        second: GenericDeserializer[T, F, B]
      ): GenericDeserializer[T, F, (A, B)] =
        first.product(second)

      override def tailRecM[A, B](
        a: A
      )(f: A => GenericDeserializer[T, F, Either[A, B]]): GenericDeserializer[T, F, B] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.tailRecM(a)(f(_).deserialize(topic, headers, bytes))
        }

      override def handleErrorWith[A](fa: GenericDeserializer[T, F, A])(
        f: Throwable => GenericDeserializer[T, F, A]
      ): GenericDeserializer[T, F, A] =
        Deserializer.instance { (topic, headers, bytes) =>
          F.handleErrorWith(fa.deserialize(topic, headers, bytes)) { throwable =>
            f(throwable).deserialize(topic, headers, bytes)
          }
        }

      override def raiseError[A](e: Throwable): GenericDeserializer[T, F, A] =
        Deserializer.fail(e)

    }

  implicit def double[F[_]](implicit F: Sync[F]): Deserializer[F, Double] =
    Deserializer
      .delegate[F, Double] {
        (new org.apache.kafka.common.serialization.DoubleDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Double]
        ]
      }
      .suspend

  implicit def float[F[_]](implicit F: Sync[F]): Deserializer[F, Float] =
    Deserializer
      .delegate[F, Float] {
        (new org.apache.kafka.common.serialization.FloatDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Float]
        ]
      }
      .suspend

  implicit def int[F[_]](implicit F: Sync[F]): Deserializer[F, Int] =
    Deserializer
      .delegate[F, Int] {
        (new org.apache.kafka.common.serialization.IntegerDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Int]
        ]
      }
      .suspend

  implicit def long[F[_]](implicit F: Sync[F]): Deserializer[F, Long] =
    Deserializer
      .delegate[F, Long] {
        (new org.apache.kafka.common.serialization.LongDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Long]
        ]
      }
      .suspend

  implicit def short[F[_]](implicit F: Sync[F]): Deserializer[F, Short] =
    Deserializer
      .delegate[F, Short] {
        (new org.apache.kafka.common.serialization.ShortDeserializer).asInstanceOf[
          org.apache.kafka.common.serialization.Deserializer[Short]
        ]
      }
      .suspend

  implicit def string[F[_]](implicit F: Sync[F]): Deserializer[F, String] =
    Deserializer.string(StandardCharsets.UTF_8)

  implicit def unit[F[_]](implicit F: Sync[F]): Deserializer[F, Unit] =
    Deserializer.const(())

  implicit def uuid[F[_]](implicit F: Sync[F]): Deserializer[F, UUID] =
    Deserializer.string[F].map(UUID.fromString).suspend

  implicit def resource[T <: KeyOrValue, F[_], A](implicit
    des: GenericDeserializer[T, F, A]
  ): Resource[F, GenericDeserializer[T, F, A]] = Resource.pure(des)

}
