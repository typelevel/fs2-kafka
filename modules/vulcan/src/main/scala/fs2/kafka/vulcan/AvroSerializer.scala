/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.kernel.Resource
import cats.effect.Sync
import fs2.kafka.{KeySerializer, Serializer, ValueSerializer}

import _root_.vulcan.Codec

final class AvroSerializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {

  def forKey[F[_]: Sync](settings: AvroSettings[F]): Resource[F, KeySerializer[F, A]] =
    create(isKey = true, settings)

  def forValue[F[_]: Sync](settings: AvroSettings[F]): Resource[F, ValueSerializer[F, A]] =
    create(isKey = false, settings)

  private def create[F[_]](isKey: Boolean, settings: AvroSettings[F])(implicit
    F: Sync[F]
  ): Resource[F, Serializer[F, A]] =
    codec.schema match {
      case Left(e)             => Resource.pure(Serializer.fail(e.throwable))
      case Right(writerSchema) =>
        Resource
          .make(settings.createAvroSerializer(isKey, Some(writerSchema))) { case (ser, _) =>
            F.delay(ser.close())
          }
          .map { case (serializer, _) =>
            Serializer.instance { (topic, _, a) =>
              F.defer {
                codec.encode(a) match {
                  case Right(value) => F.pure(serializer.serialize(topic, value))
                  case Left(error)  => F.raiseError(error.throwable)
                }
              }
            }
          }
    }

  override def toString: String =
    "AvroSerializer$" + System.identityHashCode(this)

}

object AvroSerializer {

  def apply[A](implicit codec: Codec[A]): AvroSerializer[A] =
    new AvroSerializer(codec)

}
