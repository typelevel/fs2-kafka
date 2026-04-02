/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import java.nio.ByteBuffer

import cats.effect.Resource
import cats.effect.Sync
import fs2.kafka.{Deserializer, KeyDeserializer, ValueDeserializer}

import _root_.vulcan.Codec
import io.confluent.kafka.schemaregistry.avro.AvroSchema

final class AvroDeserializer[A] private[vulcan] (
  private val codec: Codec[A]
) extends AnyVal {

  def forKey[F[_]: Sync](settings: AvroSettings[F]): Resource[F, KeyDeserializer[F, A]] =
    createDeserializer(isKey = true, settings)

  def forValue[F[_]: Sync](settings: AvroSettings[F]): Resource[F, ValueDeserializer[F, A]] =
    createDeserializer(isKey = false, settings)

  private def createDeserializer[F[_]](isKey: Boolean, settings: AvroSettings[F])(implicit
    F: Sync[F]
  ): Resource[F, Deserializer[F, A]] =
    codec.schema match {
      case Right(schema) =>
        Resource
          .make(settings.createAvroDeserializer(isKey)) { case (deserializer, _) =>
            F.delay(deserializer.close())
          }
          .map { case (deserializer, schemaRegistryClient) =>
            Deserializer.instance { (topic, _, bytes) =>
              F.defer {
                if (bytes == null || bytes.length == 0) {
                  F.raiseError(
                    new IllegalArgumentException(
                      s"Invalid Avro record: bytes is null or empty"
                    )
                  )
                } else {
                  val writerSchemaId =
                    ByteBuffer.wrap(bytes).getInt(1) // skip magic byte

                  val writerSchema = schemaRegistryClient.getSchemaById(writerSchemaId) match {
                    case avroSchema: AvroSchema => avroSchema.rawSchema
                    case _                      => null
                  }

                  codec.decode(deserializer.deserialize(topic, bytes, schema), writerSchema) match {
                    case Right(a)    => F.pure(a)
                    case Left(error) => F.raiseError(error.throwable)
                  }
                }
              }
            }
          }

      case Left(error) =>
        Resource.raiseError(error.throwable)
    }

  override def toString: String =
    "AvroDeserializer$" + System.identityHashCode(this)

}

object AvroDeserializer {

  def apply[A](implicit codec: Codec[A]): AvroDeserializer[A] =
    new AvroDeserializer(codec)

}
