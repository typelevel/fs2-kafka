/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import java.time.Instant

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.*

import _root_.vulcan.Codec
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.funspec.AnyFunSpec

final class PackageSpec extends AnyFunSpec {

  describe("avroSerializer") {
    it("should be available given explicit settings") {
      avroSerializer[Test].forKey(avroSettings)
      avroSerializer[Test].forValue(avroSettings)
    }
  }

  describe("avroDeserializer") {
    it("should be available given explicit settings") {
      avroDeserializer[Test].forKey(avroSettings)
      avroDeserializer[Test].forValue(avroSettings)
    }
  }

  describe("avroSerializer/avroDeserializer") {
    it("should be able to do roundtrip serialization") {
      (
        avroSerializer[Either[Test, Int]].forValue(avroSettings),
        avroDeserializer[Either[Test, Int]].forValue(avroSettings)
      ).parTupled
        .use { case (serializer, deserializer) =>
          val test = Test("test")

          for {
            serialized   <- serializer.serialize("topic", Headers.empty, Left(test))
            deserialized <- deserializer.deserialize("topic", Headers.empty, serialized)
          } yield assert(deserialized == Left(test))
        }
        .unsafeRunSync()
    }

    it("should be able to do roundtrip serialization using compatible schemas") {
      (
        avroSerializer[Test2].forValue(avroSettings),
        avroDeserializer[Test].forValue(avroSettings)
      ).parTupled
        .use { case (serializer, deserializer) =>
          val test2 = Test2("test", 42)
          for {
            serialized   <- serializer.serialize("topic2", Headers.empty, test2)
            deserialized <- deserializer.deserialize("topic2", Headers.empty, serialized)
          } yield assert(deserialized == Test("test"))
        }
        .unsafeRunSync()
    }

    it("should error when reader and writer schemas have mismatching logical types") {
      (
        avroSerializer[Long].forValue(avroSettings),
        avroDeserializer[Instant].forValue(avroSettings)
      ).parTupled
        .use { case (serializer, deserializer) =>
          val rawLong = 42L

          for {
            serialized   <- serializer.serialize("topic3", Headers.empty, rawLong)
            deserialized <- deserializer.deserialize("topic3", Headers.empty, serialized).attempt
          } yield assert(deserialized.isLeft)
        }
        .unsafeRunSync()
    }
  }

  case class Test(name: String)

  object Test {

    implicit val codec: Codec[Test] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
      ) { field =>
        field("name", _.name).map(Test(_))
      }

  }

  case class Test2(name: String, number: Int)
  object Test2 {

    implicit val codec: Codec[Test2] =
      Codec.record(
        name = "Test",
        namespace = "fs2.kafka.vulcan"
      ) { field =>
        (
          field("name", _.name),
          field("number", _.number)
        ).mapN(apply)
      }

  }

  val schemaRegistryClient: MockSchemaRegistryClient =
    new MockSchemaRegistryClient()

  val schemaRegistryClientSettings: SchemaRegistryClientSettings[IO] =
    SchemaRegistryClientSettings[IO]("baseUrl")
      .withAuth(Auth.Basic("username", "password"))
      .withMaxCacheSize(100)
      .withCreateSchemaRegistryClient { (_, _, _) =>
        IO.pure(schemaRegistryClient)
      }

  val avroSettings: AvroSettings[IO] =
    AvroSettings(schemaRegistryClientSettings)

}
