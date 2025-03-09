/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.StandardCharsets
import java.util.UUID

import cats.*
import cats.effect.IO
import cats.laws.discipline.*

import org.scalacheck.Arbitrary
import org.scalatest.*

final class SerializerSpec extends BaseCatsSpec {

  import cats.effect.unsafe.implicits.global

  checkAll(
    "Serializer[IO, *]", {
      // use of Ticker causes an error since CE3.3.0
      // implicit val ticker = Ticker()
      implicit val eq: Eq[IO[Array[Byte]]] = Eq.by(_.unsafeRunSync())
      ContravariantTests[Serializer[IO, *]].contravariant[String, String, String]
    }
  )

  test("Serializer#mapBytes") {
    val serializer =
      Serializer.identity[IO].mapBytes(Array(0.toByte) ++ _)

    forAll { (topic: String, bytes: Array[Byte]) =>
      serializer.serialize(topic, Headers.empty, bytes).unsafeRunSync() shouldBe (Array(
        0.toByte
      ) ++ bytes)
    }
  }

  test("Serializer#apply") {
    val serializer =
      Serializer[IO]

    forAll { (topic: String, bytes: Array[Byte]) =>
      serializer.serialize(topic, Headers.empty, bytes).unsafeRunSync() shouldBe bytes
    }
  }

  test("Serializer#identity") {
    val serializer =
      Serializer.identity[IO]

    forAll { (topic: String, bytes: Array[Byte]) =>
      serializer.serialize(topic, Headers.empty, bytes).unsafeRunSync() shouldBe bytes
    }
  }

  test("Serializer#const") {
    val serializer =
      Serializer.const[IO, Int](Array())

    forAll { (topic: String, i: Int) =>
      serializer.serialize(topic, Headers.empty, i).unsafeRunSync().isEmpty shouldBe true
    }
  }

  test("Serializer#defer") {
    val serializer =
      Serializer
        .delegate[IO, Int](
          new KafkaSerializer[Int] {

            override def close(): Unit                                                    = ()
            override def configure(props: java.util.Map[String, ?], isKey: Boolean): Unit = ()

            override def serialize(topic: String, int: Int): Array[Byte] =
              throw new RuntimeException

          }
        )
        .suspend

    forAll { (topic: String, headers: Headers, int: Int) =>
      val serialized = serializer.serialize(topic, headers, int)
      serialized.attempt.unsafeRunSync().isLeft shouldBe true
    }
  }

  test("Serializer#fail") {
    val serializer =
      Serializer.fail[IO, Int](new RuntimeException)

    forAll { (topic: String, headers: Headers, i: Int) =>
      val serialized = serializer.serialize(topic, headers, i)
      assert(serialized.attempt.unsafeRunSync().isLeft)
    }
  }

  test("Serializer#failWith") {
    val serializer =
      Serializer.failWith[IO, Int]("message")

    forAll { (topic: String, headers: Headers, i: Int) =>
      val serialized = serializer.serialize(topic, headers, i)
      assert(serialized.attempt.unsafeRunSync().isLeft)
    }
  }

  test("Serializer#suspend") {
    val serializer =
      Serializer
        .delegate[IO, Int](
          new KafkaSerializer[Int] {

            override def close(): Unit                                                    = ()
            override def configure(props: java.util.Map[String, ?], isKey: Boolean): Unit = ()

            override def serialize(topic: String, int: Int): Array[Byte] =
              throw new RuntimeException

          }
        )
        .suspend

    forAll { (topic: String, headers: Headers, int: Int) =>
      val serialized = serializer.serialize(topic, headers, int)
      serialized.attempt.unsafeRunSync() shouldBe a[Left[?, ?]]
    }
  }

  test("Serializer#headers") {
    val serializer =
      Serializer.headers { headers =>
        headers("format").map(_.as[String]) match {
          case Some("int") => Serializer[IO, Int]
          case _           => Serializer[IO, String].contramap[Int](_.toString)
        }
      }

    forAll { (topic: String, i: Int) =>
      val headers    = Headers(Header("format", "int"))
      val serialized = serializer.serialize(topic, headers, i)
      val expected   = Serializer[IO, Int].serialize(topic, Headers.empty, i)
      serialized.unsafeRunSync() shouldBe expected.unsafeRunSync()
    }

    forAll { (topic: String, i: Int) =>
      val serialized =
        serializer.serialize(topic, Headers.empty, i).unsafeRunSync()

      val expected =
        Serializer[IO, String]
          .contramap[Int](_.toString)
          .serialize(topic, Headers.empty, i)
          .unsafeRunSync()

      serialized shouldBe expected
    }
  }

  test("Serializer#topic") {
    val serializer =
      Serializer.topic {
        case "topic" => Serializer[IO, Int]
        case _       => Serializer[IO, String].contramap[Int](_.toString)
      }

    forAll { (i: Int) =>
      val serialized = serializer.serialize("topic", Headers.empty, i)
      val expected   = Serializer[IO, Int].serialize("topic", Headers.empty, i)
      serialized.unsafeRunSync() shouldBe expected.unsafeRunSync()
    }

    forAll { (topic: String, i: Int) =>
      whenever(topic != "topic") {
        val serialized =
          serializer.serialize(topic, Headers.empty, i).unsafeRunSync()

        val expected =
          Serializer[IO, String]
            .contramap[Int](_.toString)
            .serialize(topic, Headers.empty, i)
            .unsafeRunSync()

        serialized shouldBe expected
      }
    }
  }

  test("Serializer#topic.unknown") {
    val serializer =
      Serializer.topic { case "topic" =>
        Serializer[IO, Int]
      }

    forAll { (headers: Headers, int: Int) =>
      assert {
        serializer.serialize("unknown", headers, int).attempt.unsafeRunSync().isLeft
      }
    }
  }

  test("Serializer#asNull") {
    val serializer =
      Serializer.asNull[IO, Int]

    forAll { (i: Int) =>
      val serialized = serializer.serialize("topic", Headers.empty, i)
      serialized.unsafeRunSync() shouldBe null
    }
  }

  test("Serializer#empty") {
    val serializer =
      Serializer.empty[IO, Int]

    forAll { (i: Int) =>
      val serialized = serializer.serialize("topic", Headers.empty, i)
      serialized.unsafeRunSync() shouldBe empty
    }
  }

  test("Serializer#option") {
    val serializer =
      Serializer[IO, Option[String]]

    serializer.serialize("topic", Headers.empty, None).unsafeRunSync() shouldBe null

    forAll { (s: String) =>
      serializer.serialize("topic", Headers.empty, Some(s)).unsafeRunSync() shouldBe
        Serializer[IO, String].serialize("topic", Headers.empty, s).unsafeRunSync()
    }
  }

  test("Serializer#unit") {
    Serializer[IO, Unit].serialize("topic", Headers.empty, ()).unsafeRunSync() shouldBe null
  }

  test("Serializer#toString") {
    assert(Serializer[IO, Int].toString.startsWith("Serializer$"))
  }

  def roundtrip[A: Arbitrary: Eq](
    serializer: Serializer[IO, A],
    deserializer: Deserializer[IO, A]
  ): Assertion = forAll { (topic: String, headers: Headers, a: A) =>
    val serialized   = serializer.serialize(topic, headers, a).unsafeRunSync()
    val deserialized = deserializer.deserialize(topic, headers, serialized).unsafeRunSync()
    assert(deserialized === a)
  }

  def roundtripAttempt[A: Arbitrary: Eq](
    serializer: Serializer[IO, A],
    deserializer: Deserializer[IO, A]
  ): Assertion = forAll { (topic: String, headers: Headers, a: A) =>
    val serialized   = serializer.serialize(topic, headers, a).unsafeRunSync()
    val deserialized = deserializer.deserialize(topic, headers, serialized)
    assert(deserialized.attempt.unsafeRunSync().toOption === Option(a))
  }

  test("Serializer#string") {
    roundtrip(
      Serializer.string[IO](StandardCharsets.UTF_8),
      Deserializer.string(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#uuid") {
    roundtripAttempt(
      Serializer.uuid[IO](StandardCharsets.UTF_8),
      Deserializer.uuid(StandardCharsets.UTF_8)
    )
  }

  test("Serializer#uuid.default") {
    roundtripAttempt(
      Serializer[IO, UUID],
      Deserializer.uuid
    )
  }

  test("Serializer#double") {
    roundtripAttempt(
      Serializer.double[IO],
      Deserializer.double
    )
  }

  test("Serializer#float") {
    roundtripAttempt(
      Serializer.float[IO],
      Deserializer.float
    )
  }

  test("Serializer#int") {
    roundtripAttempt(
      Serializer.int[IO],
      Deserializer.int
    )
  }

  test("Serializer#long") {
    roundtripAttempt(
      Serializer.long[IO],
      Deserializer.long
    )
  }

  test("Serializer#short") {
    roundtripAttempt(
      Serializer.short[IO],
      Deserializer.short
    )
  }

}
