/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.laws.discipline.*
import cats.Eq

final class DeserializerSpec extends BaseCatsSpec {

  import cats.effect.unsafe.implicits.global

  checkAll(
    "Deserializer[IO, *]", {
      // use of Ticker causes an error since CE3.3.0
      // implicit val ticker = Ticker()
      implicit def eq[A: Eq]: Eq[IO[A]] = Eq.by(_.attempt.unsafeRunSync())
      MonadErrorTests[Deserializer[IO, *], Throwable].monadError[String, String, String]
    }
  )

  test("Deserializer#attempt") {
    forAll { (topic: String, headers: Headers, i: Int) =>
      val bytes = Serializer[IO, Int].serialize(topic, headers, i).unsafeRunSync()

      val deserialized = Deserializer[IO, Int].deserialize(topic, headers, bytes)
      assert(deserialized.attempt.unsafeRunSync().isRight)

      val deserializedTwice = Deserializer[IO, Int].deserialize(topic, headers, bytes ++ bytes)
      assert(deserializedTwice.attempt.unsafeRunSync().isLeft)
    }
  }

  test("Deserializer#fail") {
    val deserializer =
      Deserializer.fail[IO, Int](new RuntimeException)

    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized = deserializer.deserialize(topic, headers, bytes)
      assert(deserialized.attempt.unsafeRunSync().isLeft)
    }
  }

  test("Deserializer#failWith") {
    val deserializer =
      Deserializer.failWith[IO, Int]("message")

    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized = deserializer.deserialize(topic, headers, bytes)
      assert(deserialized.attempt.unsafeRunSync().isLeft)
    }
  }

  test("Deserializer#headers") {
    val deserializer =
      Deserializer.headers { headers =>
        headers("format").map(_.as[String]) match {
          case Some("int") => Deserializer[IO, Int]
          case _           => Deserializer[IO, String].map(_.toInt).suspend
        }
      }

    forAll { (topic: String, i: Int) =>
      val headers      = Headers(Header("format", "int"))
      val serialized   = Serializer[IO, Int].serialize(topic, Headers.empty, i).unsafeRunSync()
      val deserialized = deserializer.deserialize(topic, headers, serialized)
      deserialized.attempt.unsafeRunSync() shouldBe Right(i)
    }

    forAll { (topic: String, i: Int) =>
      val serialized =
        Serializer[IO, String]
          .contramap[Int](_.toString)
          .serialize(topic, Headers.empty, i)
          .unsafeRunSync()
      val deserialized =
        Deserializer[IO, String].map(_.toInt).attempt.deserialize(topic, Headers.empty, serialized)
      deserialized.unsafeRunSync() shouldBe Right(i)
    }
  }

  test("Deserializer#apply") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized =
        Deserializer[IO].deserialize(topic, headers, bytes).unsafeRunSync()

      deserialized shouldBe bytes
    }
  }

  test("Deserializer#identity") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      val deserialized =
        Deserializer.identity[IO].deserialize(topic, headers, bytes).unsafeRunSync()

      deserialized shouldBe bytes
    }
  }

  test("Deserializer#suspend") {
    forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
      var deserialized = false

      val deserializer =
        Deserializer
          .lift[IO, Array[Byte]] { bytes =>
            deserialized = true
            IO.pure(bytes)
          }
          .suspend

      val io = deserializer.deserialize(topic, headers, bytes)
      assert(!deserialized)
      io.unsafeRunSync()
      assert(deserialized)
    }
  }

  test("Deserializer#topic") {
    val deserializer =
      Deserializer.topic {
        case "topic" => Deserializer[IO, Int]
        case _       => Deserializer[IO, String].map(_.toInt).suspend
      }

    forAll { (i: Int) =>
      val serialized   = Serializer[IO, Int].serialize("topic", Headers.empty, i).unsafeRunSync()
      val deserialized = deserializer.deserialize("topic", Headers.empty, serialized)
      deserialized.attempt.unsafeRunSync() shouldBe Right(i)
    }

    forAll { (topic: String, i: Int) =>
      whenever(topic != "topic") {
        val serialized =
          Serializer[IO, String]
            .contramap[Int](_.toString)
            .serialize(topic, Headers.empty, i)
            .unsafeRunSync()
        val deserialized =
          Deserializer[IO, String].map(_.toInt).deserialize(topic, Headers.empty, serialized)

        deserialized.attempt.unsafeRunSync() shouldBe Right(i)
      }
    }
  }

  test("Deserializer#topic.unknown") {
    val deserializer =
      Deserializer.topic { case "topic" =>
        Deserializer[IO, Int]
      }

    forAll { (headers: Headers, bytes: Array[Byte]) =>
      assert {
        deserializer.deserialize("unknown", headers, bytes).attempt.unsafeRunSync().isLeft
      }
    }
  }

  test("Deserializer#attempt (implicit)") {
    val deserializer =
      Deserializer[IO, Either[Throwable, String]]

    assert(deserializer.deserialize("topic", Headers.empty, null).unsafeRunSync().isLeft)

    forAll { (s: String) =>
      val serialized = Serializer[IO, String].serialize("topic", Headers.empty, s).unsafeRunSync()
      deserializer.deserialize("topic", Headers.empty, serialized).unsafeRunSync() shouldBe Right(s)
    }
  }

  test("Deserializer#option") {
    val deserializer =
      Deserializer[IO, Option[String]]

    deserializer.deserialize("topic", Headers.empty, null).unsafeRunSync() shouldBe None

    forAll { (s: String) =>
      val serialized = Serializer[IO, String].serialize("topic", Headers.empty, s).unsafeRunSync()
      deserializer.deserialize("topic", Headers.empty, serialized).unsafeRunSync() shouldBe Some(s)
    }
  }

  test("Deserializer#unit") {
    forAll { (bytes: Array[Byte]) =>
      val unit: Unit = ()
      Deserializer[IO, Unit]
        .deserialize("topic", Headers.empty, bytes)
        .unsafeRunSync() shouldBe unit
    }
  }

  test("Deserializer#toString") {
    assert(Deserializer[IO, String].toString.startsWith("Deserializer$"))
  }

}
