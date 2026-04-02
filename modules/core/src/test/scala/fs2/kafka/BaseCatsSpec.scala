/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.util.Try

import cats.*
import cats.effect.testkit.TestInstances
import cats.effect.IO
import cats.tests.*

import org.scalacheck.*

trait BaseCatsSpec extends CatsSuite with BaseGenerators with TestInstances {

  implicit def deserializerEq[A](implicit A: Eq[IO[A]]): Eq[Deserializer[IO, A]] =
    Eq.instance { (d1, d2) =>
      Try {
        forAll { (topic: String, headers: Headers, bytes: Array[Byte]) =>
          val r1 = d1.deserialize(topic, headers, bytes)
          val r2 = d2.deserialize(topic, headers, bytes)
          (r1 === r2) shouldBe true
        }
      }.isSuccess
    }

  implicit val deserializerUnitArbitrary: Arbitrary[Deserializer[IO, Unit]] =
    Arbitrary(Gen.const(Deserializer.unit[IO]))

  implicit val byteArrayEq: Eq[Array[Byte]] =
    Eq.instance { (ba1, ba2) =>
      ba1.length == ba2.length &&
      !ba1.zip(ba2).exists { case (b1, b2) => b1 != b2 }
    }

  implicit def serializerEq[A](implicit
    A: Arbitrary[A],
    E: Eq[IO[Array[Byte]]]
  ): Eq[Serializer[IO, A]] =
    Eq.instance { (s1, s2) =>
      Try {
        forAll { (topic: String, headers: Headers, a: A) =>
          val r1 = s1.serialize(topic, headers, a)
          val r2 = s2.serialize(topic, headers, a)
          assert(r1 === r2)
        }
      }.isSuccess
    }

  implicit def headerSerializerEq[A](implicit A: Arbitrary[A]): Eq[HeaderSerializer[A]] =
    Eq.instance { (s1, s2) =>
      Try {
        forAll { (a: A) =>
          val r1 = s1.serialize(a)
          val r2 = s2.serialize(a)
          (r1 should contain).theSameElementsInOrderAs(r2)
        }
      }.isSuccess
    }

  implicit def headerDeserializerEq[A](implicit A: Eq[A]): Eq[HeaderDeserializer[A]] =
    Eq.instance { (d1, d2) =>
      Try {
        forAll { (bytes: Array[Byte]) =>
          val r1 = d1.deserialize(bytes)
          val r2 = d2.deserialize(bytes)
          (r1 === r2) shouldBe true
        }
      }.isSuccess
    }

  implicit val headerDeserializerUnitArbitrary: Arbitrary[HeaderDeserializer[Unit]] =
    Arbitrary(Gen.const(HeaderDeserializer.unit))

}
