/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.laws.discipline.*

final class HeaderDeserializerSpec extends BaseCatsSpec {

  checkAll("HeaderDeserializer", MonadTests[HeaderDeserializer].monad[String, String, String])

  test("HeaderDeserializer#delay") {
    var deserialized = false

    val deserializer =
      HeaderDeserializer
        .instance { bytes =>
          deserialized = true
          bytes
        }
        .delay

    val eval = deserializer.deserialize(Array())
    assert(!deserialized)

    val result = eval.value
    assert(deserialized)
    assert(result.isEmpty)
  }

  test("HeaderDeserializer#option") {
    val deserializer =
      HeaderDeserializer[Option[String]]

    deserializer.deserialize(null) shouldBe None

    forAll { (s: String) =>
      val serialized = HeaderSerializer[String].serialize(s)
      deserializer.deserialize(serialized) shouldBe Some(s)
    }
  }

  test("HeaderDeserializer#unit") {
    forAll { (bytes: Array[Byte]) =>
      val unit: Unit = ()
      HeaderDeserializer[Unit].deserialize(bytes) shouldBe unit
    }
  }

  test("HeaderDeserializer#toString") {
    assert(HeaderDeserializer[String].toString.startsWith("HeaderDeserializer$"))
  }

}
