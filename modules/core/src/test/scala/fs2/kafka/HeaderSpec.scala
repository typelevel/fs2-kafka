/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.show.*

final class HeaderSpec extends BaseSpec {
  describe("Header") {
    it("toString should be Header(key -> value)") {
      assert {
        Header("key", Array(1.toByte, 2.toByte)).toString == "Header(key -> [1, 2])"
      }
    }

    it("should have consistent toString and Show") {
      assert {
        val header = Header("key", Array(1.toByte, 2.toByte))
        header.show == header.toString
      }
    }

    it("should return the key with key") {
      assert(Header("key", Array[Byte]()).key == "key")
    }

    it("should return the value with value") {
      val value = Array[Byte]()
      assert(Header("key", value).value.sameElements(value))
    }

    it("should deserialize with as") {
      forAll { (s: String) =>
        val header = Header("key", s)
        assert(header.as[String] == s)
      }
    }

    it("should deserialize with attemptAs") {
      forAll { (i: Int) =>
        val header = Header("key", i)
        assert(header.attemptAs[Int] == Right(i))
      }
    }
  }
}
