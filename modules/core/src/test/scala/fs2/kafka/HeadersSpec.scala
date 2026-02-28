/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.data.Chain
import fs2.kafka.internal.converters.collection.*

final class HeadersSpec extends BaseSpec {

  describe("Headers#empty") {
    it("should have toString as Headers()") {
      assert(Headers.empty.toString == "Headers()")
    }

    it("should return true for isEmpty") {
      assert(Headers.empty.isEmpty)
    }

    it("should return false for nonEmpty") {
      assert(!Headers.empty.nonEmpty)
    }

    it("should return false for exists") {
      forAll { (key: String) =>
        assert(!Headers.empty.exists(key))
      }
    }

    it("should not find any key") {
      assert(Headers.empty("key").isEmpty)
    }

    it("should concat empty") {
      assert(Headers.empty.concat(Headers.empty).isEmpty)
    }

    it("should concat non-empty") {
      val headers = Headers(Header("key", Array[Byte]()))
      assert(Headers.empty.concat(headers).toChain.size == 1)
    }

    it("should return empty Chain from toChain") {
      assert(Headers.empty.toChain.isEmpty)
    }

    it("should create a new Headers from append(Header)") {
      assert {
        val header = Header("key", Array[Byte]())
        Headers.empty.append(header).toChain == Chain.one(header)
      }
    }

    it("should create a new Headers from append(key, value)") {
      assert {
        Headers.empty.append("key", Array[Byte]()).toChain.size == 1
      }
    }
  }

  describe("Headers#empty.asJava") {
    val empty = Headers.empty.asJava

    it("add(header) throws IllegalStateException") {
      a[IllegalStateException] should be thrownBy
        empty.add(Header("key", Array[Byte]()))
    }

    it("add(key, value) throws IllegalStateException") {
      a[IllegalStateException] should be thrownBy
        empty.add("key", Array())
    }

    it("remove throws IllegalStateException") {
      a[IllegalStateException] should be thrownBy
        empty.remove("key")
    }

    it("lastHeader returns null") {
      empty.lastHeader("key") shouldBe null
    }

    it("headers returns empty iterable") {
      assert(!empty.headers("key").iterator.hasNext)

      a[NoSuchElementException] should be thrownBy
        empty.headers("key").iterator.next()
    }

    it("toArray returns empty array") {
      assert(empty.toArray.isEmpty)
    }

    it("iterator.next throws NoSuchElementException") {
      a[NoSuchElementException] should be thrownBy
        empty.iterator.next()
    }

    it("iterator.hasNext returns false") {
      assert(!empty.iterator.hasNext)
    }
  }

  describe("Headers#nonEmpty.asJava") {
    val header  = Header("key", Array[Byte]())
    val headers = Headers(header).asJava

    it("add(header) throws IllegalStateException") {
      a[IllegalStateException] should be thrownBy
        headers.add(Header("key", Array[Byte]()))
    }

    it("add(key, value) throws IllegalStateException") {
      a[IllegalStateException] should be thrownBy
        headers.add("key", Array())
    }

    it("remove throws IllegalStateException") {
      a[IllegalStateException] should be thrownBy
        headers.remove("key")
    }

    it("lastHeader returns last header") {
      val first    = Header("key", Array[Byte](0))
      val second   = Header("key", Array[Byte](1))
      val multiple = Headers(first, second)

      multiple.asJava.lastHeader("key") shouldBe second
    }

    it("lastHeaders returns existing header") {
      headers.lastHeader("key") shouldBe header
    }

    it("lastHeader returns null for missing key") {
      headers.lastHeader("") shouldBe null
    }

    it("headers returns matching headers") {
      val list = headers.headers("key").iterator.asScala.toList
      assert(list.size == 1 && list.head == header)
    }

    it("headers returns empty if no matching headers") {
      val list = headers.headers("").iterator.asScala.toList
      assert(list.isEmpty)
    }

    it("toArray returns elements") {
      val array = headers.toArray
      assert(array.size == 1 && array.head == header)
    }

    it("iterator returns elements") {
      val list = headers.iterator.asScala.toList
      assert(list.size == 1 && list.head == header)
    }
  }

  describe("Headers#nonEmpty") {
    it("should not be empty") {
      assert(!Headers(Header("key", Array[Byte]())).isEmpty)
    }

    it("should be non empty") {
      assert(Headers(Header("key", Array[Byte]())).nonEmpty)
    }

    it("should find an existing key") {
      assert(Headers(Header("key", Array[Byte]()))("key").isDefined)
    }

    it("should find the first key") {
      val headers =
        Headers(
          Header("key", Array[Byte](1)),
          Header("key", Array[Byte](2))
        )

      assert(headers("key").map(_.value.head).contains(1.toByte))
    }

    it("should return true for existing key") {
      forAll { (key: String) =>
        assert(Headers(Header(key, "value")).exists(key))
      }
    }

    it("should return false for non-existing key") {
      forAll { (key: String) =>
        whenever(key != "key") {
          assert(!Headers(Header("key", "value")).exists(key))
        }
      }
    }

    it("should concat empty") {
      val headers = Headers(Header("key", Array[Byte]()))
      assert(headers.concat(Headers.empty).toChain.size == 1)
    }

    it("should concat non-empty") {
      val headers = Headers(Header("key", Array[Byte]()))
      assert(headers.concat(headers).toChain.size == 2)
    }

    it("should include the headers in toString") {
      val headers = Chain(Header("key1", Array[Byte]()), Header("key2", Array[Byte]()))
      assert(Headers.fromChain(headers).toString == "Headers(key1 -> [], key2 -> [])")
    }

    it("should append one more header with append(Header)") {
      val header1 = Header("key1", Array[Byte]())
      val header2 = Header("key2", Array[Byte]())

      assert(Headers(header1).append(header2).toChain.size == 2)
    }

    it("should append one more header with append(key, value)") {
      assert(Headers(Header("key1", Array[Byte]())).append("key2", Array[Byte]()).toChain.size == 2)
    }
  }

  describe("Headers#apply") {
    it("returns empty for no headers") {
      assert(Headers() == Headers.empty)
    }

    it("returns non-empty for at least one header") {
      val header = Header("key", Array[Byte]())
      assert(Headers(header).toChain == Chain.one(header))
    }
  }

  describe("Headers#fromChain") {
    it("returns empty for empty chain") {
      assert(Headers.fromChain(Chain.empty) == Headers.empty)
    }

    it("returns non-empty for non-empty chain") {
      val headers = Chain(Header("key1", Array[Byte]()), Header("key2", Array[Byte]()))
      assert(Headers.fromChain(headers).toChain == headers)
    }
  }

  describe("Headers#fromSeq") {
    it("returns empty for empty seq") {
      assert(Headers.fromSeq(Seq.empty) == Headers.empty)
    }

    it("returns non-empty for non-empty seq") {
      val headers = Seq(Header("key1", Array[Byte]()), Header("key2", Array[Byte]()))
      assert(Headers.fromSeq(headers).toChain == Chain.fromSeq(headers))
    }
  }

  describe("Headers#fromIterable") {
    it("returns empty for empty iterable") {
      assert(Headers.fromIterable(List.empty) == Headers.empty)
    }

    it("returns non-empty for non-empty iterable") {
      val headers = List(Header("key1", Array[Byte]()), Header("key2", Array[Byte]()))
      assert(Headers.fromIterable(headers).toChain == Chain.fromSeq(headers))
    }
  }

}
