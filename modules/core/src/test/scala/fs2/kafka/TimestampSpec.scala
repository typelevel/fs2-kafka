/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.show.*

final class TimestampSpec extends BaseSpec {

  describe("Timeout#createTime") {
    it("should have a createTime") {
      forAll { (value: Long) =>
        val timestamp = Timestamp.createTime(value)
        timestamp.toOption shouldBe Some(value)
        timestamp.nonEmpty shouldBe true
        timestamp.isEmpty shouldBe false
      }
    }

    it("should include the createTime in toString") {
      forAll { (value: Long) =>
        val timestamp = Timestamp.createTime(value)
        timestamp.toString should include(s"$value")
        timestamp.toString shouldBe timestamp.show
      }
    }
  }

  describe("Timeout#logAppendTime") {
    it("should have a logAppendTime") {
      forAll { (value: Long) =>
        val timestamp = Timestamp.logAppendTime(value)
        timestamp.toOption shouldBe Some(value)
        timestamp.nonEmpty shouldBe true
        timestamp.isEmpty shouldBe false
      }
    }

    it("should include the logAppendTime in toString") {
      forAll { (value: Long) =>
        val timestamp = Timestamp.logAppendTime(value)
        timestamp.toString should include(s"$value")
        timestamp.toString shouldBe timestamp.show
      }
    }
  }

  describe("Timeout#unknownTime") {
    it("should have an unknownTime") {
      forAll { (value: Long) =>
        val timestamp = Timestamp.unknownTime(value)
        timestamp.toOption shouldBe Some(value)
        timestamp.nonEmpty shouldBe true
        timestamp.isEmpty shouldBe false
      }
    }

    it("should include the unknownTime in toString") {
      forAll { (value: Long) =>
        val timestamp = Timestamp.unknownTime(value)
        timestamp.toString should include(s"$value")
        timestamp.toString shouldBe timestamp.show
      }
    }
  }

  describe("Timestamp#none") {
    it("should not have a value") {
      Timestamp.none.toOption shouldBe None
    }

    it("should be empty") {
      Timestamp.none.isEmpty shouldBe true
      Timestamp.none.nonEmpty shouldBe false
    }

    it("should not include any time in toString") {
      Timestamp.none.toString shouldBe "NoTimestamp"
      Timestamp.none.show shouldBe Timestamp.none.toString
    }
  }

}
