/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.internal.converters.option.*

import org.apache.kafka.clients.consumer.ConsumerRecord.{NO_TIMESTAMP, NULL_SIZE}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.record.TimestampType.*
import org.scalatest.*

final class ConsumerRecordSpec extends BaseSpec {

  describe("ConsumerRecord#fromJava") {
    it("should convert timestamps") {
      def check(timestamp: Long, timestampType: TimestampType)(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaByteConsumerRecord(
            "topic",
            0,
            1L,
            timestamp,
            timestampType,
            3,
            4,
            "key".getBytes,
            "value".getBytes,
            Headers.empty.asJava,
            none[Integer].toJava
          )

        f(
          ConsumerRecord
            .fromJava(record, Deserializer[IO, String], Deserializer[IO, String])
            .unsafeRunSync()
        )
      }

      check(NO_TIMESTAMP, NO_TIMESTAMP_TYPE)(_.timestamp.isEmpty shouldBe true)
      check(NO_TIMESTAMP, CREATE_TIME)(_.timestamp.isEmpty shouldBe true)
      check(NO_TIMESTAMP, LOG_APPEND_TIME)(_.timestamp.isEmpty shouldBe true)

      check(0, NO_TIMESTAMP_TYPE)(_.timestamp.toOption shouldBe Some(0))
      check(0, CREATE_TIME)(_.timestamp.toOption shouldBe Some(0))
      check(0, LOG_APPEND_TIME)(_.timestamp.toOption shouldBe Some(0))
    }

    it("should convert serialized key size") {
      def check(serializedKeySize: Int)(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaByteConsumerRecord(
            "topic",
            0,
            1,
            NO_TIMESTAMP,
            NO_TIMESTAMP_TYPE,
            serializedKeySize,
            4,
            "key".getBytes,
            "value".getBytes,
            Headers.empty.asJava,
            none[Integer].toJava
          )

        f(
          ConsumerRecord
            .fromJava(record, Deserializer[IO, String], Deserializer[IO, String])
            .unsafeRunSync()
        )
      }

      check(NULL_SIZE)(_.serializedKeySize shouldBe None)
      check(0)(_.serializedKeySize shouldBe Some(0))
    }

    it("should convert serialized value size") {
      def check(serializedValueSize: Int)(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaByteConsumerRecord(
            "topic",
            0,
            1,
            NO_TIMESTAMP,
            NO_TIMESTAMP_TYPE,
            3,
            serializedValueSize,
            "key".getBytes,
            "value".getBytes,
            Headers.empty.asJava,
            none[Integer].toJava
          )

        f(
          ConsumerRecord
            .fromJava(record, Deserializer[IO, String], Deserializer[IO, String])
            .unsafeRunSync()
        )
      }

      check(NULL_SIZE)(_.serializedValueSize shouldBe None)
      check(0)(_.serializedValueSize shouldBe Some(0))
    }

    it("should convert leader epoch") {
      def check(leaderEpoch: Option[Int])(
        f: ConsumerRecord[String, String] => Assertion
      ): Assertion = {
        val record =
          new KafkaByteConsumerRecord(
            "topic",
            0,
            1,
            NO_TIMESTAMP,
            NO_TIMESTAMP_TYPE,
            3,
            4,
            "key".getBytes,
            "value".getBytes,
            Headers.empty.asJava,
            leaderEpoch.map(i => i: Integer).toJava
          )

        f(
          ConsumerRecord
            .fromJava(record, Deserializer[IO, String], Deserializer[IO, String])
            .unsafeRunSync()
        )
      }

      check(None)(_.leaderEpoch shouldBe None)
      check(Some(1))(_.leaderEpoch shouldBe Some(1))
    }
  }

  describe("ConsumerRecord#toString") {
    it("should include headers when present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value").withHeaders(
          Headers(Header("key", Array[Byte]()))
        )

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, headers = Headers(key -> []))"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include timestamp if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value").withTimestamp(Timestamp.createTime(0))

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, timestamp = CreateTime(0))"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include serialized key size if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value").withSerializedKeySize(1)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, serializedKeySize = 1)"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include serialized value size if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value").withSerializedValueSize(1)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, serializedValueSize = 1)"

      record.toString shouldBe expected
      record.show shouldBe expected
    }

    it("should include leader epoch if present") {
      val record =
        ConsumerRecord("topic", 0, 1, "key", "value").withLeaderEpoch(1)

      val expected =
        "ConsumerRecord(topic = topic, partition = 0, offset = 1, key = key, value = value, leaderEpoch = 1)"

      record.toString shouldBe expected
      record.show shouldBe expected
    }
  }

}
