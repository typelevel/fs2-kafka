/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.Chunk
import fs2.kafka.AcknowledgeType
import fs2.kafka.BaseSpec
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.ShareConsumerState.offsetOf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration.*

final class ShareConsumerStateSpec extends BaseSpec {

  private val p0 = new TopicPartition("topic", 0)
  private val p1 = new TopicPartition("topic", 1)

  private def record(partition: TopicPartition, offset: Long): KafkaByteConsumerRecord =
    new ConsumerRecord(
      partition.topic,
      partition.partition,
      offset,
      Array.emptyByteArray,
      Array.emptyByteArray
    )

  private val r0 = record(p0, 0L)
  private val r1 = record(p0, 1L)
  private val r2 = record(p1, 0L)

  private val start = 1.second
  private val lock = 30.seconds

  /**
    * A poll which started at `start`, with every record held since then.
    */
  private def held(records: KafkaByteConsumerRecord*): ShareConsumerState =
    ShareConsumerState.empty.hold(
      records.toVector.map(r => offsetOf(r) -> Delivery(r, 1, start)),
      start,
      Some(lock)
    )

  private def ack(record: KafkaByteConsumerRecord, kind: AcknowledgeType): Acknowledgement =
    Acknowledgement(offsetOf(record), 1, kind)

  private def acknowledged(state: ShareConsumerState, at: FiniteDuration)(
    acks: Acknowledgement*
  ): ShareConsumerState =
    state.acknowledge(Chunk.from(acks), at)._1

  private def offsets(records: KafkaByteConsumerRecord*): Set[TopicPartitionOffset] =
    records.map(offsetOf).toSet

  describe("ShareConsumerState#hold") {
    it("should hold the records of a poll without an acknowledgement") {
      val state = held(r0, r1)

      state.open shouldBe true
      state.unacked shouldBe 2
      state.renewing shouldBe false
      state.takeUnsent._2 shouldBe empty
    }

    it("should keep the last lock when the broker reports none") {
      held(r0).hold(Vector.empty, start, None).lock shouldBe Some(lock)
    }
  }

  describe("ShareConsumerState#acknowledge") {
    it("should keep the last acknowledgement of a record") {
      val state = acknowledged(held(r0), 2.seconds)(
        ack(r0, AcknowledgeType.Accept),
        ack(r0, AcknowledgeType.Release)
      )

      state.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Release)
    }

    it("should return the offsets of records which are not held") {
      val (state, notHeld) = held(r0).acknowledge(
        Chunk(ack(r0, AcknowledgeType.Accept), ack(r1, AcknowledgeType.Accept)),
        2.seconds
      )

      notHeld shouldBe Vector(offsetOf(r1))
      state.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Accept)
    }

    it("should not hold a record for another delivery of it") {
      val earlier = Acknowledgement(offsetOf(r0), 2, AcknowledgeType.Accept)

      held(r0).acknowledge(Chunk(earlier), 2.seconds)._2 shouldBe Vector(offsetOf(r0))
    }

    it("should not hold any record once the poll is closed") {
      val closed = held(r0).close(AcknowledgeType.Release, 2.seconds)

      closed.acknowledge(Chunk(ack(r0, AcknowledgeType.Accept)), 3.seconds)._2 shouldBe
        Vector(offsetOf(r0))
    }

    it("should keep an acknowledgement for a record being renewed until it is back") {
      val renewing = acknowledged(held(r0).takeRenewal(2.seconds)._1, 3.seconds)(
        ack(r0, AcknowledgeType.Accept)
      )
      val back = renewing.endRenewal(Vector(r0), None, 4.seconds)._1

      renewing.takeUnsent._2 shouldBe empty
      back.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Accept)
    }
  }

  describe("ShareConsumerState#takeUnsent") {
    it("should take the acknowledgements and no longer hold their records") {
      val (taken, acks) =
        acknowledged(held(r0, r1), 2.seconds)(ack(r0, AcknowledgeType.Accept)).takeUnsent

      acks shouldBe Vector(r0 -> AcknowledgeType.Accept)
      taken.records.keySet shouldBe offsets(r1)
      taken.takeUnsent._2 shouldBe empty
    }

    it("should not take a renewal by the user") {
      val state = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Renew))

      state.takeUnsent._2 shouldBe empty
      state.records.keySet shouldBe offsets(r0)
    }
  }

  describe("ShareConsumerState#takeOverdue") {
    it("should acknowledge the records held for the max processing time") {
      val (state, acks) = held(r0, r1).takeOverdue(10.seconds, AcknowledgeType.Reject, start + 10.seconds)

      acks.toSet shouldBe Set(r0 -> AcknowledgeType.Reject, r1 -> AcknowledgeType.Reject)
      state.records shouldBe empty
    }

    it("should leave records held for less") {
      held(r0).takeOverdue(10.seconds, AcknowledgeType.Release, start + 9.seconds)._2 shouldBe empty
    }

    it("should count from the latest renewal by the user") {
      val renewed = acknowledged(held(r0, r1), 2.seconds)(ack(r1, AcknowledgeType.Renew))
      val back = renewed.takeRenewal(5.seconds)._1.endRenewal(Vector(r0, r1), None, 6.seconds)._1

      back.takeOverdue(10.seconds, AcknowledgeType.Release, start + 10.seconds)._2 shouldBe
        Vector(r0 -> AcknowledgeType.Release)
    }
  }

  describe("ShareConsumerState#close") {
    it("should acknowledge the records left with the given type") {
      val closed = acknowledged(held(r0, r1), 2.seconds)(ack(r0, AcknowledgeType.Accept))
        .close(AcknowledgeType.Release, 3.seconds)

      closed.open shouldBe false
      closed.takeUnsent._2.toSet shouldBe
        Set(r0 -> AcknowledgeType.Accept, r1 -> AcknowledgeType.Release)
    }

    it("should replace a renewal by the user which was not sent") {
      val closed = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Renew))
        .close(AcknowledgeType.Release, 3.seconds)

      closed.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Release)
    }

    it("should acknowledge records being renewed once they are back") {
      val closed = held(r0).takeRenewal(2.seconds)._1.close(AcknowledgeType.Reject, 3.seconds)
      val back = closed.endRenewal(Vector(r0), None, 4.seconds)._1

      closed.takeUnsent._2 shouldBe empty
      back.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Reject)
    }

    it("should keep an acknowledgement made during a renewal") {
      val renewing = acknowledged(held(r0).takeRenewal(2.seconds)._1, 3.seconds)(
        ack(r0, AcknowledgeType.Accept)
      )
      val back = renewing.close(AcknowledgeType.Release, 4.seconds).endRenewal(Vector(r0), None, 5.seconds)._1

      back.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Accept)
    }
  }

  describe("ShareConsumerState#takeRenewal") {
    it("should renew every unacknowledged record and take the acknowledgements to send") {
      val state = acknowledged(held(r0, r1, r2), 2.seconds)(
        ack(r0, AcknowledgeType.Accept),
        ack(r1, AcknowledgeType.Renew)
      )
      val (renewing, renewal) = state.takeRenewal(3.seconds)

      renewal.acks shouldBe Vector(r0 -> AcknowledgeType.Accept)
      renewal.records.toSet shouldBe Set(offsetOf(r1) -> r1, offsetOf(r2) -> r2)
      renewing.renewing shouldBe true
      renewing.nextRenewal(None) shouldBe None
    }
  }

  describe("ShareConsumerState#skipRenewal") {
    it("should drop every renewal by the user, also of one partition") {
      val state = acknowledged(held(r0, r1), 2.seconds)(
        ack(r0, AcknowledgeType.Renew),
        ack(r1, AcknowledgeType.Renew)
      )
      val skipped = state.skipRenewal(3.seconds)

      skipped.nextRenewal(None) shouldBe None
      skipped.unacked shouldBe 2
      skipped.records.keySet shouldBe offsets(r0, r1)
    }

    it("should count the next renewal from the skipped one") {
      held(r0).skipRenewal(3.seconds).nextRenewal(Some(10.seconds)) shouldBe Some(13.seconds)
    }
  }

  describe("ShareConsumerState#endRenewal") {
    it("should hold the returned records again and lose the others") {
      val (back, unknown) = held(r0, r1).takeRenewal(2.seconds)._1.endRenewal(Vector(r0), None, 3.seconds)

      unknown shouldBe empty
      back.records.keySet shouldBe offsets(r0)
      back.renewing shouldBe false
      back.unacked shouldBe 1
    }

    it("should acknowledge a returned record by the record which the poll returned") {
      val returned = record(p0, 0L)
      val back = held(r0).takeRenewal(2.seconds)._1.endRenewal(Vector(returned), None, 3.seconds)._1
      val acks = acknowledged(back, 4.seconds)(ack(r0, AcknowledgeType.Accept)).takeUnsent._2

      acks.map(_._1) shouldBe Vector(returned)
    }

    it("should return the returned records which are not held") {
      val (back, unknown) = held(r0).takeRenewal(2.seconds)._1.endRenewal(Vector(r0, r2), None, 3.seconds)

      unknown shouldBe Vector(r2)
      back.records.keySet shouldBe offsets(r0)
    }

    it("should renew again a record the user renewed meanwhile") {
      val renewing = acknowledged(held(r0).takeRenewal(2.seconds)._1, 3.seconds)(
        ack(r0, AcknowledgeType.Renew)
      )

      renewing.nextRenewal(None) shouldBe None
      renewing.endRenewal(Vector(r0), None, 4.seconds)._1.nextRenewal(None) shouldBe Some(Duration.Zero)
    }

    it("should keep the lock the broker reported") {
      val back = held(r0).takeRenewal(2.seconds)._1.endRenewal(Vector(r0), Some(15.seconds), 3.seconds)._1

      back.lock shouldBe Some(15.seconds)
    }
  }

  describe("ShareConsumerState#failRenewal") {
    it("should hold the records being renewed again as they were") {
      val renewing = acknowledged(held(r0, r1).takeRenewal(2.seconds)._1, 3.seconds)(
        ack(r0, AcknowledgeType.Accept)
      )
      val failed = renewing.failRenewal(4.seconds)

      failed.renewing shouldBe false
      failed.unacked shouldBe 1
      failed.takeUnsent._2 shouldBe Vector(r0 -> AcknowledgeType.Accept)
    }
  }

  describe("ShareConsumerState#nextSend") {
    it("should be due after the delay while the poll is open") {
      val state = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Accept))

      state.nextSend(Some(1.second), paused = false) shouldBe Some(3.seconds)
      state.nextSend(None, paused = false) shouldBe None
    }

    it("should be due at once while paused") {
      val state = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Accept))

      state.nextSend(None, paused = true) shouldBe Some(2.seconds)
    }

    it("should not be due for a closed poll unless paused") {
      val closed = held(r0).close(AcknowledgeType.Release, 2.seconds)

      closed.nextSend(Some(1.second), paused = false) shouldBe None
      closed.nextSend(Some(1.second), paused = true) shouldBe Some(2.seconds)
    }

    it("should not be due with nothing to send") {
      held(r0).nextSend(Some(1.second), paused = true) shouldBe None
    }
  }

  describe("ShareConsumerState#nextRenewal") {
    it("should be due at once after a renewal by the user") {
      val state = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Renew))

      state.nextRenewal(None) shouldBe Some(Duration.Zero)
    }

    it("should count from the start of the poll") {
      held(r0).nextRenewal(Some(10.seconds)) shouldBe Some(start + 10.seconds)
      held(r0).nextRenewal(None) shouldBe None
    }

    it("should not be due once every record is acknowledged") {
      val state = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Accept))

      state.nextRenewal(Some(10.seconds)) shouldBe None
    }
  }

  describe("ShareConsumerState#nextOverdue") {
    it("should count from when the record was held") {
      held(r0).nextOverdue(10.seconds) shouldBe Some(start + 10.seconds)
    }

    it("should not count a record the user renewed") {
      val state = acknowledged(held(r0), 2.seconds)(ack(r0, AcknowledgeType.Renew))

      state.nextOverdue(10.seconds) shouldBe None
    }
  }

}
