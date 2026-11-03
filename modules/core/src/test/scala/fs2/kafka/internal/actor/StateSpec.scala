/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.syntax.apply.*
import fs2.kafka.BaseSpec
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class StateSpec extends BaseSpec {

  private val p0 = new TopicPartition("topic", 0)
  private val p1 = new TopicPartition("topic", 1)

  private def assigned(partitions: TopicPartition*): State[IO, String, String] = {
    val group = (
      Semaphore[IO](1),
      Deferred[IO, Either[Throwable, Unit]],
      Queue.unbounded[IO, fs2.Chunk[fs2.kafka.CommittableConsumerRecord[IO, String, String]]]
    ).mapN(PartitionGroupState(_, _, _, Nil)).unsafeRunSync()
    State.empty[IO, String, String].withGroupState(Map(partitions.toSet -> group))
  }

  private def offset(n: Long): OffsetAndMetadata = new OffsetAndMetadata(n)

  describe("State#withRequestedCommitOffsets") {
    it("should keep the newest offset of an assigned partition") {
      val state =
        assigned(p0)
          .withRequestedCommitOffsets(Map(p0 -> offset(5)))
          .withRequestedCommitOffsets(Map(p0 -> offset(3)))

      state.requestedCommitOffsets shouldBe Map(p0 -> offset(5))
    }

    it("should ignore offsets of partitions which are not assigned") {
      val state = assigned(p0).withRequestedCommitOffsets(Map(p0 -> offset(5), p1 -> offset(7)))

      state.requestedCommitOffsets shouldBe Map(p0 -> offset(5))
    }

    it("should not keep an offset committed after its partition was revoked") {
      val (revokedState, committedOnRevoke) =
        assigned(p0, p1)
          .withRequestedCommitOffsets(Map(p1 -> offset(10)))
          .removeRequestedCommitOffsets(Set(p1))
      val state =
        revokedState
          .withGroupState(assigned(p0).partitionGroupState)
          .withRequestedCommitOffsets(Map(p1 -> offset(11)))

      committedOnRevoke shouldBe Map(p1 -> offset(10))
      state.requestedCommitOffsets shouldBe empty
    }
  }

  describe("State#withoutRequestedCommitOffsets") {
    it("should forget offsets of the given partitions") {
      val state =
        assigned(p0, p1)
          .withRequestedCommitOffsets(Map(p0 -> offset(5), p1 -> offset(7)))
          .withoutRequestedCommitOffsets(Set(p1))

      state.requestedCommitOffsets shouldBe Map(p0 -> offset(5))
    }
  }

  describe("State#withUnsubscribed") {
    it("should forget all requested offsets") {
      val state =
        assigned(p0).withRequestedCommitOffsets(Map(p0 -> offset(5))).withUnsubscribed()

      state.requestedCommitOffsets shouldBe empty
    }
  }

}
