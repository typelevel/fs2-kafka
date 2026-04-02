/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.syntax.all.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Gen

final class CommittableOffsetBatchSpec extends BaseSpec {

  describe("CommittableOffsetBatch#empty") {
    val empty: CommittableOffsetBatch[IO] =
      CommittableOffsetBatch.empty

    it("should have no offsets") {
      assert(empty.offsets.isEmpty)
    }

    it("should return updated offset as batch") {
      forAll { (offset: CommittableOffset[IO]) =>
        val updated = empty.updated(offset)
        assert(updated.offsets == offset.offsets)
      }
    }
  }

  describe("CommittableOffsetBatch#updated") {
    it("should include the provided offset") {
      forAll { (batch: CommittableOffsetBatch[IO], offset: CommittableOffset[IO]) =>
        val updatedBatch  = batch.updated(offset)
        val updatedOffset = updatedBatch.offsets.get(offset.topicPartition)

        assert {
          updatedOffset.contains(offset.offsetAndMetadata)
        }
      }
    }

    it("should override using the provided offset") {
      forAll { (batch: CommittableOffsetBatch[IO]) =>
        whenever(batch.offsets.nonEmpty) {
          Gen
            .oneOf(batch.offsets.toList)
            .sample
            .foreach { offset =>
              val (topicPartition, offsetAndMetadata) = offset

              val newOffsetAndMetadata =
                new OffsetAndMetadata(Long.MaxValue, offsetAndMetadata.metadata)

              val committable =
                CommittableOffset[IO](
                  topicPartition = topicPartition,
                  offsetAndMetadata = newOffsetAndMetadata,
                  consumerGroupId = None,
                  commit = _ => IO.unit
                )

              val updatedOffset = batch.updated(committable).offsets.get(topicPartition)
              assert(updatedOffset.contains(newOffsetAndMetadata))
            }
        }
      }
    }

    it("should be able to update with batch") {
      forAll { (batch1: CommittableOffsetBatch[IO], batch2: CommittableOffsetBatch[IO]) =>
        val result = batch1.updated(batch2)

        val offsets = batch2
          .offsets
          .map { case (topicPartition, offsetAndMetadata) =>
            CommittableOffset[IO](topicPartition, offsetAndMetadata, None, _ => IO.unit)
          }

        val expected = offsets.foldLeft(batch1)(_ updated _)

        assert {
          result.offsets == expected.offsets
        }
      }
    }
  }

  describe("CommittableOffsetBatch#toString") {
    it("should provide a Show instance and matching toString") {
      assert {
        CommittableOffsetBatch.empty[Id].toString == "CommittableOffsetBatch(<empty>)" &&
        CommittableOffsetBatch.empty[Id].show == CommittableOffsetBatch.empty[Id].show
      }

      val one = CommittableOffset[IO](
        new TopicPartition("topic", 0),
        new OffsetAndMetadata(0L),
        None,
        _ => IO.unit
      ).batch

      assert {
        one.toString == "CommittableOffsetBatch(topic-0 -> 0)" &&
        one.show == one.toString
      }

      val oneMetadata = CommittableOffset[IO](
        new TopicPartition("topic", 1),
        new OffsetAndMetadata(0L, "metadata"),
        None,
        _ => IO.unit
      ).batch

      assert {
        oneMetadata.toString == "CommittableOffsetBatch(topic-1 -> (0, metadata))" &&
        oneMetadata.show == oneMetadata.toString
      }

      val two = one.updated(oneMetadata)

      assert {
        two.toString == "CommittableOffsetBatch(topic-0 -> 0, topic-1 -> (0, metadata))" &&
        two.show == two.toString
      }
    }
  }

}
