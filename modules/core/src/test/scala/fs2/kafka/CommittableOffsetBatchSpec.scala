/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.instances.*

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
        assert(
          updated.offsets.get(offset.committer) == Some(
            Map(
              offset.topicPartition -> offset.offsetAndMetadata
            )
          )
        )
      }
    }
  }

  describe("CommittableOffsetBatch#updated") {
    it("should include the provided offset when latest") {
      forAll { (batch: CommittableOffsetBatch[IO], offset: CommittableOffset[IO]) =>
        val updatedBatch = batch.updated(offset)

        assert(
          updatedBatch
            .offsets
            .get(offset.committer)
            .flatMap(_.get(offset.topicPartition))
            .exists(_ >= offset.offsetAndMetadata)
        )
      }
    }

    it("should be able to update with batch") {
      forAll { (batch1: CommittableOffsetBatch[IO], batch2: CommittableOffsetBatch[IO]) =>
        val result = batch1.updated(batch2)

        val keys = batch1.offsets.keySet ++ batch2.offsets.keySet
        assert(result.offsets.size == keys.size)

        keys.foreach { committer =>
          val first  = batch1.offsets.get(committer)
          val second = batch2.offsets.get(committer)

          first.foreach { offsets =>
            offsets.foreach { case (topicPartition, offsetAndMetadata) =>
              assert {
                result
                  .offsets
                  .get(committer)
                  .flatMap(_.get(topicPartition))
                  .exists(_ >= offsetAndMetadata)
              }
            }
          }

          second.foreach { offsets =>
            offsets.foreach { case (topicPartition, offsetAndMetadata) =>
              assert {
                result
                  .offsets
                  .get(committer)
                  .flatMap(_.get(topicPartition))
                  .exists(_ >= offsetAndMetadata)
              }
            }
          }
        }
      }
    }
  }

}
