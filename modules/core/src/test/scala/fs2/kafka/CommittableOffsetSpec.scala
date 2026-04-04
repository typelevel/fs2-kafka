/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.SyncIO

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class CommittableOffsetSpec extends BaseSpec {
  describe("CommittableOffset") {
    it("should be able to commit the offset") {
      val partition                                         = new TopicPartition("topic", 0)
      val offsetAndMetadata                                 = new OffsetAndMetadata(0L, "metadata")
      var committed: Map[TopicPartition, OffsetAndMetadata] = null

      CommittableOffset[SyncIO](
        partition,
        offsetAndMetadata,
        KafkaCommitter[SyncIO](
          offsets => SyncIO { committed = offsets },
          SyncIO.raiseError(new NotImplementedError)
        )
      ).commit.unsafeRunSync()

      assert(committed == Map(partition -> offsetAndMetadata))
    }
  }
}
