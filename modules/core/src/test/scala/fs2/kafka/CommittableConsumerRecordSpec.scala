/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.~>
import cats.data.OptionT
import cats.effect.SyncIO

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class CommittableConsumerRecordSpec extends BaseSpec {
  describe("CommittableConsumerRecord") {
    it("should be able to commit the offset after mapK") {
      val partition                                         = new TopicPartition("topic", 0)
      val offsetAndMetadata                                 = new OffsetAndMetadata(0L, "metadata")
      val record                                            = ConsumerRecord("topic", 0, 0L, "key", "value")
      var committed: Map[TopicPartition, OffsetAndMetadata] = null

      val committableConsumerRecord =
        CommittableConsumerRecord[SyncIO, String, String](
          record,
          CommittableOffset[SyncIO](
            partition,
            offsetAndMetadata,
            KafkaCommitter[SyncIO](
              offsets => SyncIO { committed = offsets },
              SyncIO.raiseError(new NotImplementedError)
            )
          )
        )

      val f: SyncIO ~> OptionT[SyncIO, *] = new (SyncIO ~> OptionT[SyncIO, *]) {

        override def apply[A](fa: SyncIO[A]): OptionT[SyncIO, A] = OptionT.liftF(fa)

      }

      val mapped = committableConsumerRecord.mapK(f)

      assert(mapped.record == record)

      mapped.offset.commit.value.unsafeRunSync()

      assert(committed == Map(partition -> offsetAndMetadata))
    }
  }
}
