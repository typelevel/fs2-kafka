/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.~>
import cats.data.OptionT
import cats.effect.unsafe.implicits.global
import cats.effect.IO
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

    it("should be able to commit the offset after mapK") {
      val partition                                         = new TopicPartition("topic", 0)
      val offsetAndMetadata                                 = new OffsetAndMetadata(0L, "metadata")
      var committed: Map[TopicPartition, OffsetAndMetadata] = null

      val committableOffset =
        CommittableOffset[SyncIO](
          partition,
          offsetAndMetadata,
          KafkaCommitter[SyncIO](
            offsets => SyncIO { committed = offsets },
            SyncIO.raiseError(new NotImplementedError)
          )
        )

      val f: SyncIO ~> OptionT[SyncIO, *] = new (SyncIO ~> OptionT[SyncIO, *]) {

        override def apply[A](fa: SyncIO[A]): OptionT[SyncIO, A] = OptionT.liftF(fa)

      }

      val mapped = committableOffset.mapK(f)

      assert(mapped.topicPartition == partition)
      assert(mapped.offsetAndMetadata == offsetAndMetadata)

      mapped.commit.value.unsafeRunSync()

      assert(committed == Map(partition -> offsetAndMetadata))
    }

    it("should keep offsets from the same committer batchable after mapK") {
      val partition0                                              = new TopicPartition("topic", 0)
      val partition1                                              = new TopicPartition("topic", 1)
      var committed: List[Map[TopicPartition, OffsetAndMetadata]] = Nil

      val committer =
        KafkaCommitter[IO](
          offsets => IO { committed = offsets :: committed },
          IO.raiseError(new NotImplementedError)
        )

      val f = new (IO ~> OptionT[IO, *]) {
        override def apply[A](fa: IO[A]): OptionT[IO, A] = OptionT.liftF(fa)
      }

      val offsets =
        List(
          CommittableOffset[IO](partition0, new OffsetAndMetadata(1L), committer),
          CommittableOffset[IO](partition1, new OffsetAndMetadata(5L), committer),
          CommittableOffset[IO](partition0, new OffsetAndMetadata(2L), committer)
        ).map(_.mapK(f))

      assert(offsets.map(_.committer).distinct.size == 1)

      val batch = CommittableOffsetBatch.fromFoldable(offsets)

      batch.commit.value.unsafeRunSync()

      assert(batch.offsets.size == 1)
      assert(
        committed == List(
          Map(partition0 -> new OffsetAndMetadata(2L), partition1 -> new OffsetAndMetadata(5L))
        )
      )
    }
  }
}
