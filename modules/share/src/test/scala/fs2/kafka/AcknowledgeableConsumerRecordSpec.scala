/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.data.OptionT
import cats.effect.IO
import cats.effect.Ref
import cats.~>
import fs2.Chunk
import fs2.kafka.internal.Acknowledgement
import fs2.kafka.internal.Acknowledger
import org.apache.kafka.common.TopicPartition

import cats.effect.unsafe.implicits.global

final class AcknowledgeableConsumerRecordSpec extends BaseSpec {
  describe("AcknowledgeableConsumerRecord") {
    it("should be able to acknowledge the record after mapK") {
      val partition = new TopicPartition("topic", 0)
      val record = ConsumerRecord("topic", 0, 0L, "key", "value")

      val f: IO ~> OptionT[IO, *] = new (IO ~> OptionT[IO, *]) {

        override def apply[A](fa: IO[A]): OptionT[IO, A] = OptionT.liftF(fa)

      }

      val test = for {
        acked <- Ref.of[IO, Chunk[Acknowledgement]](Chunk.empty)
        acknowledger = new Acknowledger[IO] {
          override def acknowledge(acks: Chunk[Acknowledgement]): IO[Unit] = acked.update(_ ++ acks)
        }
        acknowledgeable = AcknowledgeableConsumerRecord[IO, String, String](
          record,
          AcknowledgeableOffset[IO](partition, 0L, 1, acknowledger)
        )
        mapped = acknowledgeable.mapK(f)
        _ <- mapped.accept.value
        result <- acked.get
      } yield {
        assert(mapped.record == record)
        assert(result == Chunk(Acknowledgement(partition -> 0L, 1, AcknowledgeType.Accept)))
      }

      test.unsafeRunSync()
    }
  }
}
