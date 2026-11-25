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

final class AcknowledgeableOffsetSpec extends BaseSpec {

  private val partition = new TopicPartition("topic", 0)

  private val liftF = new (IO ~> OptionT[IO, *]) {
    override def apply[A](fa: IO[A]): OptionT[IO, A] = OptionT.liftF(fa)
  }

  describe("AcknowledgeableOffset") {
    it("should acknowledge the delivery through the consumer") {
      val test = for {
        acked <- Ref.of[IO, Chunk[Acknowledgement]](Chunk.empty)
        acknowledger = recording(acked)
        offset = AcknowledgeableOffset[IO](partition, 1L, 2, acknowledger)
        _ <- offset.accept
        _ <- offset.mapK(liftF).renew.value
        result <- acked.get
      } yield assert(
        result == Chunk(
          Acknowledgement(partition -> 1L, 2, AcknowledgeType.Accept),
          Acknowledgement(partition -> 1L, 2, AcknowledgeType.Renew)
        )
      )

      test.unsafeRunSync()
    }

    it("should stay equal to itself after mapK") {
      val acknowledger = recording(Ref.unsafe[IO, Chunk[Acknowledgement]](Chunk.empty))
      val offset = AcknowledgeableOffset[IO](partition, 1L, 1, acknowledger)
      val mapped = offset.mapK(liftF)

      assert(mapped == offset.mapK(liftF))
      assert(mapped.acknowledger == offset.acknowledger)
      assert(mapped.hashCode == offset.mapK(liftF).hashCode)
    }

    it("should have expected toString") {
      val acknowledger = recording(Ref.unsafe[IO, Chunk[Acknowledgement]](Chunk.empty))
      val offset = AcknowledgeableOffset[IO](partition, 1L, 3, acknowledger)

      assert(offset.toString == "AcknowledgeableOffset(topic-0 -> 1, deliveryCount = 3)")
    }
  }

  private def recording(acked: Ref[IO, Chunk[Acknowledgement]]): Acknowledger[IO] =
    new Acknowledger[IO] {
      override def acknowledge(acks: Chunk[Acknowledgement]): IO[Unit] = acked.update(_ ++ acks)
    }

}
