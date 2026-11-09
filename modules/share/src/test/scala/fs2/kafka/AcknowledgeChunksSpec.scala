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
import fs2.Stream
import fs2.kafka.internal.Acknowledgement
import fs2.kafka.internal.Acknowledger
import org.apache.kafka.common.TopicPartition

import cats.effect.unsafe.implicits.global

final class AcknowledgeChunksSpec extends BaseSpec {

  private val partition = new TopicPartition("topic", 0)

  private val liftF = new (IO ~> OptionT[IO, *]) {
    override def apply[A](fa: IO[A]): OptionT[IO, A] = OptionT.liftF(fa)
  }

  // Records which acknowledger each call went to, with its acknowledgements.
  private def recording(calls: Ref[IO, Vector[(String, Chunk[Acknowledgement])]], name: String) =
    new Acknowledger[IO] {
      override def acknowledge(acks: Chunk[Acknowledgement]): IO[Unit] =
        calls.update(_ :+ (name -> acks))
    }

  private def offset(acknowledger: Acknowledger[IO], offset: Long): AcknowledgeableOffset[IO] =
    AcknowledgeableOffset[IO](partition, offset, 1, acknowledger)

  private def ack(offset: Long, acknowledgeType: AcknowledgeType): Acknowledgement =
    Acknowledgement(partition -> offset, 1, acknowledgeType)

  describe("acknowledgeChunks") {
    it("should acknowledge each run of one consumer's offsets with one call, in order") {
      val test = for {
        calls <- Ref.of[IO, Vector[(String, Chunk[Acknowledgement])]](Vector.empty)
        a = recording(calls, "a")
        b = recording(calls, "b")
        _ <- Stream
          .chunk(
            Chunk(
              offset(a, 1L) -> AcknowledgeType.Accept,
              offset(a, 2L) -> AcknowledgeType.Release,
              offset(b, 1L) -> AcknowledgeType.Accept,
              offset(a, 3L) -> AcknowledgeType.Reject
            )
          )
          .through(acknowledgeChunks[IO])
          .compile
          .drain
        result <- calls.get
      } yield assert(
        result == Vector(
          "a" -> Chunk(ack(1L, AcknowledgeType.Accept), ack(2L, AcknowledgeType.Release)),
          "b" -> Chunk(ack(1L, AcknowledgeType.Accept)),
          "a" -> Chunk(ack(3L, AcknowledgeType.Reject))
        )
      )

      test.unsafeRunSync()
    }

    it("should keep offsets of one consumer in one run after mapK") {
      val test = for {
        calls <- Ref.of[IO, Vector[(String, Chunk[Acknowledgement])]](Vector.empty)
        a = recording(calls, "a")
        _ <- Stream
          .chunk(
            Chunk(
              offset(a, 1L).mapK(liftF) -> AcknowledgeType.Accept,
              offset(a, 2L).mapK(liftF) -> AcknowledgeType.Accept
            )
          )
          .through(acknowledgeChunks[OptionT[IO, *]])
          .compile
          .drain
          .value
        result <- calls.get
      } yield assert(
        result == Vector(
          "a" -> Chunk(ack(1L, AcknowledgeType.Accept), ack(2L, AcknowledgeType.Accept))
        )
      )

      test.unsafeRunSync()
    }

    it("should skip a run with a record the consumer no longer holds") {
      val stale = new Acknowledger[IO] {
        override def acknowledge(acks: Chunk[Acknowledgement]): IO[Unit] =
          IO.raiseError(RecordNotInFlightException(partition, 1L))
      }

      val test = for {
        calls <- Ref.of[IO, Vector[(String, Chunk[Acknowledgement])]](Vector.empty)
        a = recording(calls, "a")
        _ <- Stream
          .chunk(Chunk(offset(stale, 1L) -> AcknowledgeType.Accept, offset(a, 1L) -> AcknowledgeType.Accept))
          .through(acknowledgeChunks[IO])
          .compile
          .drain
        result <- calls.get
      } yield assert(result == Vector("a" -> Chunk(ack(1L, AcknowledgeType.Accept))))

      test.unsafeRunSync()
    }

    it("should raise any other error of the consumer") {
      val failing = new Acknowledger[IO] {
        override def acknowledge(acks: Chunk[Acknowledgement]): IO[Unit] =
          IO.raiseError(new IllegalStateException("closed"))
      }

      val result = Stream
        .chunk(Chunk(offset(failing, 1L) -> AcknowledgeType.Accept))
        .through(acknowledgeChunks[IO])
        .compile
        .drain
        .attempt
        .unsafeRunSync()

      assert(result.left.exists(_.getMessage == "closed"))
    }
  }

}
