/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.effect.Ref
import cats.ApplicativeThrow
import fs2.Chunk
import fs2.Stream

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final class KafkaSpec extends BaseAsyncSpec {

  describe("commitBatchWithin") {
    it("should batch commit according specified arguments") {
      val committed =
        (for {
          ref    <- Stream.eval(Ref[IO].of(Option.empty[Map[TopicPartition, OffsetAndMetadata]]))
          commit  = (offsets: Map[TopicPartition, OffsetAndMetadata]) => ref.set(Some(offsets))
          offsets = Chunk.from(exampleOffsets(commit))
          _      <- Stream.chunk(offsets).covary[IO].through(commitBatchWithin(offsets.size, 10.seconds))
          result <- Stream.eval(ref.get)
        } yield result).compile.lastOrError.unsafeRunSync()

      assert(committed.contains(exampleOffsetsCommitted))
    }
  }

  def exampleOffsets[F[_]: ApplicativeThrow](
    commit: Map[TopicPartition, OffsetAndMetadata] => F[Unit]
  ): List[CommittableOffset[F]] = {
    val committer =
      KafkaCommitter[F](commit, ApplicativeThrow[F].raiseError(new NotImplementedError))

    List(
      CommittableOffset[F](
        new TopicPartition("topic", 0),
        new OffsetAndMetadata(1L),
        committer
      ),
      CommittableOffset[F](
        new TopicPartition("topic", 0),
        new OffsetAndMetadata(2L),
        committer
      ),
      CommittableOffset[F](
        new TopicPartition("topic", 1),
        new OffsetAndMetadata(1L),
        committer
      ),
      CommittableOffset[F](
        new TopicPartition("topic", 1),
        new OffsetAndMetadata(2L),
        committer
      ),
      CommittableOffset[F](
        new TopicPartition("topic", 1),
        new OffsetAndMetadata(3L),
        committer
      )
    )
  }

  val exampleOffsetsCommitted: Map[TopicPartition, OffsetAndMetadata] =
    Map(
      new TopicPartition("topic", 0) -> new OffsetAndMetadata(2L),
      new TopicPartition("topic", 1) -> new OffsetAndMetadata(3L)
    )

}
