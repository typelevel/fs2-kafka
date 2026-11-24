/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.Resource
import fs2.kafka.consumer.MkConsumer
import java.util
import java.util.concurrent.CompletableFuture
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global

final class CommitAsyncSpec extends BaseSpec {
  private val offsets = Map(new TopicPartition("topic", 0) -> new OffsetAndMetadata(1L))

  describe("KafkaConsumer#commitAsync") {
    it("should fail with CommitTimeoutException if the commit does not complete within commitTimeout") {
      val result =
        runWithin(10.seconds) {
          kafkaConsumer(new InFlightCommitsConsumer, commitTimeout = 100.millis)
            .use(_.commitAsync(offsets).attempt)
        }

      assert(result.left.exists(_.isInstanceOf[CommitTimeoutException]))
    }

    it("should be cancelable while the commit is in flight") {
      val mockConsumer = new InFlightCommitsConsumer

      val outcome =
        runWithin(10.seconds) {
          kafkaConsumer(mockConsumer, commitTimeout = 1.hour).use { consumer =>
            for {
              commit <- consumer.commitAsync(offsets).start
              _ <- mockConsumer.commitInFlight
              _ <- commit.cancel
              outcome <- commit.join
            } yield outcome
          }
        }

      assert(outcome.isCanceled)
    }

    it("should fail with CommitTimeoutException when committing after the consumer has terminated") {
      val mockConsumer = new MockConsumer[Array[Byte], Array[Byte]]("earliest")

      val result =
        runWithin(10.seconds) {
          kafkaConsumer(mockConsumer, commitTimeout = 100.millis).use { consumer =>
            consumer.terminate >> consumer.awaitTermination >> consumer.commitAsync(offsets).attempt
          }
        }

      assert(result.left.exists(_.isInstanceOf[CommitTimeoutException]))
    }
  }

  private def kafkaConsumer(
    mockConsumer: KafkaByteConsumer,
    commitTimeout: FiniteDuration
  ): Resource[IO, KafkaConsumer[IO, String, String]] = {
    implicit val mkConsumer: MkConsumer[IO] =
      new MkConsumer[IO] {
        def apply[G[_]](settings: ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] =
          IO.pure(mockConsumer)
      }

    KafkaConsumer.resource(
      ConsumerSettings[IO, String, String]
        .withGroupId("group")
        .withCommitTimeout(commitTimeout)
    )
  }

  private def runWithin[A](limit: FiniteDuration)(io: IO[A]): A =
    io.unsafeRunTimed(limit).getOrElse(fail(s"Did not complete within $limit"))

  final private class InFlightCommitsConsumer extends MockConsumer[Array[Byte], Array[Byte]]("earliest") {
    private[this] val commitRequested = new CompletableFuture[Unit]

    val commitInFlight: IO[Unit] =
      IO.fromCompletableFuture(IO.pure(commitRequested))

    override def commitAsync(
      offsets: util.Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
    ): Unit = {
      val _ = commitRequested.complete(())
    }
  }
}
