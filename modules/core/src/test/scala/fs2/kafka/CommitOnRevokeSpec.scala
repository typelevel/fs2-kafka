/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.Resource
import cats.effect.std.Queue
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.internal.converters.collection.*
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration.*
import scala.util.Try

import cats.effect.unsafe.implicits.global

final class CommitOnRevokeSpec extends BaseSpec {

  private val topic = "topic"
  private val p0 = new TopicPartition(topic, 0)
  private val p1 = new TopicPartition(topic, 1)

  describe("KafkaConsumer with commitOnRevoke") {
    it("should not rewind a partition to an offset requested after it was revoked") {
      val mockConsumer = new MockConsumer[Array[Byte], Array[Byte]]("earliest")
      mockConsumer.updateBeginningOffsets(List(p0, p1).map(_ -> java.lang.Long.valueOf(0L)).toMap.asJava)

      implicit val mkConsumer: MkConsumer[IO] =
        new MkConsumer[IO] {
          def apply[G[_]](settings: ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] =
            IO.pure(mockConsumer)
        }

      def duringPoll[A](f: => A): IO[A] =
        IO.async_[A](cb => mockConsumer.schedulePollTask(() => cb(Try(f).toEither)))

      def rebalance(partitions: TopicPartition*): IO[Unit] =
        duringPoll(mockConsumer.rebalance(partitions.asJava))

      val settings =
        ConsumerSettings[IO, String, String]
          .withGroupId("group")
          .withCommitOnRevoke(true)

      val committedByOtherConsumer = new OffsetAndMetadata(1000L)

      val committed =
        (for {
          consumer <- KafkaConsumer.resource(settings)
          records <- Resource.eval(Queue.unbounded[IO, CommittableConsumerRecord[IO, String, String]])
          _ <- Resource.eval(consumer.subscribeTo(topic))
          _ <- consumer.records.evalMap(records.offer).compile.drain.background
        } yield records)
          .use { records =>
            for {
              _ <- duringPoll {
                mockConsumer.rebalance(List(p0, p1).asJava)
                mockConsumer.addRecord(
                  new KafkaByteConsumerRecord(
                    topic,
                    p1.partition,
                    0L,
                    Array.emptyByteArray,
                    Array.emptyByteArray
                  )
                )
              }
              record <- records.take
              _ <- rebalance(p0)
              _ <- record.offset.commit
              _ <- duringPoll(mockConsumer.commitSync(Map(p1 -> committedByOtherConsumer).asJava))
              _ <- rebalance(p0, p1)
              _ <- rebalance(p0)
              committed <- duringPoll {
                // MockConsumer#committed returns offset 0 for partitions which are not assigned
                mockConsumer.rebalance(List(p0, p1).asJava)
                mockConsumer.committed(Set(p1).asJava).asScala.get(p1)
              }
            } yield committed
          }
          .timeout(30.seconds)
          .unsafeRunSync()

      committed shouldBe Some(committedByOtherConsumer)
    }
  }

}
