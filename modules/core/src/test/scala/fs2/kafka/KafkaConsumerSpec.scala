/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

import cats.data.NonEmptySet
import cats.effect.{Fiber, IO}
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.effect.Ref
import cats.syntax.all.*
import fs2.concurrent.SignallingRef
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.internal.converters.collection.*
import fs2.Stream

import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  CooperativeStickyAssignor,
  NoOffsetForPartitionException
}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion

final class KafkaConsumerSpec extends BaseKafkaSpec {

  type Consumer = KafkaConsumer[IO, String, String]

  type ConsumerStream = Stream[IO, CommittableConsumerRecord[IO, String, String]]

  describe("creating consumers") {
    it("should support defined syntax") {
      val settings =
        ConsumerSettings[IO, String, String]

      KafkaConsumer.resource[IO, String, String](settings)
      KafkaConsumer[IO].resource(settings)

      KafkaConsumer.stream[IO, String, String](settings)
      KafkaConsumer[IO].stream(settings)

      KafkaConsumer[IO].toString should startWith("ConsumerPartiallyApplied$")
    }
  }

  describe("KafkaConsumer#stream") {
    it("should consume all records with subscribe") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .subscribeTo(topic)
            .evalTap(consumer => IO(consumer.toString should startWith("KafkaConsumer$")).void)
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .records
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds) // wait some time to catch potentially duplicated records
            .compile.toVector.unsafeRunSync()

        consumed should contain theSameElementsAs produced
      }
    }

    it("should consume all records at least once with subscribing for several consumers") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO].withGroupId("test"))
            .subscribeTo(topic)
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .records
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds) // wait some time to catch potentially duplicated records

        val res = fs2
          .Stream(
            consumed,
            consumed
          )
          .parJoinUnbounded
          .compile
          .toVector
          .unsafeRunSync()

        // duplication is currently possible.
        res.distinct should contain theSameElementsAs produced
      }
    }

    it("should consume records with assign by partitions") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val partitions = NonEmptySet.fromSetUnsafe(SortedSet(0, 1, 2))

        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO].withGroupId("test2"))
            .evalTap(_.assign(topic, partitions))
            .evalTap(consumer => IO(consumer.toString should startWith("KafkaConsumer$")).void)
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .records
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds)

        val res =
          consumed.compile.toVector.unsafeRunSync()

        res should contain theSameElementsAs produced
      }
    }

    it("should consume all records with assign without partitions") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO].withGroupId("test"))
            .evalTap(_.assign(topic))
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .records
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds)

        val res =
          consumed.compile.toVector.unsafeRunSync()

        res should contain theSameElementsAs produced
      }
    }

    it("should consume all records to several consumers with assign") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO].withGroupId("test2"))
            .evalTap(_.assign(topic))
            .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
            .records
            .map(committable => committable.record.key -> committable.record.value)
            .interruptAfter(10.seconds)

        val res = fs2
          .Stream(
            consumed,
            consumed
          )
          .parJoinUnbounded
          .compile
          .toVector
          .unsafeRunSync()

        res should contain theSameElementsAs produced ++ produced
      }
    }

    it("should read from the given offset") {
      withTopic {
        seekTest(numRecords = 100, readOffset = 90)
      }
    }

    it("should fail to read from a negative offset") {
      withTopic {
        an[IllegalArgumentException] should be thrownBy seekTest(
          numRecords = 100,
          readOffset = -1
        )(_)
      }
    }

    it("should fail to read from a partition not assigned to this consumer") {
      withTopic {
        an[IllegalStateException] should be thrownBy seekTest(
          numRecords = 100,
          readOffset = 90,
          partition = Some(123)
        )(_)
      }
    }

    it("should commit the last processed offsets") {
      commitTest { case (_, offsetBatch) =>
        offsetBatch.commit
      }
    }

    it("should interrupt the stream when terminated") {
      withTopic { topic =>
        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .subscribeTo(topic)
            .evalTap(_.terminate)
            .flatTap(_.records)
            .evalTap(_.awaitTermination)
            .compile
            .toVector
            .unsafeRunSync()

        assert(consumed.isEmpty)
      }
    }

    it("should fail with an error if not subscribed or assigned") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        val consumed =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .records
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert(consumed.left.toOption.map(_.toString).contains(NotSubscribedException().toString))
      }
    }

    it("should fail with an error if subscribe is invalid") {
      withTopic { _ =>
        val subscribeName =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .evalMap(_.subscribeTo(""))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert(subscribeName.isLeft)

        val subscribeRegex =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .subscribeTo("topic")
            .evalMap(_.subscribe("".r))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert(subscribeRegex.isLeft)
      }
    }

    it("should fail with an error if assign is invalid") {
      withTopic { _ =>
        val assignEmptyName =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .evalMap(_.assign("", NonEmptySet.fromSetUnsafe(SortedSet(0))))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert {
          assignEmptyName
            .left
            .toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalArgumentException: Topic partitions to assign to cannot have null or empty topic"
            }
        }
      }
    }

    it("an error should occur if subscribe and assign at the same time") {
      withTopic { topic =>
        val assignWithSubscribeName =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .subscribeTo(topic)
            .evalMap(_.assign(topic, NonEmptySet.fromSetUnsafe(SortedSet(0))))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert {
          assignWithSubscribeName
            .left
            .toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive"
            }
        }

        val subscribeWithAssignWithName =
          KafkaConsumer
            .stream(consumerSettings[IO])
            .evalTap(_.assign(topic, NonEmptySet.fromSetUnsafe(SortedSet(0))))
            .evalMap(_.subscribeTo(topic))
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        assert {
          subscribeWithAssignWithName
            .left
            .toOption
            .map(_.toString)
            .contains {
              "java.lang.IllegalStateException: Subscription to topics, partitions and pattern are mutually exclusive"
            }
        }
      }
    }

    it("should propagate consumer errors to stream") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        val consumed =
          KafkaConsumer
            .stream {
              consumerSettings[IO].withAutoOffsetReset(AutoOffsetReset.None)
            }
            .subscribeTo(topic)
            .records
            .compile
            .lastOrError
            .attempt
            .unsafeRunSync()

        consumed.left.toOption match {
          case Some(_: NoOffsetForPartitionException) => succeed
          case Some(cause)                            => fail("Unexpected exception", cause)
          case None                                   => fail(s"Unexpected result [$consumed]")
        }
      }
    }

    it("should be able to work with offsets") {
      withTopic { topic =>
        createCustomTopic(topic)

        // Produce records in two chunks to improve chances that records get
        // assigned different timestamps.
        val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced.take(10))
        publishToKafka(topic, produced.drop(10))

        val topicPartition  = new TopicPartition(topic, 0)
        val topicPartitions = Set(topicPartition)
        val timeout         = 10.seconds

        KafkaConsumer
          .stream(consumerSettings[IO])
          .subscribeTo(topic)
          .flatTap { consumer =>
            consumer
              .records
              .take(produced.size.toLong)
              .map(_.offset)
              .chunks
              .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)
          }
          .evalTap { consumer =>
            for {
              start        <- consumer.beginningOffsets(topicPartitions)
              startTimeout <- consumer.beginningOffsets(topicPartitions, timeout)
              _            <- IO(assert(start == startTimeout && start == Map(topicPartition -> 0L)))
            } yield ()
          }
          .evalTap { consumer =>
            for {
              end        <- consumer.endOffsets(topicPartitions)
              endTimeout <- consumer.endOffsets(topicPartitions, timeout)
              _ <- IO(
                     assert(end == endTimeout && end == Map(topicPartition -> produced.size.toLong))
                   )
            } yield ()
          }
          .evalTap { consumer =>
            for {
              earliest        <- consumer.offsetsForTimes(Map(topicPartition -> 0L))
              earliestTimeout <- consumer.offsetsForTimes(Map(topicPartition -> 0L), timeout)
              _ <-
                IO(
                  assert(earliest == earliestTimeout && earliest(topicPartition).get.offset() == 0L)
                )
              earliestTimestampPlus1 = earliest(topicPartition).get.timestamp() + 1L
              afterEarliest <-
                consumer.offsetsForTimes(Map(topicPartition -> earliestTimestampPlus1))
              afterEarliestTimeout <-
                consumer.offsetsForTimes(Map(topicPartition -> earliestTimestampPlus1), timeout)
              topicPartitionAfterEarliest = afterEarliest(topicPartition).get
              _ <- IO(
                     assert(
                       afterEarliest == afterEarliestTimeout && topicPartitionAfterEarliest
                         .offset() > 0L && topicPartitionAfterEarliest
                         .timestamp() >= earliestTimestampPlus1
                     )
                   )
            } yield ()
          }
          .evalTap { consumer =>
            for {
              assigned <- consumer.assignment
              _        <- IO(assert(assigned.nonEmpty))
              _        <- consumer.seekToBeginning(assigned)
              start    <- assigned.toList.parTraverse(consumer.position)
              _        <- IO(start.forall(_ === 0))
              _        <- consumer.seekToEnd(assigned)
              end      <- assigned.toList.parTraverse(consumer.position(_, 10.seconds))
              _        <- IO(end.sum === produced.size)
              _        <- consumer.seekToBeginning
              start    <- assigned.toList.parTraverse(consumer.position)
              _        <- IO(start.forall(_ === 0))
              _        <- consumer.seekToEnd
              end      <- assigned.toList.parTraverse(consumer.position(_, 10.seconds))
              _        <- IO(end.sum === produced.size)
            } yield ()
          }
          .compile
          .drain
          .unsafeRunSync()
      }
    }

    it("should return empty partition for offsets by times as None") {
      withTopic { topic =>
        createCustomTopic(topic)

        val emptyPartition = new TopicPartition(topic, 0)
        val timeout        = 10.seconds

        KafkaConsumer
          .resource(consumerSettings[IO])
          .use { consumer =>
            for {
              empty        <- consumer.offsetsForTimes(Map(emptyPartition -> 0L))
              emptyTimeout <- consumer.offsetsForTimes(Map(emptyPartition -> 0L), timeout)
              expected      = Map(emptyPartition -> None)
              _            <- IO(assert(empty == expected && emptyTimeout == expected))
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    def seekTest(numRecords: Long, readOffset: Long, partition: Option[Int] = None)(
      topic: String
    ) = {
      createCustomTopic(topic)

      val produced = (0L until numRecords).map(n => s"key-$n" -> s"value->$n")
      publishToKafka(topic, produced)

      val consumed =
        KafkaConsumer
          .stream(consumerSettings[IO])
          .flatMap { consumer =>
            val validSeekParams =
              consumer
                .records
                .take(Math.max(readOffset, 1))
                .map(_.offset)
                .compile
                .toList
                .map(_.last)
                .map(co => (co.topicPartition, co.offsetAndMetadata.offset()))

            val seekParams =
              validSeekParams.map { case (topicPartition, offset) =>
                val p = partition.map(new TopicPartition(topic, _)).getOrElse(topicPartition)
                val o = Math.min(readOffset, offset)

                (p, o)
              }

            val setOffset =
              seekParams.flatMap { case (tp, o) => consumer.seek(tp, o) }

            val consume = consumer.records.take(numRecords - readOffset)

            Stream.eval(consumer.subscribeTo(topic)).drain ++
              (Stream.exec(setOffset) ++ consume)
                .map(_.record)
                .map(record => record.key -> record.value)
          }
          .compile
          .toVector
          .unsafeRunSync()

      consumed should contain theSameElementsAs produced.drop(readOffset.toInt)
    }
  }

  describe("KafkaConsumer#partitionsMapStream") {
    it("should handle rebalance") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced1     = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        val produced2     = (100 until 200).map(n => s"key-$n" -> s"value->$n")
        val producedTotal = produced1.size.toLong + produced2.size.toLong

        def startConsumer(
          consumedQueue: Queue[IO, CommittableConsumerRecord[IO, String, String]],
          stopSignal: SignallingRef[IO, Boolean]
        ): IO[Fiber[IO, Throwable, Vector[Set[Int]]]] =
          Ref[IO]
            .of(Vector.empty[Set[Int]])
            .flatMap { assignedPartitionsRef =>
              KafkaConsumer
                .stream(consumerSettings[IO])
                .subscribeTo(topic)
                .flatMap(_.partitionsMapStream)
                .filter(_.nonEmpty)
                .evalMap { assignment =>
                  assignedPartitionsRef
                    .update(_ :+ assignment.keySet.map(_.partition()))
                    .as {
                      Stream
                        .emits(
                          assignment
                            .map { case (_, stream) =>
                              stream.evalMap(consumedQueue.offer)
                            }
                            .toList
                        )
                        .covary[IO]
                    }
                }
                .flatten
                .parJoinUnbounded
                .interruptWhen(stopSignal)
                .compile
                .drain >> assignedPartitionsRef.get
            }
            .start

        (for {
          stopSignal <- SignallingRef[IO, Boolean](false)
          queue      <- Queue.unbounded[IO, CommittableConsumerRecord[IO, String, String]]
          ref        <- Ref.of[IO, Map[String, Int]](Map.empty)
          fiber1     <- startConsumer(queue, stopSignal)
          _          <- IO.sleep(5.seconds)
          _          <- IO(publishToKafka(topic, produced1))
          fiber2     <- startConsumer(queue, stopSignal)
          _          <- IO.sleep(5.seconds)
          _          <- IO(publishToKafka(topic, produced2))
          _ <- Stream
                 .fromQueueUnterminated(queue)
                 .evalMap { committable =>
                   ref.modify { counts =>
                     val key       = committable.record.key
                     val newCounts = counts.updated(key, counts.getOrElse(key, 0) + 1)
                     (newCounts, newCounts)
                   }
                 }
                 .takeWhile(_.size < 200)
                 .timeout(20.seconds)
                 .compile
                 .drain
                 .guarantee(stopSignal.set(true))
          consumer1assignments <- fiber1.joinWithNever
          consumer2assignments <- fiber2.joinWithNever
          keys                 <- ref.get
        } yield {
          assert {
            keys.size.toLong == producedTotal && {
              keys == (0 until 200)
                .map { n =>
                  s"key-$n" -> (if (n < 100) 2 else 1)
                }
                .toMap
            } &&
            consumer1assignments.size == 2 &&
            consumer1assignments(0) == Set(0, 1, 2) &&
            consumer1assignments(1).size < 3 &&
            consumer2assignments.size == 1 &&
            consumer2assignments(0).size < 3 &&
            consumer1assignments(1) ++ consumer2assignments(0) == Set(0, 1, 2)
          }
        }).unsafeRunSync()
      }
    }

    it("should handle rebalance with CooperativeStickyAssignor") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced1     = (0 until 100).map(n => s"key-$n" -> s"value->$n")
        val produced2     = (100 until 200).map(n => s"key-$n" -> s"value->$n")
        val producedTotal = produced1.size.toLong + produced2.size.toLong

        def startConsumer(
          consumedQueue: Queue[IO, CommittableConsumerRecord[IO, String, String]],
          stopSignal: SignallingRef[IO, Boolean]
        ): IO[Fiber[IO, Throwable, Vector[Set[Int]]]] =
          Ref[IO]
            .of(Vector.empty[Set[Int]])
            .flatMap { assignedPartitionsRef =>
              KafkaConsumer
                .stream(
                  consumerSettings[IO].withProperties(
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG -> classOf[
                      CooperativeStickyAssignor
                    ].getName
                  )
                )
                .subscribeTo(topic)
                .flatMap(_.partitionsMapStream)
                .filter(_.nonEmpty)
                .evalMap { assignment =>
                  assignedPartitionsRef
                    .update(_ :+ assignment.keySet.map(_.partition()))
                    .as {
                      Stream
                        .emits(
                          assignment
                            .map { case (_, stream) =>
                              stream.evalMap(consumedQueue.offer)
                            }
                            .toList
                        )
                        .covary[IO]
                    }
                }
                .flatten
                .parJoinUnbounded
                .interruptWhen(stopSignal)
                .compile
                .drain >> assignedPartitionsRef.get
            }
            .start

        (for {
          stopSignal <- SignallingRef[IO, Boolean](false)
          queue      <- Queue.unbounded[IO, CommittableConsumerRecord[IO, String, String]]
          ref        <- Ref.of[IO, Map[String, Int]](Map.empty)
          fiber1     <- startConsumer(queue, stopSignal)
          _          <- IO.sleep(5.seconds)
          _          <- IO(publishToKafka(topic, produced1))
          fiber2     <- startConsumer(queue, stopSignal)
          _          <- IO.sleep(5.seconds)
          _          <- IO(publishToKafka(topic, produced2))
          _ <- Stream
                 .fromQueueUnterminated(queue)
                 .evalMap { committable =>
                   ref.modify { counts =>
                     val key       = committable.record.key
                     val newCounts = counts.updated(key, counts.getOrElse(key, 0) + 1)
                     (newCounts, newCounts)
                   }
                 }
                 .takeWhile(_.size < 200)
                 .timeout(20.seconds)
                 .compile
                 .drain
                 .guarantee(stopSignal.set(true))
          consumer1assignments <- fiber1.joinWithNever
          consumer2assignments <- fiber2.joinWithNever
          keys                 <- ref.get
        } yield {
          assert {
            keys.size.toLong == producedTotal &&
            keys.values.sum == 236 &&
            consumer1assignments.size == 1 &&
            consumer1assignments(0) == Set(0, 1, 2) &&
            consumer2assignments.size == 1 &&
            consumer2assignments(0) == Set(2)
          }
        }).unsafeRunSync()
      }
    }

    it("should close all old streams on rebalance") {
      withTopic { topic =>
        val numPartitions = 3
        createCustomTopic(topic, partitions = numPartitions)

        val stream =
          KafkaConsumer.stream(consumerSettings[IO].withGroupId("test")).subscribeTo(topic)

        (for {
          stopSignal       <- SignallingRef[IO, Boolean](false)
          closedStreamsRef <- Ref[IO].of(Vector.empty[Int])
          assignmentNumRef <- Ref[IO].of(1)
          _ <- stream
                 .flatMap(_.partitionsMapStream)
                 .filter(_.nonEmpty)
                 .evalMap { assignment =>
                   assignmentNumRef
                     .getAndUpdate(_ + 1)
                     .map { assignmentNum =>
                       if (assignmentNum == 1) {
                         Stream
                           .emits(
                             assignment
                               .map { case (partition, partitionStream) =>
                                 partitionStream.onFinalize {
                                   closedStreamsRef.update(_ :+ partition.partition())
                                 }
                               }
                               .toList
                           )
                           .covary[IO]
                       } else if (assignmentNum == 2) {
                         Stream.eval(stopSignal.set(true)) >> Stream.empty.covary[IO]
                       } else {
                         Stream.empty.covary[IO]
                       }
                     }
                 }
                 .flatten
                 .parJoinUnbounded
                 .concurrently(
                   // run second stream to start a rebalance after initial rebalance, default timeout is 3 secs
                   Stream.sleep[IO](5.seconds) >> stream.records
                 )
                 .interruptWhen(stopSignal)
                 .compile
                 .drain
          closedStreams <- closedStreamsRef.get
        } yield assert(closedStreams.toSet == Set(0, 1, 2))).unsafeRunSync()
      }
    }

    it("should handle multiple rebalances with multiple instances under load #532") {
      withTopic { topic =>
        val numPartitions = 3
        createCustomTopic(topic, partitions = numPartitions)

        val produced = (0 until 10000).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        def run(instance: Int, allAssignments: SignallingRef[IO, Map[Int, Set[Int]]]): IO[Unit] =
          KafkaConsumer
            .stream(consumerSettings[IO].withGroupId("test"))
            .subscribeTo(topic)
            .flatMap(_.partitionsMapStream)
            .flatMap { assignment =>
              Stream.eval(allAssignments.update { current =>
                current.updated(instance, assignment.keySet.map(_.partition()))
              }) >> Stream
                .emits(
                  assignment
                    .map { case (_, partitionStream) =>
                      partitionStream.evalMap(_ => IO.sleep(10.millis)) // imitating some work
                    }
                    .toList
                )
                .parJoinUnbounded
            }
            .compile
            .drain

        def checkAssignments(
          allAssignments: SignallingRef[IO, Map[Int, Set[Int]]]
        )(instances: Set[Int]) =
          allAssignments
            .discrete
            .filter { state =>
              state.keySet == instances &&
              instances.forall { instance =>
                state.get(instance).exists(_.nonEmpty)
              } && state.values.toList.flatMap(_.toList).sorted == List(0, 1, 2)
            }
            .take(1)
            .compile
            .drain

        (for {
          allAssignments <- SignallingRef[IO, Map[Int, Set[Int]]](Map.empty)
          check           = checkAssignments(allAssignments)(_)
          fiber0         <- run(0, allAssignments).start
          _              <- check(Set(0))
          fiber1         <- run(1, allAssignments).start
          _              <- check(Set(0, 1))
          fiber2         <- run(2, allAssignments).start
          _              <- check(Set(0, 1, 2))
          _              <- fiber2.cancel
          _              <- allAssignments.update(_ - 2)
          _              <- check(Set(0, 1))
          _              <- fiber1.cancel
          _              <- allAssignments.update(_ - 1)
          _              <- check(Set(0))
          _              <- fiber0.cancel
        } yield succeed).unsafeRunSync()
      }
    }
  }

  describe("KafkaConsumer#assignmentStream") {
    it("should stream assignment updates to listeners") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        val consumer =
          for {
            queue <- Stream.eval(Queue.unbounded[IO, Option[SortedSet[TopicPartition]]])
            _ <- KafkaConsumer
                   .stream(consumerSettings[IO])
                   .subscribeTo(topic)
                   .evalMap { consumer =>
                     consumer
                       .assignmentStream
                       .evalMap(as => queue.offer(Some(as)))
                       .compile
                       .drain
                       .start
                       .void
                   }
          } yield queue

        (for {
          queue1 <- consumer
          _      <- Stream.eval(IO.sleep(5.seconds))
          queue2 <- consumer
          _      <- Stream.eval(IO.sleep(5.seconds))
          _      <- Stream.eval(queue1.offer(None))
          _      <- Stream.eval(queue2.offer(None))
          consumer1Updates <- Stream.eval(
                                Stream.fromQueueNoneTerminated(queue1).compile.toList
                              )
          consumer2Updates <- Stream.eval(
                                Stream.fromQueueNoneTerminated(queue2).compile.toList
                              )
          _ <- Stream.eval(IO(assert {
                 // Startup assignments (zero), initial assignments (all topics),
                 // revoke all on 2nd joining (zero), assign rebalanced set (< 3)
                 consumer1Updates.length == 4 &&
                 consumer1Updates.head.isEmpty &&
                 consumer1Updates(1).size == 3 &&
                 consumer1Updates(2).isEmpty &&
                 consumer1Updates(3).size < 3 &&
                 // Startup assignments (zero), initial assignments (< 3)
                 consumer2Updates.length == 2 &&
                 consumer2Updates.head.isEmpty &&
                 consumer2Updates(1).size < 3 &&
                 (consumer1Updates(3) ++ consumer2Updates(1)) == consumer1Updates(1)
               }))
        } yield ()).compile.drain.unsafeRunSync()
      }
    }

    it("should stream assignment updates to listeners when using CooperativeStickyAssignor") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        val consumer =
          for {
            queue <- Stream.eval(Queue.unbounded[IO, Option[SortedSet[TopicPartition]]])
            _ <- KafkaConsumer
                   .stream(
                     consumerSettings[IO].withProperties(
                       ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG -> classOf[
                         CooperativeStickyAssignor
                       ].getName
                     )
                   )
                   .subscribeTo(topic)
                   .evalMap { consumer =>
                     consumer
                       .assignmentStream
                       .evalMap(as => queue.offer(Some(as)))
                       .compile
                       .drain
                       .start
                       .void
                   }
          } yield queue

        (for {
          queue1 <- consumer
          _      <- Stream.eval(IO.sleep(5.seconds))
          queue2 <- consumer
          _      <- Stream.eval(IO.sleep(5.seconds))
          _      <- Stream.eval(queue1.offer(None))
          _      <- Stream.eval(queue2.offer(None))
          consumer1Updates <- Stream.eval(
                                Stream.fromQueueNoneTerminated(queue1).compile.toList
                              )
          consumer2Updates <- Stream.eval(
                                Stream.fromQueueNoneTerminated(queue2).compile.toList
                              )
          _ <- Stream.eval(IO(assert {
                 // Startup assignment (zero), initial assignment (all partitions),
                 // minimal revocation when 2nd joins (keep two)
                 consumer1Updates.length == 3 &&
                 consumer1Updates.head.isEmpty &&
                 consumer1Updates(1).size == 3 &&
                 consumer1Updates(2).size == 2 &&
                 // Startup assignments (zero), initial assignments (one)
                 consumer2Updates.length == 2 &&
                 consumer2Updates.head.isEmpty &&
                 consumer2Updates(1).size == 1 &&
                 (consumer1Updates(2) ++ consumer2Updates(1)) == consumer1Updates(1)
               }))
        } yield ()).compile.drain.unsafeRunSync()
      }
    }

    it("begin from the current assignments") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        (for {
          consumer <- KafkaConsumer.stream(consumerSettings[IO]).subscribeTo(topic)
          _        <- Stream.eval(IO.sleep(5.seconds)).concurrently(consumer.records)
          queue    <- Stream.eval(Queue.unbounded[IO, Option[SortedSet[TopicPartition]]])
          _ <- Stream.eval(
                 consumer.assignmentStream.evalTap(as => queue.offer(Some(as))).compile.drain.start
               )
          _       <- Stream.eval(IO.sleep(5.seconds))
          _       <- Stream.eval(queue.offer(None))
          updates <- Stream.eval(Stream.fromQueueNoneTerminated(queue).compile.toList)
          _ <- Stream.eval(IO(assert {
                 updates.length == 1 && updates.head.size == 3
               }))
        } yield ()).compile.drain.unsafeRunSync()
      }
    }
  }

  describe("KafkaConsumer#unsubscribe") {
    it("should correctly unsubscribe") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 1).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val cons = KafkaConsumer.stream(consumerSettings[IO].withGroupId("test")).subscribeTo(topic)

        val topicStream = (for {
          cntRef       <- Stream.eval(Ref.of[IO, Int](0))
          unsubscribed <- Stream.eval(Ref.of[IO, Boolean](false))
          partitions   <- Stream.eval(Ref.of[IO, Set[TopicPartition]](Set.empty[TopicPartition]))

          consumer1 <- cons
          consumer2 <- cons

          _ <- Stream(
                 consumer1.records.evalTap(_ => cntRef.update(_ + 1)),
                 consumer2
                   .records
                   .concurrently(
                     consumer2
                       .assignmentStream
                       .evalTap(assignedTopicPartitions => partitions.set(assignedTopicPartitions))
                   )
               ).parJoinUnbounded

          cntValue          <- Stream.eval(cntRef.get)
          unsubscribedValue <- Stream.eval(unsubscribed.get)
          _ <- Stream.eval(
                 if (cntValue >= 3 && !unsubscribedValue)          // wait for some processed elements from first consumer
                   unsubscribed.set(true) >> consumer1.unsubscribe // unsubscribe
                 else IO.unit
               )
          _ <- Stream.eval(IO(publishToKafka(topic, produced))) // publish some elements to topic

          partitionsValue <- Stream.eval(partitions.get)
        } yield partitionsValue).interruptAfter(10.seconds)

        val res = topicStream.compile.toVector.unsafeRunSync()

        res.last.size shouldBe 3 // in last message should be all partitions
      }
    }
  }

  describe("KafkaConsumer#stopConsuming") {
    it("should gracefully stop running stream") {
      withTopic { topic =>
        createCustomTopic(topic)
        val messages  = 20
        val produced1 = (0 until messages).map(n => n.toString -> n.toString).toVector
        val produced2 = (messages until messages * 2).map(n => n.toString -> n.toString).toVector
        publishToKafka(topic, produced1)

        // all messages from a single poll batch should land into one chunk
        val settings = consumerSettings[IO].withMaxPollRecords(messages)

        val run = for {
          consumedRef <- Ref[IO].of(Vector.empty[(String, String)])
          _ <- KafkaConsumer
                 .resource(settings)
                 .use { consumer =>
                   for {
                     _ <- consumer.subscribeTo(topic)
                     _ <- consumer
                            .records
                            .evalMap { msg =>
                              consumedRef
                                .getAndUpdate(_ :+ (msg.record.key -> msg.record.value))
                                .flatMap { prevConsumed =>
                                  if (prevConsumed.isEmpty) {
                                    // stop consuming right after the first message was received and publish a new batch
                                    consumer.stopConsuming >> IO(publishToKafka(topic, produced2))
                                  } else IO.unit
                                } >> msg.offset.commit
                            }
                            .compile
                            .drain
                   } yield ()
                 }
          consumed <- consumedRef.get
        } yield consumed

        val consumed = run.timeout(15.seconds).unsafeRunSync()

        // only messages from the first batch (before stopConsuming was called) should be received
        assert(consumed == produced1)
      }
    }

    it("should stop running stream even when there is no data in it") {
      withTopic { topic =>
        createCustomTopic(topic)
        val settings = consumerSettings[IO]

        val run = KafkaConsumer
          .resource(settings)
          .use { consumer =>
            for {
              _         <- consumer.subscribeTo(topic)
              runStream  = consumer.records.compile.drain
              stopStream = consumer.stopConsuming
              _         <- (runStream, IO.sleep(1.second) >> stopStream).parTupled
            } yield succeed
          }

        run.timeout(15.seconds).unsafeRunSync()
      }
    }

    it("should not start new streams after 'stopConsuming' call") {
      withTopic { topic =>
        createCustomTopic(topic)
        val produced = (0 until 10).map(n => n.toString -> n.toString).toVector
        publishToKafka(topic, produced)

        val run = KafkaConsumer
          .stream(consumerSettings[IO])
          .subscribeTo(topic)
          .evalTap(_.stopConsuming)
          .evalTap(_ => IO(publishToKafka(topic, produced)))
          .records
          .evalTap { _ =>
            IO.raiseError(new RuntimeException("Stream should be empty"))
          }

        run.compile.drain.unsafeRunSync()

        succeed
      }
    }
  }

  describe("KafkaConsumer#partitionsFor") {
    it("should correctly return partitions for topic") {
      withTopic { topic =>
        val partitions = List(0, 1, 2)

        createCustomTopic(topic, partitions = partitions.size)

        val info =
          KafkaConsumer.stream(consumerSettings[IO]).evalMap(_.partitionsFor(topic))

        val res =
          info.compile.lastOrError.unsafeRunSync()

        res.map(_.partition()) should contain theSameElementsAs partitions
        res.map(_.topic()).toSet should contain theSameElementsAs Set(topic)
      }
    }

    it("should fail when timeout is too small") {
      withTopic { topic =>
        val partitions = List(0, 1, 2)

        createCustomTopic(topic, partitions = partitions.size)

        val info =
          KafkaConsumer.stream(consumerSettings[IO]).evalTap(_.partitionsFor(topic, 1.nanos))

        val res =
          info.compile.lastOrError.attempt.unsafeRunSync()

        res.left.toOption match {
          case Some(e) => e shouldBe a[TimeoutException]
          case _       => fail("No exception was raised!")
        }
      }
    }
  }

  describe("KafkaConsumer#commitAsync") {
    it("should commit offsets of messages from the topic to which consumer assigned") {
      commitTest { case (consumer, offsetBatch) =>
        consumer.commitAsync(offsetBatch.offsets)
      }
    }
  }

  describe("KafkaConsumer#commitSync") {
    it("should commit offsets of messages from the topic to which consumer assigned") {
      commitTest { case (consumer, offsetBatch) =>
        consumer.commitSync(offsetBatch.offsets)
      }
    }
  }

  describe("KafkaConsumer#metrics") {
    it("should return metrics") {
      withTopic { topic =>
        val partitions = List(0, 1, 2)

        createCustomTopic(topic, partitions = partitions.size)

        val info =
          KafkaConsumer.stream(consumerSettings[IO]).evalMap(_.metrics)

        val res =
          info.take(1).compile.lastOrError.unsafeRunSync()

        assert(res.nonEmpty)
      }
    }
  }

  describe("KafkaConsumer#listTopics") {
    it("should return topics") {
      withTopic { topic =>
        val timeout = 10.seconds

        KafkaConsumer
          .stream(consumerSettings[IO])
          .evalTap { consumer =>
            for {
              _                  <- IO(createCustomTopic(topic, partitions = 2))
              topicsAfter        <- consumer.listTopics
              topicsAfterTimeout <- consumer.listTopics(timeout)
              _ <-
                IO(
                  assert(
                    topicsAfter.keySet == topicsAfterTimeout.keySet && topicsAfter(topic).size == 2
                  )
                )
            } yield ()
          }
          .compile
          .drain
          .unsafeRunSync()
      }
    }
  }

  describe("KafkaConsumer#consumeChunk") {
    it("should process the messages and commit the offsets") {
      withTopic { topic =>
        val produced = (0 until 5).map(n => s"key-$n" -> s"value-$n")
        publishToKafka(topic, produced)

        val consumed = for {
          ref <- Ref.of[IO, Vector[(String, String)]](Vector.empty)
          _ <- KafkaConsumer
                 .stream(consumerSettings[IO])
                 .evalTap(_.assign(topic))
                 .evalMap(IO.sleep(3.seconds).as(_)) // sleep a bit to trigger potential race condition with _.stream
                 .evalMap(
                   _.consumeChunk(chunk =>
                     chunk
                       .traverse(record => ref.getAndUpdate(_ :+ (record.key -> record.value)))
                       .as(CommitNow)
                   )
                 ).interruptAfter(10.seconds).compile.drain
          res <- ref.get
        } yield res

        val res = consumed.unsafeRunSync()

        (res should contain).theSameElementsInOrderAs(produced)

        val topicPartition = new TopicPartition(topic, 0)

        val actuallyCommitted = withKafkaConsumer(defaultConsumerProperties) { consumer =>
          consumer.committed(Set(topicPartition).asJava).asScala.toMap
        }.map { case (k, v) => k -> v.offset() }.toMap

        actuallyCommitted shouldBe Map(topicPartition -> 5L)
      }
    }
  }

  private def commitTest(
    commit: (KafkaConsumer[IO, String, String], CommittableOffsetBatch[IO]) => IO[Unit]
  ): Assertion =
    withTopic { topic =>
      val partitionsAmount = 3
      createCustomTopic(topic, partitions = partitionsAmount)
      val produced = (0 until 100).map(n => s"key-$n" -> s"value->$n")
      publishToKafka(topic, produced)

      val partitions = (0 until partitionsAmount).toSet

      val topicPartitions = partitions.map(partition => new TopicPartition(topic, partition))

      val createConsumer = KafkaConsumer.stream(consumerSettings[IO]).subscribeTo(topic)

      val committed = {
        for {
          consumer <- createConsumer
          consumed <- consumer
                        .records
                        .take(produced.size.toLong)
                        .map(_.offset)
                        .fold(CommittableOffsetBatch.empty[IO])(_ updated _)
          _                    <- Stream.eval(commit(consumer, consumed))
          committed            <- Stream.eval(consumer.committed(topicPartitions))
          committedWithTimeout <- Stream.eval(consumer.committed(topicPartitions, 10.seconds))
        } yield List(consumed.offsets, committed, committedWithTimeout)
      }.compile.lastOrError.unsafeRunSync()

      val actuallyCommitted = withKafkaConsumer(defaultConsumerProperties) { consumer =>
        consumer.committed(topicPartitions.asJava).asScala.toMap
      }

      assert {
        committed.forall { offsets =>
          offsets.values.toList.foldMap(_.offset) == produced.size.toLong &&
          offsets == actuallyCommitted
        }
      }
    }

}
