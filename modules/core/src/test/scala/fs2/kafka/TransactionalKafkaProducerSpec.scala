/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{CompletableFuture, Future}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.producer.MkProducer

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, RecordMetadata}
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.TopicPartition
import org.scalatest.EitherValues

class TransactionalKafkaProducerSpec extends BaseKafkaSpec with EitherValues {

  describe("creating transactional producers") {
    it("should support defined syntax") {
      val settings = TransactionalProducerSettings("id", ProducerSettings[IO, String, String])

      TransactionalKafkaProducer.resource[IO, String, String](settings)
      TransactionalKafkaProducer[IO].resource(settings)

      TransactionalKafkaProducer.stream[IO, String, String](settings)
      TransactionalKafkaProducer[IO].resource(settings)

      TransactionalKafkaProducer[IO].toString should startWith(
        "TransactionalProducerPartiallyApplied$"
      )
    }
  }

  it("should be able to produce single records with offsets in a transaction") {
    withTopic { topic =>
      testSingle(
        topic,
        Some(i =>
          CommittableOffset[IO](
            new TopicPartition(topic, (i % 3).toInt),
            new OffsetAndMetadata(i),
            Some("group"),
            _ => IO.unit
          )
        )
      )
    }
  }

  it("should be able to produce single records without offsets in a transaction") {
    withTopic { topic =>
      testSingle(
        topic,
        None
      )
    }
  }

  private def testSingle(topic: String, makeOffset: Option[Long => CommittableOffset[IO]]) = {
    createCustomTopic(topic, partitions = 3)
    val toProduce = (0 to 10).map(n => s"key-$n" -> s"value-$n")

    val produced =
      (for {
        producer <- TransactionalKafkaProducer.stream(
                      TransactionalProducerSettings(
                        s"id-$topic",
                        producerSettings[IO].withRetries(Int.MaxValue)
                      )
                    )
        _ <- Stream.eval(IO(producer.toString should startWith("TransactionalKafkaProducer$")))
        (records, passthrough) <-
          Stream
            .chunk(Chunk.from(toProduce))
            .zipWithIndex
            .map { case ((key, value), i) =>
              val record = ProducerRecord(topic, key, value)

              makeOffset.fold[
                Either[
                  ProducerRecords[String, String],
                  TransactionalProducerRecords[IO, String, String]
                ]
              ](Left(ProducerRecords.one(record)))(offset =>
                Right(
                  TransactionalProducerRecords.one(
                    CommittableProducerRecords.one(
                      record,
                      offset(i)
                    )
                  )
                )
              ) -> ((key, value))
            }
        passthrough <-
          Stream
            .eval(
              records.fold(producer.produceWithoutOffsets, producer.produce).tupleRight(passthrough)
            )
            .map(_._2)
            .buffer(toProduce.size)
      } yield passthrough).compile.toVector.unsafeRunSync()

    produced should contain theSameElementsAs toProduce

    val consumed = {
      consumeNumberKeyedMessagesFrom[String, String](
        topic,
        produced.size,
        customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
      )
    }

    consumed should contain theSameElementsAs produced.toList
  }

  it("should be able to produce multiple records with offsets in a transaction") {
    withTopic { topic =>
      testMultiple(
        topic,
        Some(i =>
          CommittableOffset[IO](
            new TopicPartition(topic, i % 3),
            new OffsetAndMetadata(i.toLong),
            Some("group"),
            _ => IO.unit
          )
        )
      )
    }
  }

  it("should be able to produce multiple records without offsets in a transaction") {
    withTopic { topic =>
      testMultiple(
        topic,
        None
      )
    }
  }

  it("should be able to commit offset without producing records in a transaction") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val commitState = new AtomicBoolean(false)
      implicit val mk: MkProducer[IO] = new MkProducer[IO] {

        def apply[G[_]](settings: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
          IO.delay {
            new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
              (settings.properties: Map[String, AnyRef]).asJava,
              new ByteArraySerializer,
              new ByteArraySerializer
            ) {

              override def sendOffsetsToTransaction(
                offsets: util.Map[TopicPartition, OffsetAndMetadata],
                consumerGroupMetadata: ConsumerGroupMetadata
              ): Unit = {
                commitState.set(true)
                super.sendOffsetsToTransaction(offsets, consumerGroupMetadata)
              }

            }
          }

      }
      for {
        producer <- TransactionalKafkaProducer.stream(
                      TransactionalProducerSettings(
                        s"id-$topic",
                        producerSettings[IO].withRetries(Int.MaxValue)
                      )
                    )
        offsets = (i: Int) =>
                    CommittableOffset[IO](
                      new TopicPartition(topic, i % 3),
                      new OffsetAndMetadata(i.toLong),
                      Some("group"),
                      _ => IO.unit
                    )

        records = Chunk.from(0 to 100).map(i => CommittableProducerRecords(Chunk.empty, offsets(i)))

        results <- Stream.eval(producer.produce(records))
      } yield {
        results should be(empty)
        commitState.get shouldBe true
      }
    }.compile.lastOrError.unsafeRunSync()
  }

  private def testMultiple(topic: String, makeOffset: Option[Int => CommittableOffset[IO]]) = {
    createCustomTopic(topic, partitions = 3)
    val toProduce =
      Chunk.from((0 to 100).toList.map(n => s"key-$n" -> s"value-$n"))

    val toPassthrough = "passthrough"

    val produced =
      (for {
        producer <- TransactionalKafkaProducer.stream(
                      TransactionalProducerSettings(
                        s"id-$topic",
                        producerSettings[IO].withRetries(Int.MaxValue)
                      )
                    )
        recordsToProduce = toProduce.map { case (key, value) =>
                             ProducerRecord(topic, key, value)
                           }

        produce = makeOffset match {
                    case Some(offset) =>
                      val offsets = toProduce.mapWithIndex { case (_, i) =>
                        offset(i)
                      }
                      val records =
                        recordsToProduce
                          .zip(offsets)
                          .map { case (record, offset) =>
                            CommittableProducerRecords.one(
                              record,
                              offset
                            )
                          }
                      producer.produce(records).tupleLeft(toPassthrough)
                    case None =>
                      val records = ProducerRecords(recordsToProduce)
                      producer.produceWithoutOffsets(records).tupleLeft(toPassthrough)
                  }

        result <- Stream.eval(produce)
      } yield result).compile.lastOrError.unsafeRunSync()

    val records =
      produced
        ._2
        .map { case (record, _) =>
          record.key -> record.value
        }

    assert(records == toProduce && produced._1 == toPassthrough)

    val consumed = {
      val customConsumerProperties =
        Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
      consumeNumberKeyedMessagesFrom[String, String](
        topic,
        records.size,
        customProperties = customConsumerProperties
      )
    }

    consumed should contain theSameElementsAs records.toList
  }

  it("should not allow concurrent access to a producer during a transaction") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce =
        Chunk.from((0 to 1000000).toList.map(n => s"key-$n" -> s"value-$n"))

      val result =
        (for {
          producer <- TransactionalKafkaProducer.stream(
                        TransactionalProducerSettings(
                          s"id-$topic",
                          producerSettings[IO].withRetries(Int.MaxValue)
                        )
                      )
          recordsToProduce = toProduce.map { case (key, value) =>
                               ProducerRecord(topic, key, value)
                             }
          offsets = toProduce.mapWithIndex { case (_, i) =>
                      CommittableOffset[IO](
                        new TopicPartition(topic, i % 3),
                        new OffsetAndMetadata(i.toLong),
                        Some("group"),
                        _ => IO.unit
                      )
                    }
          records = recordsToProduce
                      .zip(offsets)
                      .map { case (record, offset) =>
                        CommittableProducerRecords.one(
                          record,
                          offset
                        )
                      }
          _ <- Stream
                 .eval(producer.produce(records))
                 .concurrently(
                   Stream.eval(
                     producer.produce(
                       TransactionalProducerRecords.one(
                         CommittableProducerRecords.one(
                           ProducerRecord[String, String](topic, "test", "test"),
                           CommittableOffset[IO](
                             new TopicPartition(topic, 0),
                             new OffsetAndMetadata(0),
                             Some("group"),
                             _ => IO.unit
                           )
                         )
                       )
                     )
                   )
                 )
        } yield ()).compile.lastOrError.attempt.unsafeRunSync()

      assert(result == Right(()))
    }
  }

  it("should abort transactions if committing offsets fails") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce     = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n")
      val toPassthrough = "passthrough"

      val error = new RuntimeException("BOOM")

      implicit val mk: MkProducer[IO] = new MkProducer[IO] {

        def apply[G[_]](settings: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
          IO.delay {
            new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
              (settings.properties: Map[String, AnyRef]).asJava,
              new ByteArraySerializer,
              new ByteArraySerializer
            ) {

              override def sendOffsetsToTransaction(
                offsets: util.Map[TopicPartition, OffsetAndMetadata],
                groupMetadata: ConsumerGroupMetadata
              ): Unit =
                if (offsets.containsKey(new TopicPartition(topic, 2))) {
                  throw error
                } else {
                  super.sendOffsetsToTransaction(offsets, groupMetadata)
                }

            }
          }

      }

      val produced =
        (for {
          producer <- TransactionalKafkaProducer.stream(
                        TransactionalProducerSettings(
                          s"id-$topic",
                          producerSettings[IO].withRetries(Int.MaxValue)
                        )
                      )
          recordsToProduce = toProduce.map { case (key, value) =>
                               ProducerRecord(topic, key, value)
                             }
          offsets = toProduce.mapWithIndex { case (_, i) =>
                      CommittableOffset(
                        new TopicPartition(topic, i % 3),
                        new OffsetAndMetadata(i.toLong),
                        Some("group"),
                        _ => IO.unit
                      )
                    }
          records = Chunk
                      .from(recordsToProduce.zip(offsets))
                      .map { case (record, offset) =>
                        CommittableProducerRecords.chunk(
                          Chunk.singleton(record),
                          offset
                        )
                      }
          result <- Stream.eval(producer.produce(records).tupleLeft(toPassthrough).attempt)
        } yield result).compile.lastOrError.unsafeRunSync()

      produced shouldBe Left(error)

      val consumedOrError = {
        Either.catchNonFatal(
          consumeFirstKeyedMessageFrom[String, String](
            topic,
            customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
        )
      }

      consumedOrError.isLeft shouldBe true
    }
  }

  it("should fail fast to produce records with multiple") {
    val (key0, value0)              = "key-0" -> "value-0"
    val (key1, value1)              = "key-1" -> "value-1"
    val (key2, value2)              = "key-2" -> "value-2"
    var transactionAborted          = false
    val expectedErrorOnSecondRecord = new RuntimeException("~Failed to produce second record~")

    withTopic { topic =>
      createCustomTopic(topic)

      implicit val mk: MkProducer[IO] = new MkProducer[IO] {

        def apply[G[_]](settings: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
          IO.delay {
            new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
              (settings.properties: Map[String, AnyRef]).asJava,
              new ByteArraySerializer,
              new ByteArraySerializer
            ) {
              override def send(
                record: org.apache.kafka.clients.producer.ProducerRecord[Array[Byte], Array[Byte]],
                callback: Callback
              ): Future[RecordMetadata] = {
                val key = new String(record.key(), StandardCharsets.UTF_8)
                val futureResult = CompletableFuture.completedFuture(
                  new RecordMetadata(new TopicPartition(topic, 0), 0, 0, 0, 0, 0)
                )

                key match {
                  case `key0` => futureResult

                  case `key1` =>
                    callback.onCompletion(null, expectedErrorOnSecondRecord)
                    Thread.sleep(500) // ensure the callback completes and the fail-fast mechanism is triggered
                    futureResult.completeExceptionally(expectedErrorOnSecondRecord)
                    futureResult

                  case key =>
                    fail(s"Unexpected key: $key, the producer should not produce any record after key $key1.")
                }
              }

              override def abortTransaction(): Unit = {
                transactionAborted = true
                super.abortTransaction()
              }
            }
          }

      }

      val producerRecords = List(
        ProducerRecord(topic, key0, value0),
        ProducerRecord(topic, key1, value1),
        ProducerRecord(topic, key2, value2)
      )
      val committableOffset = CommittableOffset[IO](
        new TopicPartition("topic-consumer", 0),
        new OffsetAndMetadata(0),
        Some("consumer-group"),
        _ => IO.raiseError(new RuntimeException("Commit should not be called")).void
      )
      val committable = CommittableProducerRecords(producerRecords, committableOffset)

      val settings = TransactionalProducerSettings(
        transactionalId = s"fail-fast-$topic",
        producerSettings = producerSettings[IO]
          .withRetries(Int.MaxValue)
          .withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
          .withFailFastProduce(true)
      )

      val result = intercept[RuntimeException] {
        TransactionalKafkaProducer
          .stream(settings)
          .evalMap(_.produce(Chunk.singleton(committable)))
          .compile
          .lastOrError
          .unsafeRunSync()
      }

      result shouldBe expectedErrorOnSecondRecord
      assert(transactionAborted, "The transaction should be aborted")
    }
  }

  it("should get metrics") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)

      val info =
        TransactionalKafkaProducer[IO]
          .stream(
            TransactionalProducerSettings(
              transactionalId = s"id-$topic",
              producerSettings = producerSettings[IO].withRetries(Int.MaxValue)
            )
          )
          .evalMap(_.metrics)

      val res =
        info.take(1).compile.lastOrError.unsafeRunSync()

      assert(res.nonEmpty)
    }
  }

}

class TransactionalKafkaProducerTimeoutSpec extends BaseKafkaSpec with EitherValues {
  it("should use user-specified transaction timeouts") {
    withTopic { topic =>
      createCustomTopic(topic, partitions = 3)
      val toProduce = (0 to 100).toList.map(n => s"key-$n" -> s"value-$n")

      implicit val mkProducer: MkProducer[IO] = new MkProducer[IO] {

        def apply[G[_]](settings: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO.delay {
          new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
            (settings.properties: Map[String, AnyRef]).asJava,
            new ByteArraySerializer,
            new ByteArraySerializer
          ) {

            override def commitTransaction(): Unit = {
              Thread.sleep(2 * transactionTimeoutInterval.toMillis)
              super.commitTransaction()
            }

          }
        }

      }

      val produced =
        (for {
          producer <- TransactionalKafkaProducer.stream(
                        TransactionalProducerSettings(
                          s"id-$topic",
                          producerSettings[IO].withRetries(Int.MaxValue)
                        ).withTransactionTimeout(transactionTimeoutInterval - 250.millis)
                      )
          recordsToProduce = toProduce.map { case (key, value) =>
                               ProducerRecord(topic, key, value)
                             }
          offset = CommittableOffset(
                     new TopicPartition(topic, 1),
                     new OffsetAndMetadata(recordsToProduce.length.toLong),
                     Some("group"),
                     _ => IO.unit
                   )
          records = TransactionalProducerRecords.one(
                      CommittableProducerRecords(recordsToProduce, offset)
                    )
          result <- Stream.eval(producer.produce(records).attempt)
        } yield result).compile.lastOrError.unsafeRunSync()

      produced.left.value shouldBe an[ProducerFencedException]

      val consumedOrError = {
        Either.catchNonFatal(
          consumeFirstKeyedMessageFrom[String, String](
            topic,
            customProperties = Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
          )
        )
      }

      consumedOrError.isLeft shouldBe true
    }
  }
}
