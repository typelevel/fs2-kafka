/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.producer.MkProducer
import fs2.Chunk
import fs2.Stream

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.TopicPartition
import org.scalatest.EitherValues

class TransactionalKafkaProducerSpec extends BaseKafkaSpec with EitherValues {

  describe("creating transactional producers") {
    it("should support defined syntax") {
      val settings = producerSettingsTransactional[IO].withTransactionId("id")

      KafkaProducer.transactional[IO, String, String](settings)
      KafkaProducer[IO].resource(settings)

      KafkaProducer.stream[IO, String, String](settings)
      KafkaProducer[IO].resource(settings)

      KafkaProducer[IO].toString should startWith(
        "TransactionalProducerPartiallyApplied$"
      )
    }
  }

  it("should be able to produce single records with offsets in a transaction") {
    val committer = kafkaCommitter("group")

    withTopic { topic =>
      testSingle(
        topic,
        Some(i =>
          CommittableOffset[IO](
            new TopicPartition(topic, (i % 3).toInt),
            new OffsetAndMetadata(i),
            committer
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
        producer <-
          KafkaProducer.stream(producerSettingsTransactional[IO].withTransactionId(s"id-$topic"))
        _                      <- Stream.eval(IO(producer.toString should startWith("TransactionalKafkaProducer$")))
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
              records
                .fold(producer.produceTransactionally, producer.produceAndCommitTransactionally)
                .tupleRight(passthrough)
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
    val committer = kafkaCommitter("group")

    withTopic { topic =>
      testMultiple(
        topic,
        Some(i =>
          CommittableOffset[IO](
            new TopicPartition(topic, i % 3),
            new OffsetAndMetadata(i.toLong),
            committer
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
      val commitState                 = new AtomicBoolean(false)
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
      val committer = kafkaCommitter("group")
      for {
        producer <- KafkaProducer.transactionalStream(
                      producerSettingsTransactional[IO].withTransactionId(s"id-$topic")
                    )
        offsets = (i: Int) =>
                    CommittableOffset[IO](
                      new TopicPartition(topic, i % 3),
                      new OffsetAndMetadata(i.toLong),
                      committer
                    )

        records = Chunk.from(0 to 100).map(i => CommittableProducerRecords(Chunk.empty, offsets(i)))

        results <- Stream.eval(producer.produceAndCommitTransactionally(records))
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
        producer <- KafkaProducer.transactionalStream(
                      producerSettingsTransactional[IO].withTransactionId(s"id-$topic")
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
                      producer.produceAndCommitTransactionally(records).tupleLeft(toPassthrough)
                    case None =>
                      val records = ProducerRecords(recordsToProduce)
                      producer.produceTransactionally(records).tupleLeft(toPassthrough)
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
          producer <- KafkaProducer.transactionalStream(
                        producerSettingsTransactional[IO].withTransactionId(s"id-$topic")
                      )
          recordsToProduce = toProduce.map { case (key, value) =>
                               ProducerRecord(topic, key, value)
                             }
          committer = kafkaCommitter("group")
          offsets   =
            toProduce.mapWithIndex { case (_, i) =>
              CommittableOffset[IO](
                new TopicPartition(topic, i % 3),
                new OffsetAndMetadata(i.toLong),
                committer
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
                 .eval(producer.produceAndCommitTransactionally(records))
                 .concurrently(
                   Stream.eval(
                     producer.produceAndCommitTransactionally(
                       TransactionalProducerRecords.one(
                         CommittableProducerRecords.one(
                           ProducerRecord[String, String](topic, "test", "test"),
                           CommittableOffset[IO](
                             new TopicPartition(topic, 0),
                             new OffsetAndMetadata(0),
                             committer
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
          producer <- KafkaProducer.transactionalStream(
                        producerSettingsTransactional[IO].withTransactionId(s"id-$topic")
                      )
          recordsToProduce = toProduce.map { case (key, value) =>
                               ProducerRecord(topic, key, value)
                             }
          committer = kafkaCommitter("group")
          offsets   =
            toProduce.mapWithIndex { case (_, i) =>
              CommittableOffset(
                new TopicPartition(topic, i % 3),
                new OffsetAndMetadata(i.toLong),
                committer
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
          result <-
            Stream.eval(
              producer.produceAndCommitTransactionally(records).tupleLeft(toPassthrough).attempt
            )
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
                val key          = new String(record.key(), StandardCharsets.UTF_8)
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
        KafkaCommitter[IO](
          _ => IO.raiseError(new RuntimeException("Commit should not be called")),
          IO.pure(consumerGroupMetadata("consumer-group"))
        )
      )
      val committable = CommittableProducerRecords(producerRecords, committableOffset)

      val settings = producerSettingsTransactional[IO]
        .withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
        .withFailFastProduce(true)
        .withTransactionId(s"fail-fast-$topic")

      val result = intercept[RuntimeException] {
        KafkaProducer
          .transactionalStream(settings)
          .evalMap(_.produceAndCommitTransactionally(Chunk.singleton(committable)))
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
        KafkaProducer[IO]
          .stream(producerSettingsTransactional[IO].withTransactionId(s"id-$topic"))
          .evalMap(_.metrics)

      val res =
        info.take(1).compile.lastOrError.unsafeRunSync()

      assert(res.nonEmpty)
    }
  }

  @annotation.nowarn("msg=deprecated")
  private def consumerGroupMetadata(groupId: String): ConsumerGroupMetadata =
    new ConsumerGroupMetadata(groupId)

  private def kafkaCommitter(groupId: String): KafkaCommitter[IO] =
    KafkaCommitter(_ => IO.unit, IO.pure(consumerGroupMetadata(groupId)))

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
          producer <- KafkaProducer.transactionalStream(
                        producerSettingsTransactional[IO].withTransactionId(s"id-$topic")
                      )
          recordsToProduce = toProduce.map { case (key, value) =>
                               ProducerRecord(topic, key, value)
                             }
          offset = CommittableOffset(
                     new TopicPartition(topic, 1),
                     new OffsetAndMetadata(recordsToProduce.length.toLong),
                     kafkaCommitter("group")
                   )
          records = TransactionalProducerRecords.one(
                      CommittableProducerRecords(recordsToProduce, offset)
                    )
          result <- Stream.eval(producer.produceAndCommitTransactionally(records).attempt)
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

  @annotation.nowarn("msg=deprecated")
  private def consumerGroupMetadata(groupId: String): ConsumerGroupMetadata =
    new ConsumerGroupMetadata(groupId)

  private def kafkaCommitter(groupId: String): KafkaCommitter[IO] =
    KafkaCommitter(_ => IO.unit, IO.pure(consumerGroupMetadata(groupId)))

}
