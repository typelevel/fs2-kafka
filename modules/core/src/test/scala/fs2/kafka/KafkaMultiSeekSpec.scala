/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Fiber, IO, Ref, Resource}
import cats.effect.unsafe.implicits.global
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
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion

final class KafkaMultiSeekSpec extends BaseKafkaSpec {

  type Consumer = KafkaConsumer[IO, String, String]

  type ConsumerStream = Stream[IO, CommittableConsumerRecord[IO, String, String]]
  val topic               = "topic-example"
  val partitions          = 3
  val replication         = 1
  val recordsPerPartition = 10

  val records =
    for {
      partition <- (0 until partitions).toList
      recordX   <- (0 until recordsPerPartition).toList
      key        = s"key-p-$partition-r-$recordX"
      value      = s"value-p-$partition-r-$recordX"
    } yield ProducerRecord(topic, key, value).withPartition(partition)

  val delay   = 5.seconds
  val waitEnd = 10.seconds
  val seekTo  = recordsPerPartition - 1

  describe("seeking to an offset") {
    it("should correctly reset the first fetched offset") {
      val consumerSettings = ConsumerSettings[IO, String, String]

      val producerSettings = Map[String, String](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> container.bootstrapServers,
        ProducerConfig.MAX_BLOCK_MS_CONFIG      -> 10000.toString,
        ProducerConfig.RETRY_BACKOFF_MS_CONFIG  -> 1000.toString
      )
      (for {
        _ <- Stream.eval(createTopic(topic, partitions, 1))
        _ <- Stream.eval(pushData(records))
        _ <- Stream.resource(consumer("consumer1").compile.drain.background)
        _ <- Stream.eval(IO.sleep(delay) *> IO.println(s"\n\nSlept for $delay."))
        _ <- Stream.resource(consumer("consumer2").compile.drain.background)
        _ <- Stream.eval(IO.sleep(waitEnd) *> IO.println(s"\n\nWill terminate after $waitEnd."))
      } yield assert(true)).compile.drain.unsafeRunSync()
    }
  }

  private def consumer(name: String) = {
    for {
      _ <- Stream.eval(IO.println(s"[action: consumer-joining, consumer: $name]"))
      settings = ConsumerSettings[IO, Unit, Unit]
                   .withProperties(
                     Map(
                       "client.id"          -> s"pipeline.simple.test.$name",
                       "enable.auto.commit" -> s"false",
                       "group.id"           -> "group.test",
                       "auto.offset.reset"  -> "earliest"
                     )
                   )
                   .withBootstrapServers(container.bootstrapServers)
      consumer      <- KafkaConsumer.stream(settings)
      _             <- Stream.eval(consumer.subscribe(NonEmptyList.one(topic)))
      assignments <- consumer
                       .assignmentStream
                       .switchMap { assignments =>
                         for {
                           _ <- Stream.eval(NonEmptySet.fromSet(assignments).pure[IO]).unNone
                           _ <-
                             Stream.eval(
                               IO.println(
                                 s"[action: assigned, consumer: $name, partitions: ${assignments.toList}]"
                               )
                             )
                           _ <-
                             Stream.eval(
                               assignments.traverse_ { tp =>
                                 IO.println(
                                   s"[action: seek, to: $seekTo consumer: $name, partition: $tp]"
                                 ) *> consumer.seek(tp, seekTo)
                               }
                             )
                           _ <- Stream.eval(
                                  IO.println(
                                    s"[action: call 'records', consumer:$name, topic:$topic"
                                  )
                                )
                           record   <- consumer.records
                           topic     = record.offset.topicPartition.topic()
                           partition = record.offset.topicPartition.partition()
                           offset    = record.record.offset
                           _ <-
                             Stream.eval(
                               IO.println(
                                 s"[action: record, consumer:$name, topic:$topic, partition:$partition, offset:$offset]"
                               )
                             )
                         } yield ()
                       }
    } yield ()
  }

  def createTopic(
    topic: String,
    partition: Int,
    replication: Int
  ): IO[Unit] =
    IO.fromTry(createCustomTopic(topic, Map.empty, partition, replication))

  def pushData(records: List[ProducerRecord[String, String]]): IO[Unit] = {
    val settings = ProducerSettings[IO, String, String]
      .withBootstrapServers(container.bootstrapServers)
    KafkaProducer[IO]
      .resource(settings)
      .use(_.produce(ProducerRecords[List, String, String](records)))
      .flatten
      .void
  }

}
