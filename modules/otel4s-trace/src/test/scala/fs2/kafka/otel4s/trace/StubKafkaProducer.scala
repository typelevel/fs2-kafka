/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.{IO, Ref, Resource}
import fs2.kafka._
import fs2.kafka.internal.ProducerMetadata
import fs2.Chunk

import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

class StubKafkaProducer[K, V](
  override private[kafka] val metadata: ProducerMetadata
) extends KafkaProducer[IO, K, V] {

  override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
    unsupported

  override def initTransactions: IO[Unit] =
    unsupported

  override def transaction: Resource[IO, Unit] =
    Resource.eval(unsupported)

  override def sendOffsetsToTransaction(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    consumerGroupMetadata: ConsumerGroupMetadata
  ): IO[Unit] =
    unsupported

  override def produceAndCommitTransactionally(
    records: Chunk[CommittableProducerRecords[IO, K, V]]
  ): IO[ProducerResult[K, V]] =
    unsupported

  override def produceTransactionally(
    records: ProducerRecords[K, V]
  ): IO[ProducerResult[K, V]] =
    unsupported

  override def metrics: IO[Map[MetricName, Metric]] =
    unsupported

  override def partitionsFor(topic: String): IO[List[PartitionInfo]] =
    unsupported

  override def withSerializers[K2, V2](
    keySerializer: KeySerializer[IO, K2],
    valueSerializer: ValueSerializer[IO, V2]
  ): KafkaProducer[IO, K2, V2] =
    new StubKafkaProducer[K2, V2](metadata)

  private def unsupported[A]: IO[A] =
    IO.raiseError(new AssertionError("StubKafkaProducer method should not be called"))

}

object StubKafkaProducer {

  trait Recorder[K, V] extends KafkaProducer[IO, K, V] {

    def getCaptured: IO[Chunk[ProducerRecord[K, V]]]
    def getCompletions: IO[Int]
    def getTransactionUses: IO[Int]
    def getSentOffsets: IO[Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]]

  }

  def metadataOnly[K, V](
    clientId: String = "producer-client"
  ): KafkaProducer[IO, K, V] =
    new StubKafkaProducer[K, V](ProducerMetadata(Some(clientId))) {
      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        IO.pure {
          IO.pure(
            records
              .zipWithIndex
              .map { case (record, index) =>
                val partition = record.partition.getOrElse(0)
                val metadata  =
                  new RecordMetadata(
                    new TopicPartition(record.topic, partition),
                    index.toLong + 42L,
                    0,
                    0L,
                    0,
                    0
                  )

                record -> metadata
              }
          )
        }
    }

  def recorder[K, V](clientId: String = "producer-client"): IO[Recorder[K, V]] =
    for {
      captured        <- Ref[IO].of(Chunk.empty[ProducerRecord[K, V]])
      completions     <- Ref[IO].of(0)
      transactionUses <- Ref[IO].of(0)
      sentOffsets     <- Ref[IO].of(
                       Vector.empty[
                         (Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)
                       ]
                     )
    } yield new StubKafkaProducer[K, V](ProducerMetadata(Some(clientId))) with Recorder[K, V] {

      override def getCaptured: IO[Chunk[ProducerRecord[K, V]]] =
        captured.get

      override def getCompletions: IO[Int] =
        completions.get

      override def getTransactionUses: IO[Int] =
        transactionUses.get

      override def getSentOffsets
        : IO[Vector[(Map[TopicPartition, OffsetAndMetadata], ConsumerGroupMetadata)]] =
        sentOffsets.get

      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        captured.set(records).as(completions.update(_ + 1).as(Chunk.empty))

      override def transaction: Resource[IO, Unit] =
        Resource.make(transactionUses.update(_ + 1))(_ => IO.unit)

      override def sendOffsetsToTransaction(
        offsets: Map[TopicPartition, OffsetAndMetadata],
        consumerGroupMetadata: ConsumerGroupMetadata
      ): IO[Unit] =
        sentOffsets.update(_ :+ (offsets -> consumerGroupMetadata))

    }

  def failingAwait[K, V](
    cause: Throwable,
    clientId: String = "producer-client"
  ): KafkaProducer[IO, K, V] =
    new StubKafkaProducer[K, V](ProducerMetadata(Some(clientId))) {
      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        IO.pure(IO.raiseError(cause))

      override def produceAndCommitTransactionally(
        records: Chunk[CommittableProducerRecords[IO, K, V]]
      ): IO[ProducerResult[K, V]] =
        IO.raiseError(cause)

      override def produceTransactionally(
        records: ProducerRecords[K, V]
      ): IO[ProducerResult[K, V]] =
        IO.raiseError(cause)
    }

}
