/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

import cats.{Foldable, Reducible}
import cats.data.NonEmptySet
import cats.effect.IO
import fs2.kafka.{CommittableConsumerRecord, KafkaConsumer}
import fs2.kafka.internal.ConsumerMetadata
import fs2.Stream

import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata,
  OffsetAndMetadata,
  OffsetAndTimestamp
}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

class StubKafkaConsumer[K, V](
  override private[kafka] val metadata: ConsumerMetadata
) extends KafkaConsumer.Unsealed[IO, K, V] {

  def assignment: IO[SortedSet[TopicPartition]] = unsupported

  def assignmentStream: Stream[IO, SortedSet[TopicPartition]] = Stream.eval(unsupported)

  def assign(partitions: NonEmptySet[TopicPartition]): IO[Unit] = unsupported

  def assign(topic: String): IO[Unit] = unsupported

  def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = unsupported

  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = unsupported

  def stream: Stream[IO, CommittableConsumerRecord[IO, K, V]] = Stream.eval(unsupported)

  def partitionedStream: Stream[IO, Stream[IO, CommittableConsumerRecord[IO, K, V]]] =
    Stream.eval(unsupported)

  def partitionsMapStream
    : Stream[IO, Map[TopicPartition, Stream[IO, CommittableConsumerRecord[IO, K, V]]]] =
    Stream.eval(unsupported)

  def stopConsuming: IO[Unit] = unsupported

  def terminate: IO[Unit] = unsupported

  def awaitTermination: IO[Unit] = unsupported

  def metrics: IO[Map[MetricName, Metric]] = unsupported

  def committed(partitions: Set[TopicPartition]): IO[Map[TopicPartition, OffsetAndMetadata]] =
    unsupported

  def committed(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): IO[Map[TopicPartition, OffsetAndMetadata]] = unsupported

  def seek(partition: TopicPartition, offset: Long): IO[Unit] = unsupported

  def seekToBeginning[G[_]: Foldable](partitions: G[TopicPartition]): IO[Unit] = unsupported

  def seekToEnd[G[_]: Foldable](partitions: G[TopicPartition]): IO[Unit] = unsupported

  def position(partition: TopicPartition): IO[Long] = unsupported

  def position(partition: TopicPartition, timeout: FiniteDuration): IO[Long] = unsupported

  def subscribe[G[_]: Reducible](topics: G[String]): IO[Unit] = unsupported

  def subscribe(regex: Regex): IO[Unit] = unsupported

  def unsubscribe: IO[Unit] = unsupported

  def groupMetadata: IO[ConsumerGroupMetadata] = unsupported

  def partitionsFor(topic: String): IO[List[PartitionInfo]] = unsupported

  def partitionsFor(topic: String, timeout: FiniteDuration): IO[List[PartitionInfo]] = unsupported

  def beginningOffsets(partitions: Set[TopicPartition]): IO[Map[TopicPartition, Long]] = unsupported

  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): IO[Map[TopicPartition, Long]] = unsupported

  def endOffsets(partitions: Set[TopicPartition]): IO[Map[TopicPartition, Long]] = unsupported

  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): IO[Map[TopicPartition, Long]] = unsupported

  def listTopics: IO[Map[String, List[PartitionInfo]]] = unsupported

  def listTopics(timeout: FiniteDuration): IO[Map[String, List[PartitionInfo]]] = unsupported

  def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Long]
  ): IO[Map[TopicPartition, Option[OffsetAndTimestamp]]] = unsupported

  def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Long],
    timeout: FiniteDuration
  ): IO[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    unsupported

  private def unsupported[A]: IO[A] =
    IO.raiseError(new AssertionError("StubKafkaConsumer method should not be called"))

}

object StubKafkaConsumer {

  def metadataOnly[K, V](
    clientId: String = "consumer-client",
    groupId: String = "consumer-group"
  ): KafkaConsumer[IO, K, V] =
    new StubKafkaConsumer[K, V](ConsumerMetadata(Some(clientId), Some(groupId)))

}
