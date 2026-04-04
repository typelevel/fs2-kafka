/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import scala.concurrent.duration.FiniteDuration

import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition

trait KafkaTopics[F[_]] {

  /**
    * Returns the partitions for the specified topic.
    *
    * Timeout is determined by `default.api.timeout.ms`, which is set using
    * [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def partitionsFor(topic: String): F[List[PartitionInfo]]

  /**
    * Returns the partitions for the specified topic.
    */
  def partitionsFor(topic: String, timeout: FiniteDuration): F[List[PartitionInfo]]

  /**
    * Returns the first offset for the specified partitions.<br><br>
    *
    * Timeout is determined by `default.api.timeout.ms`, which is set using
    * [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def beginningOffsets(partitions: Set[TopicPartition]): F[Map[TopicPartition, Long]]

  /**
    * Returns the first offset for the specified partitions.
    */
  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]

  /**
    * Returns the last offset for the specified partitions.<br><br>
    *
    * Timeout is determined by `request.timeout.ms`, which is set using
    * [[ConsumerSettings#withRequestTimeout]].
    */
  def endOffsets(partitions: Set[TopicPartition]): F[Map[TopicPartition, Long]]

  /**
    * Returns the last offset for the specified partitions.
    */
  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]

  /**
    * Get metadata about partitions for all topics that the user is authorized to view. This method
    * will issue a remote call to the server.<br><br>
    *
    * Timeout is determined by `default.api.timeout.ms`, which is set using
    * [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def listTopics: F[Map[String, List[PartitionInfo]]]

  /**
    * Get metadata about partitions for all topics that the user is authorized to view. This method
    * will issue a remote call to the server.<br><br>
    */
  def listTopics(timeout: FiniteDuration): F[Map[String, List[PartitionInfo]]]

  /**
    * Look up the offsets for the given partitions by timestamp. The returned offset for each
    * partition is the earliest offset whose timestamp is greater than or equal to the given
    * timestamp in the corresponding partition.<br><br>
    *
    * The consumer does not have to be assigned the partitions. If no messages exist yet for a
    * partition, it will not exist in the returned map.<br><br>
    *
    * Timeout is determined by `default.api.timeout.ms`, which is set using
    * [[ConsumerSettings#withDefaultApiTimeout]].
    */
  def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  /**
    * Look up the offsets for the given partitions by timestamp. The returned offset for each
    * partition is the earliest offset whose timestamp is greater than or equal to the given
    * timestamp in the corresponding partition.
    *
    * The consumer does not have to be assigned the partitions. If no messages exist yet for a
    * partition, it will not exist in the returned map.
    */
  def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Long],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]]

}
