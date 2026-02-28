/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import scala.concurrent.duration.FiniteDuration

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

}
