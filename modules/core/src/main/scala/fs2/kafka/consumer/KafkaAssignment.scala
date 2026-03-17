/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import scala.collection.immutable.SortedSet

import cats.data.NonEmptySet
import fs2.*
import fs2.kafka.instances.*

import org.apache.kafka.common.TopicPartition

trait KafkaAssignment[F[_]] {

  /**
    * Returns the set of partitions currently assigned to this consumer.
    */
  def assignment: F[SortedSet[TopicPartition]]

  /**
    * `Stream` where the elements are the set of `TopicPartition`s currently assigned to this
    * consumer. The stream emits whenever a rebalance changes partition assignments.
    */
  def assignmentStream: Stream[F, SortedSet[TopicPartition]]

  /**
    * Manually assigns the specified list of topic partitions to the consumer. This function does
    * not allow for incremental assignment and will replace the previous assignment (if there is
    * one).<br><br>
    *
    * Manual topic assignment through this method does not use the consumer's group management
    * functionality. As such, there will be no rebalance operation triggered when group membership
    * or cluster and topic metadata change. Note that it is not possible to use both manual
    * partition assignment with `assign` and group assignment with `subscribe`.<br><br>
    *
    * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered
    * before the new assignment replaces the old one.<br><br>
    *
    * To unassign all partitions, use [[KafkaConsumer#unsubscribe]].
    *
    * @see
    *   org.apache.kafka.clients.consumer.KafkaConsumer#assign
    */
  def assign(partitions: NonEmptySet[TopicPartition]): F[Unit]

  /**
    * Manually assigns the specified list of partitions for the specified topic to the consumer.
    * This function does not allow for incremental assignment and will replace the previous
    * assignment (if there is one).<br><br>
    *
    * Manual topic assignment through this method does not use the consumer's group management
    * functionality. As such, there will be no rebalance operation triggered when group membership
    * or cluster and topic metadata change. Note that it is not possible to use both manual
    * partition assignment with `assign` and group assignment with `subscribe`.<br><br>
    *
    * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered
    * before the new assignment replaces the old one.<br><br>
    *
    * To unassign all partitions, use [[KafkaConsumer#unsubscribe]].
    *
    * @see
    *   org.apache.kafka.clients.consumer.KafkaConsumer#assign
    */
  def assign(topic: String, partitions: NonEmptySet[Int]): F[Unit] =
    assign(partitions.map(new TopicPartition(topic, _)))

  /**
    * Manually assigns all partitions for the specified topic to the consumer.
    */
  def assign(topic: String): F[Unit]

}
