/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.effect.Async
import cats.effect.Deferred
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka] object State {

  def empty[F[_]: Async, K, V] =
    State[F, K, V](
      Map.empty,
      false,
      false,
      Map.empty
    )

}

final private[kafka] case class PartitionGroupState[F[_], K, V](
  groupSemaphore: Semaphore[F],
  interrupt: Deferred[F, Either[Throwable, Unit]],
  queue: Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]],
  spillover: List[Chunk[CommittableConsumerRecord[F, K, V]]]
) {

  def withSpillover(record: Chunk[CommittableConsumerRecord[F, K, V]]) =
    copy(spillover = spillover :+ record)

  def setSpillover(newSpillover: List[Chunk[CommittableConsumerRecord[F, K, V]]]) =
    copy(spillover = newSpillover)

  def clearSpillover(): PartitionGroupState[F, K, V] = copy(spillover = Nil)

}

final private[kafka] case class State[F[_], K, V](
  partitionGroupState: Map[Set[TopicPartition], PartitionGroupState[F, K, V]],
  subscribed: Boolean,
  streaming: Boolean,
  requestedCommitOffsets: Map[TopicPartition, OffsetAndMetadata]
)(implicit
  F: Async[F]
) {

  def withGroupState(s: Map[Set[TopicPartition], PartitionGroupState[F, K, V]]) =
    copy(partitionGroupState = s)

  def withUnsubscribed(): State[F, K, V] =
    copy(
      partitionGroupState = Map.empty,
      subscribed = false,
      streaming = false,
      requestedCommitOffsets = Map.empty
    )

  def withSubscribed(): State[F, K, V] = copy(subscribed = true)

  def withStreaming(): State[F, K, V] =
    copy(streaming = true)

  def assignedPartitions: Set[TopicPartition] =
    partitionGroupState.keySet.flatten

  def withRequestedCommitOffsets(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): State[F, K, V] = {
    val assigned = assignedPartitions
    copy(requestedCommitOffsets = offsets.foldLeft(requestedCommitOffsets) {
      case (acc, (partition, offsetAndMetadata)) =>
        val isNewer = acc.get(partition).forall(_.offset < offsetAndMetadata.offset)
        if (assigned.contains(partition) && isNewer) acc.updated(partition, offsetAndMetadata)
        else acc
    })
  }

  def withoutRequestedCommitOffsets(partitions: Set[TopicPartition]): State[F, K, V] =
    copy(requestedCommitOffsets = requestedCommitOffsets -- partitions)

  def removeRequestedCommitOffsets(
    revoked: Set[TopicPartition]
  ): (State[F, K, V], Map[TopicPartition, OffsetAndMetadata]) = {
    val (toCommit, retained) =
      requestedCommitOffsets.partition { case (partition, _) => revoked.contains(partition) }
    (copy(requestedCommitOffsets = retained), toCommit)
  }

  override def toString: String =
    s"State(partitionGroupState = $partitionGroupState, subscribed = $subscribed, streaming = $streaming)"

}
