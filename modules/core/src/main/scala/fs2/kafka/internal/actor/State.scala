/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.effect.Async
import cats.effect.Deferred
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk

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
    copy(partitionGroupState = Map.empty, subscribed = false, streaming = false)

  def withSubscribed(): State[F, K, V] = copy(subscribed = true)

  def withStreaming(): State[F, K, V] =
    copy(streaming = true)

  def withNotStreaming(): State[F, K, V] = copy(streaming = false)

  /**
    * Remembers the latest requested commit offset per partition, so that — if those partitions are
    * revoked before the asynchronous commit is acknowledged — they can be committed synchronously
    * from within the rebalance listener. Keeps the highest offset seen per partition.
    */
  def withRequestedCommitOffsets(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): State[F, K, V] =
    copy(requestedCommitOffsets = offsets.foldLeft(requestedCommitOffsets) {
      case (acc, (partition, offsetAndMetadata)) =>
        val isNewer = acc.get(partition).forall(_.offset < offsetAndMetadata.offset)
        if (isNewer) acc.updated(partition, offsetAndMetadata) else acc
    })

  /**
    * Removes and returns the tracked commit offsets for the given (revoked) partitions.
    */
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
