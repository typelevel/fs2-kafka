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

import org.apache.kafka.common.TopicPartition

private[kafka] object State {

  def empty[F[_]: Async, K, V] =
    State[F, K, V](
      Map.empty,
      false,
      false
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

}

final private[kafka] case class State[F[_], K, V](
  partitionGroupState: Map[Set[TopicPartition], PartitionGroupState[F, K, V]],
  subscribed: Boolean,
  streaming: Boolean
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

  override def toString: String =
    s"State(partitionGroupState = $partitionGroupState, subscribed = $subscribed, streaming = $streaming)"

}
