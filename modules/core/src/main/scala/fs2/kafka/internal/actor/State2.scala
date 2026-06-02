package fs2.kafka.internal.actor

import cats.effect.Async
import cats.effect.Deferred
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk
import org.apache.kafka.common.TopicPartition

object State2 {
  def empty[F[_]: Async, K, V] =
    State2[F, K, V](
      Map.empty,
      false,
      false
    )
}

case class PartitionGroupState[F[_], K, V](
  groupSemaphore: Semaphore[F],
  interrupt:      Deferred[F, Either[Throwable, Unit]],
  queue:          Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]],
  spillover:      List[Chunk[CommittableConsumerRecord[F, K, V]]]
) {
  def withSpillover(record: Chunk[CommittableConsumerRecord[F, K, V]]) =
    copy(spillover = spillover :+ record)
}

final case class State2[F[_], K, V](
  partitionGroupState: Map[Set[TopicPartition], PartitionGroupState[F, K, V]],
  subscribed:          Boolean,
  streaming:           Boolean
)(implicit
  F:                   Async[F]
) {

    def withGroupState(s: Map[Set[TopicPartition], PartitionGroupState[F, K, V]]) =
    copy(partitionGroupState = s)
  def withUnsubscribed():                        State2[F, K, V] = copy(partitionGroupState = Map.empty, subscribed = false, streaming = false)
  def withSubscribed():                          State2[F, K, V] = copy(subscribed = true)
  def withStreaming():                           State2[F, K, V] =
    copy(streaming = true)
  def withNotStreaming():                        State2[F, K, V] = copy(streaming = false)

  override def toString: String =
    s"State2(partitionGroupState = $partitionGroupState, subscribed = $subscribed, streaming = $streaming)"

}
