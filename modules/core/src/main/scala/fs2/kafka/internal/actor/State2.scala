package fs2.kafka.internal.actor

import cats.data.Chain
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk
import org.apache.kafka.common.TopicPartition

object State2 {
  def empty[F[_]: Async] =
    State2(
      Map.empty,
      Chain.empty,
      false,
      false,
      false
    )
}

case class PartitionGroupState[F[_], K, V](
  groupSemaphore: Semaphore[F],
  interrupt:      Deferred[F, Either[Throwable, Unit]],
  queue:          Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]],
  spillover:      List[Chunk[CommittableConsumerRecord[F, K, V]]],
)

final case class State2[F[_], K, V](
  partitionGroupState: Map[Set[TopicPartition], PartitionGroupState[F, K, V]],
  pendingCommits:      Chain[F[Unit]],
  rebalancing:         Boolean,
  subscribed:          Boolean,
  streaming:           Boolean
) {

  override def toString: String = ???

}
