package fs2.kafka.internal.actor

import cats.effect.{Async, Deferred}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk
import org.apache.kafka.common.TopicPartition

final case class PartitionState[F[_]: Async, K, V](
  queue: Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]],
  spillover: List[Chunk[CommittableConsumerRecord[F, K, V]]],
  closeSignal: Deferred[F, Unit]
) {

  def isQueueFull: Boolean = spillover.nonEmpty

  def close: F[Unit] = closeSignal.complete(()).void

  override def toString: String =
    spillover.head match {
      case None         => "()"
      case Some(record) => s"(offset = ${record.offset}, size = ${spillover.size})"
    }

}

object PartitionState {
  type PartitionStateMap[F[_], K, V] = Map[Set[TopicPartition], PartitionState[F, K,V]]
}
