package fs2.kafka.internal.actor

import scala.collection.immutable.SortedSet

import org.apache.kafka.common.TopicPartition

final case class OnRebalance[F[_]](
  onAssigned: SortedSet[TopicPartition] => F[Unit],
  onRevoked:  SortedSet[TopicPartition] => F[Unit]
) {

  override def toString: String =
    "OnRebalance$" + System.identityHashCode(this)

}
