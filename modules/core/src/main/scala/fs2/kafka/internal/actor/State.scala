package fs2.kafka.internal.actor

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import cats.data.Chain
import cats.effect.Async
import cats.syntax.all.*
import fs2.kafka.internal.Logging
import fs2.kafka.internal.actor.PartitionState.PartitionStateMap
import fs2.kafka.internal.LogEntry.AssignedPartitions
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk
import org.apache.kafka.common.TopicPartition

object State {

  def empty[F[_]: Async, K, V]: State[F, K, V] =
    State(
      partitionState = Map.empty,
      pendingCommits = Chain.empty,
      onRebalances   = Chain.empty,
      rebalancing    = false,
      subscribed     = false,
      streaming      = false
    )

}

final case class State[F[_], K, V](
  partitionState: PartitionStateMap[F, K, V],
  pendingCommits: Chain[F[Unit]],
  onRebalances:   Chain[OnRebalance[F]],
  rebalancing:    Boolean,
  subscribed:     Boolean,
  streaming:      Boolean
)(implicit F:     Async[F]
) {

  /** State update function that updates `partitionState` to ensure it includes a state for all requested partitions.
    *
    * If no previous state exists for a given partition, the proposed `PartitionState` is added to the new state.
    * Otherwise, the existing partition state is kept.
    *
    * Use with `Ref.modify`.
    */
  def addPartitionStates(
    newPartitionState: PartitionStateMap[F, K, V]
  ): (State[F, K, V], PartitionStateMap[F, K, V]) = {
    // Own partitionState takes precedence over newPartitionState
    val newState: State[F, K, V] = copy(partitionState = newPartitionState ++ partitionState)
    (newState, newState.partitionState)
  }

  /** Updates the state based on a set of assigned partitions, received as part of a rebalance operation; concludes a
    * previous rebalance operation.
    *
    * Partition state for newly assigned partitions will be lazily initialized when records are fetched, or a new stream
    * created for the partition.
    *
    * Returns an effect with the registered `OnRebalance.onAssigned` callbacks, so that it may be invoked outside an
    * uncancelable block.
    *
    * Use with `Ref.flatModify`, and then `.flatten` to invoke registered `OnRebalance.onAssigned` callbacks.
    */
  def withAssignedPartitions(
    assigned:         SortedSet[TopicPartition]
  )(implicit logging: Logging[F]
  ): (State[F, K, V], F[F[Unit]]) = {
    val newState: State[F, K, V] = if (!rebalancing) this else copy(rebalancing = false)
    (
      newState,
      logging
        .log(AssignedPartitions(assigned, newState))
        .as(onRebalances.traverse_(_.onAssigned(assigned)))
    )
  }

  /** Updates the state based on a set of revoked partitions, received as part of a rebalance operation; initiates a
    * rebalance operation.
    *
    * Partition state is dropped for any partitions that are not part of the assignment, and their `closeSignal`
    * triggered in the returned effect.
    *
    * Returns an effect with the registered `OnRebalance.onRevoked` callbacks, so that it may be invoked outside an
    * uncancelable block.
    *
    * Use with `Ref.flatModify`, and then `.flatten` to invoke registered `OnRebalance.onRevoked` callbacks.
    */
  def withRevokedPartitions(
    sessionTimeout:   FiniteDuration,
    revoked:          SortedSet[TopicPartition]
  )(implicit logging: Logging[F]
  ): (State[F, K, V], F[F[Unit]]) = ???

  /** Updates the state based on the current set of assigned partitions.
    *
    * Partition state is dropped for any partitions that are not a part of the assignment, and their `closeSignal`
    * triggered in the returned effect.
    *
    * Use with `Ref.flatModify`.
    */
  def dropUnassignedPartitions(
    assignment:       Set[TopicPartition]
  )(implicit logging: Logging[F]
  ): (State[F, K, V], F[List[TopicPartition]]) = ???

  /** Resets partition states with a new set of spillover records after a poll operation.
    */
  def resetSpilloverAfterPoll(
    spillover: Map[TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]]]
  ): State[F, K, V] = ???

  /** Resets pending commits after a poll operation.
    *
    * Pending commits are reset only if a rebalance operation is no longer underway.
    *
    * Use with `Ref.flatModify`.
    */
  def resetPendingCommitsAfterPoll: (State[F, K, V], F[Unit]) =
    if (pendingCommits.isEmpty || rebalancing) (this, F.unit)
    else (copy(pendingCommits = Chain.empty), pendingCommits.sequence_)

  def withOnRebalance(onRebalance: OnRebalance[F]): State[F, K, V] =
    copy(onRebalances = onRebalances.append(onRebalance))

  def withPendingCommit(pendingCommit: F[Unit]): State[F, K, V] =
    copy(pendingCommits = pendingCommits.append(pendingCommit))

  def asSubscribed: State[F, K, V] =
    if (subscribed) this else copy(subscribed = true)

  def asUnsubscribed: State[F, K, V] =
    if (!subscribed) this else copy(subscribed = false)

  def asStreaming: State[F, K, V] =
    if (streaming) this else copy(streaming = true)

  override def toString: String =
    s"State(partitionState = $partitionState, pendingCommits = $pendingCommits, onRebalances = $onRebalances, rebalancing = $rebalancing, subscribed = $subscribed, streaming = $streaming)"

}
