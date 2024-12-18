/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.time.Duration
import java.util

import scala.collection.immutable.SortedSet

import cats.data.Chain
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.instances.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.KafkaConsumerActor.*
import fs2.kafka.internal.LogEntry.*
import fs2.Chunk

import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRebalanceListener,
  OffsetAndMetadata
}
import org.apache.kafka.common.TopicPartition

/**
  * [[KafkaConsumerActor]] wraps a Java `KafkaConsumer` and works similar to a traditional actor, in
  * the sense that it receives requests one at-a-time via a queue, which are received as calls to
  * the `handle` function. `Poll` requests are scheduled at a fixed interval and, when handled,
  * calls the `KafkaConsumer#poll` function, allowing the Java consumer to perform necessary
  * background functions, and to return fetched records.<br><br>
  *
  * The actor receives `Fetch` requests for topic-partitions for which there is demand. The actor
  * then attempts to fetch records for topic-partitions where there is a `Fetch` request. For
  * topic-partitions where there is no request, no attempt to fetch records is made. This
  * effectively enables backpressure, as long as `Fetch` requests are only issued when there is more
  * demand.
  */
final private[kafka] class KafkaConsumerActor[F[_], K, V](
  settings: ConsumerSettings[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  val ref: Ref[F, State[F, K, V]],
  requests: Queue[F, Request[F, K, V]],
  withConsumer: WithConsumer[F]
)(implicit
  F: Async[F],
  dispatcher: Dispatcher[F],
  logging: Logging[F],
  jitter: Jitter[F]
) {

  private[this] type ConsumerRecords =
    Map[TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]]]

  private[this] type ConsumerQueue = Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]]

  private[this] val consumerGroupId: Option[String] =
    settings.properties.get(ConsumerConfig.GROUP_ID_CONFIG)

  val consumerRebalanceListener: ConsumerRebalanceListener =
    new ConsumerRebalanceListener {

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
        dispatcher.unsafeRunSync(revoked(partitions.toSortedSet))

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
        dispatcher.unsafeRunSync(assigned(partitions.toSortedSet))

    }

  private[this] def commitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    callback: Either[Throwable, Unit] => Unit
  ): F[Unit] =
    withConsumer
      .blocking {
        _.commitAsync(
          offsets.asJava,
          (_, exception) => callback(Option(exception).toLeft(()))
        )
      }
      .handleErrorWith(e => F.delay(callback(Left(e))))

  private[this] def commit(request: Request.Commit[F]): F[Unit] =
    ref.flatModify { state =>
      val commitF = commitAsync(request.offsets, request.callback)
      if (state.rebalancing) {
        val newState =
          state.withPendingCommit(commitF >> logging.log(CommittedPendingCommit(request)))
        (newState, logging.log(StoredPendingCommit(request, newState)))
      } else
        (state, commitF)
    }

  private[this] def manualCommitSync(request: Request.ManualCommitSync[F]): F[Unit] = {
    val commit =
      withConsumer.blocking(_.commitSync(request.offsets.asJava, settings.commitTimeout.toJava))
    commit.attempt >>= request.callback
  }

  private[this] def runCommitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  )(
    k: (Either[Throwable, Unit] => Unit) => F[Unit]
  ): F[Unit] =
    F.async[Unit] { (cb: Either[Throwable, Unit] => Unit) =>
        k(cb).as(Some(F.unit))
      }
      .timeoutTo(
        settings.commitTimeout,
        F.defer(F.raiseError[Unit] {
          CommitTimeoutException(
            settings.commitTimeout,
            offsets
          )
        })
      )

  private[this] def assigned(assigned: SortedSet[TopicPartition]): F[Unit] =
    ref.flatModify(_.withAssignedPartitions(assigned)).flatten

  private[this] def revoked(revoked: SortedSet[TopicPartition]): F[Unit] =
    ref.flatModify(_.withRevokedPartitions(revoked)).flatten

  def offsetCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    runCommitAsync(offsets)(cb => requests.offer(Request.Commit(offsets, cb)))

  private[this] def resilientOffsetCommitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit] =
    offsetCommitAsync(offsets).handleErrorWith {
      settings.commitRecovery.recoverCommitWith(offsets, offsetCommitAsync(offsets))
    }

  private[this] def committableConsumerRecord(
    record: ConsumerRecord[K, V],
    partition: TopicPartition
  ): CommittableConsumerRecord[F, K, V] =
    CommittableConsumerRecord(
      record = record,
      offset = CommittableOffset(
        topicPartition = partition,
        consumerGroupId = consumerGroupId,
        offsetAndMetadata = new OffsetAndMetadata(
          record.offset + 1L,
          settings.recordMetadata(record)
        ),
        commit = resilientOffsetCommitAsync
      )
    )

  private[this] def records(batch: KafkaByteConsumerRecords): F[ConsumerRecords] =
    batch
      .partitions
      .toVector
      .traverse { partition =>
        batch
          .records(partition)
          .toVector
          .traverse { record =>
            ConsumerRecord
              .fromJava(record, keyDeserializer, valueDeserializer)
              .map(committableConsumerRecord(_, partition))
          }
          .map(records => (partition, Chunk.from(records)))
      }
      .map(_.toMap)

  private[this] val pollTimeout: Duration =
    settings.pollTimeout.toJava

  private[this] val poll: F[Unit] = {

    // Checks whether to poll Kafka, at all, and drops state and records for
    // unassigned partitions.
    //
    // Returns a 3-tuple:
    //  1. whether to poll, at all;
    //  2. the current partition assignment;
    //  3. partitions to pause in pollForRecords.
    val updateStateBeforePoll =
      withConsumer
        .blocking(_.assignment.toSet)
        .flatMap { assignment =>
          ref.flatModify {
            case state if state.subscribed && state.streaming =>
              val isStreaming        = true
              val (newState, result) = state.dropUnassignedPartitions(assignment)
              (newState, result.map((isStreaming, assignment, _)))

            case state =>
              val isStreaming = false
              (state, F.pure((isStreaming, assignment, List.empty[TopicPartition])))
          }
        }

    // Polls for records, and deserialize.
    val pollForRecords = { (assignment: Set[TopicPartition], queueIsFull: List[TopicPartition]) =>
      withConsumer
        .blocking { consumer =>
          val resumePartitions = assignment -- queueIsFull

          if (queueIsFull.nonEmpty)
            consumer.pause(queueIsFull.asJava)

          if (resumePartitions.nonEmpty)
            consumer.resume(resumePartitions.asJava)

          consumer.poll(pollTimeout)
        }
        .flatMap(records)
    }

    // Attempts to enqueue existing spillover records and newly fetched records. Returns updated spillover.
    val queueRecords = {
      (partitionState: PartitionStateMap[F, K, V], newRecords: ConsumerRecords) =>
        require(newRecords.forall(kv => partitionState.contains(kv._1)))

        partitionState
          .toList
          .flatTraverse { case (partition, PartitionState(queue, spillover, _)) =>
            val chunk = spillover ++ newRecords.getOrElse(partition, Chunk.empty)

            F.pure(chunk.isEmpty)
              .ifM[List[(TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]])]](
                F.pure(Nil),
                queue.tryOffer(chunk).ifF(Nil, List(partition -> chunk))
              )
          }
    }

    // Resets spillover, resets pending commits.
    val updateStateAfterPoll = { (spillover: ConsumerRecords) =>
      ref.flatModify(_.resetSpilloverAfterPoll(spillover).resetPendingCommitsAfterPoll)
    }

    updateStateBeforePoll.flatMap {
      case (isStreaming @ true, assignment, queueIsFull) =>
        for {
          newRecords     <- pollForRecords(assignment, queueIsFull) // Poll may change assignment
          partitionState <- ensurePartitionStateFor(newRecords.keySet)
          spillover      <- queueRecords(partitionState, newRecords)
          _              <- updateStateAfterPoll(spillover.toMap)
        } yield ()

      case _ =>
        // Not streaming
        F.unit
    }
  }

  /**
    * Ensures the state holds a PartitionState for each requested partition, and returns the map of
    * all partition states (including at least the requested partitions).
    *
    * At the cost of additional synchronization on the internal state `ref`, this method
    * optimistically assumes there is already an entry for the requested partition. The intent is to
    * avoid over-provisioning `PartitionState` instances. New instances are then created as
    * necessary, and atomically added to the state.
    *
    * This may still create and discard duplicate `PartitionState` instances. Any previously added
    * to the state will be returned and not overwritten.
    *
    * Called by `poll`, and `getQueueAndStopSignalFor` (for `KafkaConsumer`).
    */
  private[this] def ensurePartitionStateFor(
    partitions: Set[TopicPartition]
  ): F[PartitionStateMap[F, K, V]] =
    ref
      .get
      .flatMap { state =>
        (partitions -- state.partitionState.keys).toList match {
          case Nil => F.pure(state.partitionState)
          case missing =>
            missing
              .traverse { partition =>
                (
                  Queue.bounded[F, Chunk[CommittableConsumerRecord[F, K, V]]](
                    settings.maxPrefetchBatches
                  ),
                  F.pure(Chunk.empty[CommittableConsumerRecord[F, K, V]]),
                  F.deferred[Unit]
                ).parMapN(partition -> PartitionState(_, _, _))
              }
              .flatMap { newPartitionState =>
                val newPartitionStateMap = newPartitionState.toMap
                ref.modify(_.addPartitionStates(newPartitionStateMap))
              }
        }
      }

  def getQueueAndStopSignalFor(partition: TopicPartition): F[(ConsumerQueue, F[Unit])] =
    ensurePartitionStateFor(Set(partition)).flatMap { partitionState =>
      partitionState.get(partition) match {
        case Some(ps) => F.pure((ps.queue, ps.closeSignal.get))
        case None =>
          F.raiseError(new IllegalStateException(s"PartitionState not added for $partition"))
      }
    }

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Poll()                           => poll
      case request @ Request.Commit(_, _)           => commit(request)
      case request @ Request.ManualCommitSync(_, _) => manualCommitSync(request)
      case Request.WithPermit(fa, cb)               => fa.attempt >>= cb
    }

}

private[kafka] object KafkaConsumerActor {

  final case class PartitionState[F[_]: Async, K, V](
    queue: Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]],
    spillover: Chunk[CommittableConsumerRecord[F, K, V]],
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

  private type PartitionStateMap[F[_], K, V] = Map[TopicPartition, PartitionState[F, K, V]]

  final case class State[F[_], K, V](
    partitionState: PartitionStateMap[F, K, V],
    pendingCommits: Chain[F[Unit]],
    onRebalances: Chain[OnRebalance[F]],
    rebalancing: Boolean,
    subscribed: Boolean,
    streaming: Boolean
  )(implicit F: Async[F]) {

    /**
      * State update function that updates `partitionState` to ensure it includes a state for all
      * requested partitions.
      *
      * If no previous state exists for a given partition, the proposed `PartitionState` is added to
      * the new state. Otherwise, the existing partition state is kept.
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

    /**
      * Updates the state based on a set of assigned partitions, received as part of a rebalance
      * operation; concludes a previous rebalance operation.
      *
      * Partition state for newly assigned partitions will be lazily initialized when records are
      * fetched, or a new stream created for the partition.
      *
      * Returns an effect with the registered `OnRebalance.onAssigned` callbacks, so that it may be
      * invoked outside an uncancelable block.
      *
      * Use with `Ref.flatModify`, and then `.flatten` to invoke registered `OnRebalance.onAssigned`
      * callbacks.
      */
    def withAssignedPartitions(
      assigned: SortedSet[TopicPartition]
    )(implicit logging: Logging[F]): (State[F, K, V], F[F[Unit]]) = {
      val newState: State[F, K, V] = if (!rebalancing) this else copy(rebalancing = false)

      (
        newState,
        logging
          .log(AssignedPartitions(assigned, newState))
          .as(onRebalances.traverse_(_.onAssigned(assigned)))
      )
    }

    /**
      * Updates the state based on a set of revoked partitions, received as part of a rebalance
      * operation; initiates a rebalance operation.
      *
      * Partition state is dropped for any partitions that are not part of the assignment, and their
      * `closeSignal` triggered in the returned effect.
      *
      * Returns an effect with the registered `OnRebalance.onRevoked` callbacks, so that it may be
      * invoked outside an uncancelable block.
      *
      * Use with `Ref.flatModify`, and then `.flatten` to invoke registered `OnRebalance.onRevoked`
      * callbacks.
      */
    def withRevokedPartitions(
      revoked: SortedSet[TopicPartition]
    )(implicit logging: Logging[F]): (State[F, K, V], F[F[Unit]]) = {
      val (revokedToClose, stillAssigned) = partitionState.partition(e => revoked.contains(e._1))

      val newState: State[F, K, V] = copy(partitionState = stillAssigned, rebalancing = true)

      (
        newState,
        for {
          _ <- logging.log(RevokedPartitions(revoked, revokedToClose, newState))
          _ <- revokedToClose.values.toList.traverse_(_.close)
        } yield onRebalances.traverse_(_.onRevoked(revoked))
      )
    }

    /**
      * Updates the state based on the current set of assigned partitions.
      *
      * Partition state is dropped for any partitions that are not a part of the assignment, and
      * their `closeSignal` triggered in the returned effect.
      *
      * Use with `Ref.flatModify`.
      */
    def dropUnassignedPartitions(
      assignment: Set[TopicPartition]
    )(implicit logging: Logging[F]): (State[F, K, V], F[List[TopicPartition]]) = {
      val (assigned, revoked) = partitionState.partition(e => assignment.contains(e._1))

      val newState: State[F, K, V] = copy(partitionState = assigned)
      val queueIsFull              = assigned.filter(_._2.isQueueFull).keys.toList

      (
        newState,
        (for {
          _ <- revoked.values.toList.traverse_(_.close)
          _ <- logging.log(RevokedPartitions(revoked.keySet, revoked, newState))
        } yield ()).whenA(revoked.nonEmpty).as(queueIsFull)
      )
    }

    /**
      * Resets partition states with a new set of spillover records after a poll operation.
      */
    def resetSpilloverAfterPoll(
      spillover: Map[TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]]]
    ): State[F, K, V] =
      if (spillover.isEmpty)
        this
      else {
        require(spillover.forall(kv => partitionState.contains(kv._1)))

        val newPartitionState: PartitionStateMap[F, K, V] = partitionState.map {
          case (partition, partitionState) =>
            (
              partition,
              spillover
                .get(partition)
                .map(spillover => partitionState.copy(spillover = spillover))
                .getOrElse(
                  if (partitionState.spillover.isEmpty)
                    partitionState
                  else
                    partitionState.copy(spillover = Chunk.empty)
                )
            )
        }

        copy(partitionState = newPartitionState)
      }

    /**
      * Resets pending commits after a poll operation.
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

  object State {

    def empty[F[_]: Async, K, V]: State[F, K, V] =
      State(
        partitionState = Map.empty,
        pendingCommits = Chain.empty,
        onRebalances = Chain.empty,
        rebalancing = false,
        subscribed = false,
        streaming = false
      )

  }

  final case class OnRebalance[F[_]](
    onAssigned: SortedSet[TopicPartition] => F[Unit],
    onRevoked: SortedSet[TopicPartition] => F[Unit]
  ) {

    override def toString: String =
      "OnRebalance$" + System.identityHashCode(this)

  }

  sealed abstract class Request[F[_], -K, -V]

  object Request {

    final case class WithPermit[F[_], A](fa: F[A], callback: Either[Throwable, A] => F[Unit])
        extends Request[F, Any, Any]

    final case class Poll[F[_]]() extends Request[F, Any, Any]

    private[this] val pollInstance: Poll[Nothing] =
      Poll[Nothing]()

    def poll[F[_]]: Poll[F] =
      pollInstance.asInstanceOf[Poll[F]]

    final case class Commit[F[_]](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: Either[Throwable, Unit] => Unit
    ) extends Request[F, Any, Any]

    final case class ManualCommitSync[F[_]](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: Either[Throwable, Unit] => F[Unit]
    ) extends Request[F, Any, Any]

  }

}
