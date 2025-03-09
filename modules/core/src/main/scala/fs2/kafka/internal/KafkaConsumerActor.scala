/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.time.Duration
import java.util

import scala.collection.immutable.SortedSet

import cats.data.{Chain, NonEmptyVector, StateT}
import cats.effect.*
import cats.effect.std.*
import cats.effect.syntax.all.*
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
    Map[TopicPartition, NonEmptyVector[CommittableConsumerRecord[F, K, V]]]

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
      if (state.rebalancing) {
        val newState = state.withPendingCommit(request)
        (newState, logging.log(StoredPendingCommit(request, newState)))
      } else
        (state, commitAsync(request.offsets, request.callback))
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

  private[this] def manualCommitAsync(request: Request.ManualCommitAsync[F]): F[Unit] = {
    val commit = runCommitAsync(request.offsets) { cb =>
      commitAsync(request.offsets, cb)
    }

    val res = commit.attempt >>= request.callback

    // We need to start this action in a separate fiber without waiting for the result,
    // because commitAsync could be resolved only with the poll consumer call.
    // Which could be done only when the current request is processed.
    res.start.void
  }

  private[this] def assigned(assigned: SortedSet[TopicPartition]): F[Unit] =
    ref
      .flatModify { state =>
        val newState = state.withRebalancing(false)
        (
          newState,
          logging.log(AssignedPartitions(assigned, state)).as(newState.onRebalances)
        )
      }
      .flatMap(_.traverse_(_.onAssigned(assigned)))

  private[this] def revoked(revoked: SortedSet[TopicPartition]): F[Unit] = {
    def withState[A] = StateT.apply[Id, State[F, K, V], A](_)

    def completeWithRecords(withRecords: Set[TopicPartition]) = withState { st =>
      if (withRecords.nonEmpty) {
        val newState = st.withoutFetchesAndRecords(withRecords)

        val action = st
          .fetches
          .filterKeysStrictList(withRecords)
          .traverse { case (partition, partitionFetches) =>
            val records = Chunk.from(st.records(partition).toVector)
            partitionFetches.values.toList.traverse(_.completeRevoked(records))
          } >> logging.log(
          RevokedFetchesWithRecords(st.records.filterKeysStrict(withRecords), newState)
        )

        (newState, action)
      } else (st, F.unit)
    }

    def completeWithoutRecords(withoutRecords: SortedSet[TopicPartition]) = withState { st =>
      if (withoutRecords.nonEmpty) {
        val newState = st.withoutFetches(withoutRecords)

        val action = st
          .fetches
          .filterKeysStrictValuesList(withoutRecords)
          .traverse(_.values.toList.traverse(_.completeRevoked(Chunk.empty))) >>
          logging.log(RevokedFetchesWithoutRecords(withoutRecords, newState))

        (newState, action)
      } else (st, F.unit)
    }

    def removeRevokedRecords(revokedNonFetches: SortedSet[TopicPartition]) = withState { st =>
      if (revokedNonFetches.nonEmpty) {
        val revokedRecords = st.records.filterKeysStrict(revokedNonFetches)

        if (revokedRecords.nonEmpty) {
          val newState = st.withoutRecords(revokedRecords.keySet)

          val action = logging.log(RemovedRevokedRecords(revokedRecords, newState))

          (newState, action)
        } else (st, F.unit)
      } else (st, F.unit)
    }

    ref
      .flatModify { state =>
        val withRebalancing = state.withRebalancing(true)

        val fetches = withRebalancing.fetches.keySetStrict
        val records = withRebalancing.records.keySetStrict

        val revokedFetches    = revoked.intersect(fetches)
        val revokedNonFetches = revoked.diff(revokedFetches)

        val withRecords    = records.intersect(revokedFetches)
        val withoutRecords = revokedFetches.diff(records)

        (for {
          completeWithRecords    <- completeWithRecords(withRecords)
          completeWithoutRecords <- completeWithoutRecords(withoutRecords)
          removeRevokedRecords   <- removeRevokedRecords(revokedNonFetches)
        } yield for {
          _ <- logging.log(RevokedPartitions(revoked, withRebalancing))
          _ <- completeWithRecords
          _ <- completeWithoutRecords
          _ <- removeRevokedRecords
        } yield withRebalancing.onRebalances).run(withRebalancing)
      }
      .flatMap(_.traverse_(_.onRevoked(revoked)))
  }

  private[this] val offsetCommit: Map[TopicPartition, OffsetAndMetadata] => F[Unit] =
    offsets => {
      val commit = runCommitAsync(offsets) { cb =>
        requests.offer(Request.Commit(offsets, cb))
      }

      commit.handleErrorWith {
        settings.commitRecovery.recoverCommitWith(offsets, commit)
      }
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
        commit = offsetCommit
      )
    )

  private[this] def records(batch: KafkaByteConsumerRecords): F[ConsumerRecords] =
    batch
      .partitions
      .toVector
      .traverse { partition =>
        NonEmptyVector
          .fromVectorUnsafe(batch.records(partition).toVector)
          .traverse { record =>
            ConsumerRecord
              .fromJava(record, keyDeserializer, valueDeserializer)
              .map(committableConsumerRecord(_, partition))
          }
          .map((partition, _))
      }
      .map(_.toMap)

  private[this] val pollTimeout: Duration =
    settings.pollTimeout.toJava

  private[this] val poll: F[Unit] = {
    def pollConsumer(state: State[F, K, V]): F[ConsumerRecords] =
      withConsumer
        .blocking { consumer =>
          val assigned  = consumer.assignment.toSet
          val requested = state.fetches.keySetStrict
          val available = state.records.keySetStrict

          val resume = requested.intersect(assigned).diff(available)
          val pause  = assigned.diff(resume)

          if (pause.nonEmpty)
            consumer.pause(pause.asJava)

          if (resume.nonEmpty)
            consumer.resume(resume.asJava)

          consumer.poll(pollTimeout)
        }
        .flatMap(records)

    def handlePoll(newRecords: ConsumerRecords, initialRebalancing: Boolean): F[Unit] = {
      def handleBatch(
        state: State[F, K, V],
        pendingCommits: Option[HandlePollResult.PendingCommits]
      ) =
        if (state.fetches.isEmpty) {
          if (newRecords.isEmpty) {
            (state, HandlePollResult.StateNotChanged(pendingCommits))
          } else {
            val newState = state.withRecords(newRecords)
            (newState, HandlePollResult.Stored(StoredRecords(newRecords, newState), pendingCommits))
          }
        } else {
          val allRecords = state.records.combine(newRecords)

          if (allRecords.nonEmpty) {
            val requested = state.fetches.keySetStrict

            val canBeCompleted = allRecords.keySetStrict.intersect(requested)
            val canBeStored    = newRecords.keySetStrict.diff(canBeCompleted)

            def completeFetches: F[Unit] =
              state
                .fetches
                .filterKeysStrictList(canBeCompleted)
                .traverse_ { case (partition, fetches) =>
                  val records = Chunk.from(allRecords(partition).toVector)
                  fetches.values.toList.traverse_(_.completeRecords(records))
                }

            (canBeCompleted.nonEmpty, canBeStored.nonEmpty) match {
              case (true, true) =>
                val storeRecords = newRecords.filterKeysStrict(canBeStored)
                val newState =
                  state.withoutFetchesAndRecords(canBeCompleted).withRecords(storeRecords)
                (
                  newState,
                  HandlePollResult.CompletedAndStored(
                    completeFetches = completeFetches,
                    completedLog = CompletedFetchesWithRecords(
                      allRecords.filterKeysStrict(canBeCompleted),
                      newState
                    ),
                    storedLog = StoredRecords(storeRecords, newState),
                    pendingCommits = pendingCommits
                  )
                )

              case (true, false) =>
                val newState = state.withoutFetchesAndRecords(canBeCompleted)
                (
                  newState,
                  HandlePollResult.Completed(
                    completeFetches = completeFetches,
                    log = CompletedFetchesWithRecords(
                      allRecords.filterKeysStrict(canBeCompleted),
                      newState
                    ),
                    pendingCommits = pendingCommits
                  )
                )

              case (false, true) =>
                val storeRecords = newRecords.filterKeysStrict(canBeStored)
                val newState     = state.withRecords(storeRecords)
                (
                  newState,
                  HandlePollResult.Stored(StoredRecords(storeRecords, newState), pendingCommits)
                )

              case (false, false) =>
                (state, HandlePollResult.StateNotChanged(pendingCommits))
            }
          } else {
            (state, HandlePollResult.StateNotChanged(pendingCommits))
          }
        }

      def handlePendingCommits(state: State[F, K, V]) = {
        val currentRebalancing = state.rebalancing

        if (initialRebalancing && !currentRebalancing && state.pendingCommits.nonEmpty) {
          val newState = state.withoutPendingCommits
          (
            newState,
            Some(
              HandlePollResult.PendingCommits(
                commits = state.pendingCommits,
                log = CommittedPendingCommits(state.pendingCommits, newState)
              )
            )
          )
        } else (state, None)
      }

      ref
        .modify { state =>
          val (stateWithoutPendingCommits, pendingCommits) = handlePendingCommits(state)
          handleBatch(stateWithoutPendingCommits, pendingCommits)
        }
        .flatMap { result =>
          (result match {
            case HandlePollResult.StateNotChanged(_) =>
              F.unit
            case HandlePollResult.Stored(log, _) =>
              logging.log(log)
            case HandlePollResult.Completed(completeFetches, log, _) =>
              completeFetches >> logging.log(log)
            case HandlePollResult.CompletedAndStored(completeFetches, completedLog, storedLog, _) =>
              completeFetches >> logging.log(completedLog) >> logging.log(storedLog)
          }) >> result.pendingCommits.traverse_(_.commit)
        }
    }

    ref
      .get
      .flatMap { state =>
        F.uncancelable { poll =>
            val initialRebalancing = state.rebalancing
            for {
              records <- poll(pollConsumer(state))
              _       <- handlePoll(records, initialRebalancing)
            } yield ()
          }
          .whenA(state.subscribed && state.streaming)
      }
  }

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Poll()                            => poll
      case request @ Request.Commit(_, _)            => commit(request)
      case request @ Request.ManualCommitAsync(_, _) => manualCommitAsync(request)
      case request @ Request.ManualCommitSync(_, _)  => manualCommitSync(request)
      case Request.WithPermit(fa, cb)                => fa.attempt >>= cb
    }

  sealed private[this] trait HandlePollResult {
    def pendingCommits: Option[HandlePollResult.PendingCommits]
  }

  private[this] object HandlePollResult {

    case class PendingCommits(
      commits: Chain[Request.Commit[F]],
      log: CommittedPendingCommits[F]
    ) {

      def commit: F[Unit] =
        commits.traverse { commitRequest =>
          commitAsync(commitRequest.offsets, commitRequest.callback)
        } >> logging.log(log)

    }

    case class StateNotChanged(pendingCommits: Option[PendingCommits]) extends HandlePollResult

    case class Stored(
      log: LogEntry.StoredRecords[F],
      pendingCommits: Option[PendingCommits]
    ) extends HandlePollResult

    case class Completed(
      completeFetches: F[Unit],
      log: LogEntry.CompletedFetchesWithRecords[F],
      pendingCommits: Option[PendingCommits]
    ) extends HandlePollResult

    case class CompletedAndStored(
      completeFetches: F[Unit],
      completedLog: LogEntry.CompletedFetchesWithRecords[F],
      storedLog: LogEntry.StoredRecords[F],
      pendingCommits: Option[PendingCommits]
    ) extends HandlePollResult

  }

}

private[kafka] object KafkaConsumerActor {

  final case class FetchRequest[F[_], K, V](
    callback: ((Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)) => F[Unit]
  ) {

    def completeRevoked(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] =
      callback((chunk, FetchCompletedReason.TopicPartitionRevoked))

    def completeRecords(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] =
      callback((chunk, FetchCompletedReason.FetchedRecords))

    override def toString: String =
      "FetchRequest$" + System.identityHashCode(this)

  }

  type StreamId = Int

  final case class State[F[_], K, V](
    fetches: Map[TopicPartition, Map[StreamId, FetchRequest[F, K, V]]],
    records: Map[TopicPartition, NonEmptyVector[CommittableConsumerRecord[F, K, V]]],
    pendingCommits: Chain[Request.Commit[F]],
    onRebalances: Chain[OnRebalance[F]],
    rebalancing: Boolean,
    subscribed: Boolean,
    streaming: Boolean
  ) {

    def withOnRebalance(onRebalance: OnRebalance[F]): State[F, K, V] =
      copy(onRebalances = onRebalances.append(onRebalance))

    /**
      * @return
      *   (new-state, old-fetches-to-revoke)
      */
    def withFetch(
      partition: TopicPartition,
      streamId: StreamId,
      callback: ((Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)) => F[Unit]
    ): (State[F, K, V], List[FetchRequest[F, K, V]]) = {
      val newFetchRequest =
        FetchRequest(callback)

      val oldPartitionFetches: Map[StreamId, FetchRequest[F, K, V]] =
        fetches.getOrElse(partition, Map.empty)

      val newFetches: Map[TopicPartition, Map[StreamId, FetchRequest[F, K, V]]] =
        fetches.updated(partition, oldPartitionFetches.updated(streamId, newFetchRequest))

      val fetchesToRevoke: List[FetchRequest[F, K, V]] =
        oldPartitionFetches.get(streamId).toList

      (
        copy(fetches = newFetches),
        fetchesToRevoke
      )
    }

    def withoutFetches(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(
        fetches = fetches.filterKeysStrict(!partitions.contains(_))
      )

    def withRecords(
      records: Map[TopicPartition, NonEmptyVector[CommittableConsumerRecord[F, K, V]]]
    ): State[F, K, V] =
      copy(records = this.records.combine(records))

    def withoutFetchesAndRecords(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(
        fetches = fetches.filterKeysStrict(!partitions.contains(_)),
        records = records.filterKeysStrict(!partitions.contains(_))
      )

    def withoutRecords(partitions: Set[TopicPartition]): State[F, K, V] =
      copy(records = records.filterKeysStrict(!partitions.contains(_)))

    def withPendingCommit(pendingCommit: Request.Commit[F]): State[F, K, V] =
      copy(pendingCommits = pendingCommits.append(pendingCommit))

    def withoutPendingCommits: State[F, K, V] =
      if (pendingCommits.isEmpty) this else copy(pendingCommits = Chain.empty)

    def withRebalancing(rebalancing: Boolean): State[F, K, V] =
      if (this.rebalancing == rebalancing) this else copy(rebalancing = rebalancing)

    def asSubscribed: State[F, K, V] =
      if (subscribed) this else copy(subscribed = true)

    def asUnsubscribed: State[F, K, V] =
      if (!subscribed) this else copy(subscribed = false)

    def asStreaming: State[F, K, V] =
      if (streaming) this else copy(streaming = true)

    override def toString: String = {
      val fetchesString =
        fetches
          .toList
          .sortBy { case (tp, _) => tp }
          .mkStringAppend { case (append, (tp, fs)) =>
            append(tp.toString)
            append(" -> ")
            append(fs.mkString("[", ", ", "]"))
          }("", ", ", "")

      s"State(fetches = Map($fetchesString), records = Map(${recordsString(records)}), pendingCommits = $pendingCommits, onRebalances = $onRebalances, rebalancing = $rebalancing, subscribed = $subscribed, streaming = $streaming)"
    }

  }

  object State {

    def empty[F[_], K, V]: State[F, K, V] =
      State(
        fetches = Map.empty,
        records = Map.empty,
        pendingCommits = Chain.empty,
        onRebalances = Chain.empty,
        rebalancing = false,
        subscribed = false,
        streaming = false
      )

  }

  sealed abstract class FetchCompletedReason {

    final def topicPartitionRevoked: Boolean = this match {
      case FetchCompletedReason.TopicPartitionRevoked => true
      case FetchCompletedReason.FetchedRecords        => false
    }

  }

  object FetchCompletedReason {

    case object TopicPartitionRevoked extends FetchCompletedReason

    case object FetchedRecords extends FetchCompletedReason

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

    final case class ManualCommitAsync[F[_]](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: Either[Throwable, Unit] => F[Unit]
    ) extends Request[F, Any, Any]

    final case class ManualCommitSync[F[_]](
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: Either[Throwable, Unit] => F[Unit]
    ) extends Request[F, Any, Any]

  }

}
