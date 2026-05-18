/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.time.Duration
import java.util
import scala.collection.immutable.SortedSet
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.instances.*
import fs2.kafka.internal.Logging
import fs2.kafka.internal.WithConsumer
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.LogEntry.*
import fs2.Chunk
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import PartitionState.PartitionStateMap
import fs2.Stream
import fs2.concurrent.SignallingRef

/** [[KafkaConsumerActor]] wraps a Java `KafkaConsumer` and works similar to a traditional actor, in the sense that it
  * receives requests one at-a-time via a queue, which are received as calls to the `handle` function. `Poll` requests
  * are scheduled at a fixed interval and, when handled, calls the `KafkaConsumer#poll` function, allowing the Java
  * consumer to perform necessary background functions, and to return fetched records.<br><br>
  *
  * The actor receives `Fetch` requests for topic-partitions for which there is demand. The actor then attempts to fetch
  * records for topic-partitions where there is a `Fetch` request. For topic-partitions where there is no request, no
  * attempt to fetch records is made. This effectively enables backpressure, as long as `Fetch` requests are only issued
  * when there is more demand.
  */
final private[kafka] class KafkaConsumerActor[F[_], K, V](
  settings:          ConsumerSettings[F, K, V],
  keyDeserializer:   KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  val ref:           Ref[F, State[F, K, V]],
  requests:          Queue[F, Request[F, K, V]],
  val assignment:    SignallingRef[F, Map[Set[TopicPartition], Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]]]],
  close:             SignallingRef[F, Boolean],
  withConsumer:      WithConsumer[F],
  maxParallel:       Int
)(implicit
  F:                 Async[F],
  dispatcher:        Dispatcher[F],
  logging:           Logging[F],
  jitter:            Jitter[F]
) {

  private[this] type ConsumerRecords = Map[Set[TopicPartition], Chunk[CommittableConsumerRecord[F, K, V]]]
  private[this] type ConsumerQueue   = Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]]

  def getQueueAndStopSignalFor(partition: Set[TopicPartition]): F[(ConsumerQueue, F[Unit])] =
    ensurePartitionStateFor(Set(partition)).flatMap { partitionState =>
      partitionState.get(partition) match {
        case Some(ps) => F.pure((ps.queue, ps.closeSignal.get))
        case None     => new IllegalStateException(s"PartitionState not added for $partition").raiseError
      }
    }

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Poll()                           => poll
      case request @ Request.Commit(_, _)           => commit(request)
      case request @ Request.ManualCommitSync(_, _) => manualCommitSync(request)
      case Request.WithPermit(fa, cb)               => fa.attempt >>= cb
    }

  private[this] def assigned(assigned: SortedSet[TopicPartition]): F[Unit] =
    ref.flatModify(_.withAssignedPartitions(assigned)).flatten

  private[this] def revoked(revoked: SortedSet[TopicPartition]): F[Unit] = {
    calcPartitionDistribution(???, ???, maxParallel)
    ref.flatModify(_.withRevokedPartitions(settings.sessionTimeout, revoked)).flatten
  }

  val consumerRebalanceListener: ConsumerRebalanceListener = new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]):  Unit =
      dispatcher.unsafeRunSync(revoked(partitions.toSortedSet))
    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
      dispatcher.unsafeRunSync(assigned(partitions.toSortedSet))
  }

  /** Drives the partition assignment and record consumption based on the internal assignment state.
    *
    * Listens to the `assignment` [[cats.effect.std.MapRef]] for changes via `discrete`. Each time a new assignment map
    * is emitted, the previous assignment's streams are canceled and finalized before the new map is produced
    * downstream.
    *
    * The returned stream emits a `Map` where each key is a set of topic-partitions and each value is a stream of record
    * chunks for that partition group.
    *
    * Coordination mechanism:
    *   - A `Deferred` is threaded through the `mapAccumulate` fold so that each new assignment can signal the previous
    *     one to stop.
    *   - A list of `Semaphore`s (one per partition group) ensures that the previous assignment's per-partition streams
    *     have fully finalized before the new assignment proceeds.
    *   - Because finalization acquires the semaphore, any previous stream that has not yet started will find the permit
    *     unavailable and will emit nothing (`Stream.empty`), preventing stale Streams from being used.
    */
  def consume(): Stream[F, Map[Set[TopicPartition], Stream[F, Chunk[CommittableConsumerRecord[F, K, V]]]]] =
    for {
      initialDef   <- Stream.eval(Deferred[F, Unit])
      initialWaiEnd = List.empty[Semaphore[F]]
      result       <- assignment
                        .discrete
                        .mapAccumulate(initialDef -> initialWaiEnd) { case ((prevDeferred, prevSemaphores), assignment) =>
                          for {
                            _               <- Stream.eval(prevDeferred.complete(()))
                            nextDeferred    <- Stream.eval(Deferred[F, Either[Throwable, Unit]])
                            _               <- Stream.eval(prevSemaphores.traverse(_.acquire)) // prev semaphores are forever locked
                            nextSemaphores  <- Stream.eval((0 to assignment.size).toList.traverse(_ => Semaphore[F](1)))
                            assignmentStream = assignment
                                                 .toList
                                                 .zipWithIndex
                                                 .map { case ((k, value), i) =>
                                                   val mySemaphore  = nextSemaphores(i)
                                                   // semaphore are allocated and released when stream is interrupted
                                                   // if we can't acquire it's because the stream was already acquired
                                                   // in that case we're better just not emitting anything since it signals
                                                   // a new assignment was issued
                                                   val recordStream = for {
                                                     acquired <- Stream.resource(mySemaphore.tryPermit)
                                                     result   <- if (acquired) Stream.fromQueueUnterminated(value)
                                                                 else Stream.empty.covary[F]
                                                   } yield result
                                                   k -> recordStream
                                                 }
                                                 .toMap
                          } yield (nextDeferred -> nextSemaphores) -> assignmentStream
                        }
    } yield result._2

  private def poll: F[Unit] =
    for {
      targetAssignment <- withConsumer.blocking(_.assignment()).map(_.asScala.toSet)
      stateUpdate      <- ref.flatModify {
                            case state if state.subscribed && state.streaming =>
                              val currentAssignment = state.partitionState.keys.flatten
                              val requiresUpdate    = currentAssignment.toSet.diff(targetAssignment).nonEmpty
                              if (requiresUpdate) updateState(state, targetAssignment)
                              else state -> none.pure[F]
                            case state                                        => state -> none.pure[F]
                          }
      _                <- stateUpdate.traverse { case (_, newAssignment) =>
                            ensurePartitionStateFor(newAssignment.groupGoal)
                          }
      _                <- stateUpdate match { // add all records as spill over
                            case Some((spillover, _)) => addAsSpillOver(spillover).whenA(spillover.nonEmpty)
                            case None                 => ().pure[F]
                          }
      _                <- enqueueSpillOver
      records          <- pollRecords
      newState         <- enqueueRecords(records).flatMap(stateAndReset => ref.flatModify(_ => stateAndReset))
    } yield ()

  private def enqueueSpillOver: F[State[F, K, V]] =
    for {
      state    <- ref.get
      newState <- state
                    .partitionState
                    .toList
                    .parTraverse { case (tp, state) =>
                      for {
                        spillover <- state.queue.tryOfferN(state.spillover)
                      } yield tp -> state.copy(spillover = spillover)
                    }
                    .map(_.toMap)
                    .map(newState => state.copy(partitionState = newState))
      _        <- ref.set(newState)
    } yield ()

  private def addAsSpillOver(
    spillOver: List[Chunk[CommittableConsumerRecord[F, K, V]]]
  ): F[Unit] =
    // TODO: we can create the map of chunks from the spill over in a bit of a better way but for now let's do it this way
    ref.update { state =>
      val stateKeyMap = state
        .partitionState
        .keys
        .toList
        .flatMap(group => group.map(tp => tp -> group))
        .toMap
      spillOver
        .groupBy(_.head.get.offset.topicPartition)
        .toList
        .foldLeft(state) { case (state, (tp, chunks)) =>
          stateKeyMap.get(tp) match {
            case None        => state // should never happen
            case Some(group) =>
              val groupState    = state.partitionState.get(group).get // improve
              val newGroupState = groupState.copy(spillover = groupState.spillover ++ chunks)
              state.copy(partitionState = state.partitionState.updated(group, newGroupState))
          }
        }
    }

  private def updateState(state: State[F, K, V], target: Set[TopicPartition]) = {
    val currentAssignment             = state.partitionState.keys.toSet
    val assignmentCalc                = ReassignmentCalculation.align(target, currentAssignment, maxParallel)
    val (stateToKeep, stateToDiscard) = state
      .partitionState
      .partition { case (tps, _) => assignmentCalc.groupGoal.contains(tps) }
    val revokeF                       = stateToDiscard
      .toList
      .parTraverse { case (group, state) =>
        for {
          _                  <- state.closeSignal.complete(())
          hasPartitionsToKeep = group.exists(assignmentCalc.targetAssignment.contains)
          stateToEnqueue     <- if (hasPartitionsToKeep) {
                                  drainRecordsToKeep(state, assignmentCalc.targetAssignment)
                                } else List.empty.pure[F]
        } yield stateToEnqueue
      }
      .map(_.flatten)
      .flatTap { _ =>
        val revoked = assignmentCalc.groupRevoke.flatten.diff(assignmentCalc.targetAssignment)
        logging.log(RevokedPartitions(revoked, ???, ???))
      }
      .map(recordsToKeep => recordsToKeep -> assignmentCalc)
    state.copy(partitionState = stateToKeep) -> revokeF.map(_.some)
  }

  private def drainRecordsToKeep(
    partitionState: PartitionState[F, K, V],
    target:         Set[TopicPartition]
  ): F[List[Chunk[CommittableConsumerRecord[F, K, V]]]] =
    partitionState
      .queue
      .tryTakeN(none)
      .map(_ ++ partitionState.spillover)
      .map(recordChunks =>
        recordChunks
          .map(chunk => chunk.filter(r => target.contains(r.offset.topicPartition)))
          .filter(_.nonEmpty)
      )

  private[this] val committer: KafkaCommitter[F] =
    KafkaCommitter(
      commitOffsets         = resilientOffsetCommitAsync,
      consumerGroupMetadata = withConsumer.blocking(_.groupMetadata())
    )

  private[this] def resilientOffsetCommitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit] =
    offsetCommitAsync(offsets).handleErrorWith {
      settings.commitRecovery.recoverCommitWith(offsets, offsetCommitAsync(offsets))
    }

  private[this] def commitAsync(
    offsets:  Map[TopicPartition, OffsetAndMetadata],
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
      if (state.rebalancing || state.pendingCommits.nonEmpty) {
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
  )(k:       (Either[Throwable, Unit] => Unit) => F[Unit]
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

  def offsetCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    runCommitAsync(offsets)(cb => requests.offer(Request.Commit(offsets, cb)))

  private[this] def committableConsumerRecord(
    record:    ConsumerRecord[K, V],
    partition: TopicPartition
  ): CommittableConsumerRecord[F, K, V] =
    CommittableConsumerRecord(
      record = record,
      offset = CommittableOffset(
        topicPartition    = partition,
        offsetAndMetadata = new OffsetAndMetadata(
          record.offset + 1L,
          settings.recordMetadata(record)
        ),
        committer         = committer
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

  def enqueueRecords(records: ConsumerRecords) =
    for {
      state              <- ref.get
      groupsMap           = state.partitionState.keys.toList.flatMap(tps => tps.map(tp => tp -> tps).toSet).toMap
      newPartitionsState <- records.toList.traverse { case (tp, records) =>
                              val group          = groupsMap.get(tp).get
                              val partitionState = state
                                .partitionState
                                .get(group)
                                .get
                              val toEnqueue      = partitionState.spillover :+ records
                              partitionState
                                .queue
                                .tryOfferN(toEnqueue)
                                .map(spillover => group -> partitionState.copy(spillover = spillover))
                            }.map(_.toMap)
    } yield state.copy(partitionState = newPartitionsState)

  def pollRecords =
    for {
      state           <- ref.get
      partitions       = state.partitionState.partition { case (_, state) => state.isQueueFull }
      pausePartitions  = partitions._1
      resumePartitions = partitions._2
      batch           <- withConsumer.blocking { consumer =>
                           if (pausePartitions.nonEmpty) consumer.pause(pausePartitions.keys.toSet.flatten.asJava)
                           if (resumePartitions.nonEmpty) consumer.resume(resumePartitions.keys.toSet.flatten.asJava)
                           consumer.poll(pollTimeout)
                         }
      result          <- records(batch)
    } yield result

  /** Ensures the state holds a PartitionState for each requested partition, and returns the map of all partition states
    * (including at least the requested partitions).
    *
    * At the cost of additional synchronization on the internal state `ref`, this method optimistically assumes there is
    * already an entry for the requested partition. The intent is to avoid over-provisioning `PartitionState` instances.
    * New instances are then created as necessary, and atomically added to the state.
    *
    * This may still create and discard duplicate `PartitionState` instances. Any previously added to the state will be
    * returned and not overwritten.
    *
    * Called by `poll`, and `getQueueAndStopSignalFor` (for `KafkaConsumer`).
    */
  private[this] def ensurePartitionStateFor(
    partitions: Set[Set[TopicPartition]]
  ): F[PartitionStateMap[F, K, V]] =
    ref
      .get
      .flatMap { state =>
        (partitions -- state.partitionState.keys.toSet).toList match {
          case Nil     => F.pure(state.partitionState)
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

}
