/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.time.Duration
import java.util
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
import fs2.Chunk
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import fs2.Stream

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
  requests:          Queue[F, Request[F, K, V]],
  assignment:        Queue[F, Option[Map[Set[TopicPartition], PartitionGroupState[F, K, V]]]],
  state2:            AtomicCell[F, State2[F, K, V]],
  withConsumer:      WithConsumer[F],
  maxParallel:       Int
)(implicit
  F:                 Async[F],
  dispatcher:        Dispatcher[F],
  logging:           Logging[F],
  jitter:            Jitter[F]
) {

  private[this] type ConsumerRecords = Map[TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]]]
  private type GroupState            = Map[Set[TopicPartition], PartitionGroupState[F, K, V]]
  private[this] val pollTimeout: Duration = settings.pollTimeout.toJava
  private[this] val maxPrefetch: Int      = settings.maxPrefetchBatches

  val consumerRebalanceListener: ConsumerRebalanceListener = new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
      dispatcher.unsafeRunSync {
        state2.evalUpdate { state =>
          val currentAssignment = state.partitionGroupState.keys.toList.flatten.toSet
          val targetAssignment  = currentAssignment -- partitions.asScala
          alignPartitionState(targetAssignment, state.partitionGroupState)
            .map(state.withGroupState)
        }
      }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
      dispatcher.unsafeRunSync {
        state2.evalUpdate { state =>
          val currentAssignment = state.partitionGroupState.keys.toList.flatten.toSet
          val targetAssignment  = currentAssignment ++ partitions.asScala
          alignPartitionState(targetAssignment, state.partitionGroupState)
            .map(state.withGroupState)
        }
      }
  }

  /** Emits the current partition-group assignment as a stream of record streams.
    *
    * Each element is a `Map` from a partition group (`Set[TopicPartition]`) to a stream of `Chunk`s of
    * [[CommittableConsumerRecord]]s for that group. The outer stream advances whenever the assignment is realigned
    * (subscribe, rebalance).
    *
    * Partition groups
    *
    * Assigned partitions are split into up to `maxParallel` groups, each consumed by its own downstream fiber. Groups
    * are sized as evenly as possible; see [[PartitionGroupingCalculator]].
    *
    * Rebalance and partial partition loss
    *
    * A rebalance may revoke only some partitions from what was previously one group (e.g. `{p0, p1, p2}` loses `p1`).
    * Groups whose membership or size is no longer valid are revoked: their record stream is interrupted, buffered
    * records are drained from the group's queue and spillover, and chunks for partitions that are still assigned are
    * re-enqueued into the appropriate new group(s).
    *
    * Groups that still match the target assignment and sizing are kept as-is and keep streaming.
    *
    * Synchronization
    *
    * Each group owns a binary semaphore (`groupSemaphore`). Record consumption acquires it with `tryPermit` and the
    * permit is held until the stream is interrupted; On reassignment (`alignPartitionState`) the stream is interrupted
    * which eventually leads to the permit being released. Once that happens it is safe to drain the state held in the
    * queues and spillover buffers. Only the consumption or reassignment may touch a group's buffer at any time, which
    * makes draining on revoke safe.
    */
  def consume(): Stream[F, Map[Set[TopicPartition], Stream[F, Chunk[CommittableConsumerRecord[F, K, V]]]]] =
    for {
      _          <- Stream.eval(state2.get.map(_.subscribed).ifM(().pure[F], NotSubscribedException().raiseError[F, Unit]))
      _          <- Stream.resource(Resource.make(state2.update(_.withStreaming()))(_ => state2.update(_.withNotStreaming())))
      _          <- Stream.resource(Resource.onFinalize(state2.update(_.withGroupState(Map.empty))))
      assignment <- Stream
                      .fromQueueNoneTerminated(assignment)
                      .map { assignment =>
                        assignment.view.mapValues { state =>
                          (for {
                            hasPermit <- Stream.resource(state.groupSemaphore.tryPermit)
                            result    <- if (hasPermit) Stream.fromQueueUnterminated(state.queue)
                                         else Stream.empty
                          } yield result).interruptWhen(state.interrupt)
                        }.toMap
                      }
    } yield assignment

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Poll()                           =>
        for {
          records <- pollRecords
          _       <- state2.evalUpdate { s =>
                       for {
                         targetAssignment   <- withConsumer.blocking(_.assignment()).map(_.asScala.toSet)
                         alignedGroups      <- alignPartitionState(targetAssignment, s.partitionGroupState)
                         stateAfterEnqueued <- enqueueRecords(records, alignedGroups)
                         spillOverEnqueued  <- enqueueSpillOver(stateAfterEnqueued)
                       } yield s.withGroupState(spillOverEnqueued)
                     }
        } yield ()
      case request @ Request.Commit(_, _)           =>
        commit(request)
      case request @ Request.ManualCommitSync(_, _) =>
        manualCommitSync(request)
      case Request.WithPermit(fa, cb)               =>
        fa.attempt >>= cb
    }

  def unsubscribe(): F[Unit] = state2.evalUpdate { state =>
    for {
      _ <- assignment.offer(None)
      _ <- state
             .partitionGroupState
             .values
             .toList
             .parTraverse(group => group.interrupt.complete(().asRight) *> group.groupSemaphore.acquire)
    } yield state.withUnsubscribed()
  }

  def subscribed(): F[Unit] = state2.update(state => state.withSubscribed())

  /** Realigns partition-group state with `targetAssignment`.
    *
    * If the assigned partitions are unchanged, returns `state` as-is. Otherwise:
    *
    *   1. Computes the target grouping via [[PartitionGroupingCalculator.align]].
    *   2. For each existing group whose partition set is not a key in `groupGoal`, interrupts its record stream,
    *      acquires `groupSemaphore` (so dequeuing has stopped), and drains its queue and `spillover` buffer.
    *   3. Keeps existing groups whose partition set is a key in `groupGoal` unchanged.
    *   4. From drained records, keeps only chunks whose partition is still in `targetAssignment`, groups them by
    *      `TopicPartition`, and carries them into newly created groups.
    *   5. For each partition set in `groupGoal` that is not already in `state`, creates a new [[PartitionGroupState]]
    *      and enqueues carried-over chunks into its queue; chunks that do not fit remain in `spillover` (and may be
    *      retried by [[enqueueSpillOver]] on poll).
    *   6. Publishes `newState` to the `assignment` queue so [[consume]] can expose the updated streams.
    *
    * When several revoked groups contribute records to the same new group, that group's `spillover` list may
    * temporarily hold more than one chunk until the queue accepts them.
    */
  private def alignPartitionState(targetAssignment: Set[TopicPartition], state: GroupState): F[GroupState] = {
    val currentAssignment = state.keys.toSet
    if (targetAssignment == currentAssignment.flatten) {
      state.pure[F]
    } else {
      for {
        assignmentCalc <- PartitionGroupingCalculator.align(targetAssignment, currentAssignment, maxParallel).pure[F]
        toRevoke        = state.removedAll(assignmentCalc.groupGoal)
        toKeep          = state.removedAll(toRevoke.keys)
        spillover      <- toRevoke.toList.flatTraverse { case (_, state) =>
                            for {
                              _        <- state.interrupt.complete(().asRight)
                              _        <- state.groupSemaphore.acquire
                              queued   <- state.queue.tryTakeN(none) // safe since we acquired the semaphore beforehand
                              spillover = state.spillover
                              records   = (queued ++ spillover)
                                .map(_.filter(r => targetAssignment.contains(r.offset.topicPartition)))
                                .filter(_.nonEmpty)
                            } yield records
                          }
        spilloverMap    = spillover
                            .map(_.toList.groupByNel(_.offset.topicPartition).toMap)
                            .foldMap(_.view.mapValues(List(_)).toMap)
        needsAdding     = assignmentCalc.groupGoal.diff(currentAssignment)
        newPartitions  <- needsAdding.toList.traverse { group =>
                            for {
                              semaphore <- Semaphore[F](1)
                              deferred  <- Deferred[F, Either[Throwable, Unit]]
                              queue     <- Queue.bounded[F, Chunk[CommittableConsumerRecord[F, K, V]]](maxPrefetch)
                              carryOver  = group.toList.foldMap(tp => spilloverMap.get(tp).toList.flatten)
                              spillover <- queue.tryOfferN(carryOver.map(_.toList).map(Chunk.from))
                            } yield group -> PartitionGroupState(semaphore, deferred, queue, spillover)
                          }
        newState        = newPartitions.toMap ++ toKeep
        _              <- assignment.offer(newState.some)
      } yield newState
    }
  }

  private def enqueueSpillOver(groupState: GroupState): F[GroupState] =
    groupState.toList.traverse { case (group, state) =>
      for {
        newSpillover <- state.queue.tryOfferN(state.spillover)
        newState      = state.copy(spillover = newSpillover)
      } yield group -> newState
    }.map(_.toMap)

  private def enqueueRecords(records: ConsumerRecords, groupState: GroupState): F[GroupState] = {
    val groupMap = groupState.keys.flatMap(set => set.map(tp => tp -> set)).toMap
    for {
      enqueueResult <- records.toList.traverse { case (tp, records) =>
                         groupMap
                           .get(tp)
                           .flatMap(g => groupState.get(g).tupleLeft(g))
                           .traverse { case (group, gState) =>
                             gState
                               .queue
                               .tryOffer(records)
                               .ifF(gState, gState.withSpillover(records))
                               .tupleLeft(group)
                           }
                       }
      toOverride     = enqueueResult.flatten.toMap
    } yield groupState ++ toOverride
  }

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
          (_, exception) =>
            dispatcher.unsafeRunSync(F.delay(callback(Option(exception).toLeft(()))))
        )
      }
      .handleErrorWith(e => F.delay(callback(Left(e))))

  private[this] def commit(request: Request.Commit[F]): F[Unit] =
    state2.get.flatMap { state =>
      val commitF = commitAsync(request.offsets, request.callback)
      if (state.pendingCommits.nonEmpty)
        state2.update(_.withPendingCommit(commitF))
      else
        commitF
    }

  private[this] def manualCommitSync(request: Request.ManualCommitSync[F]): F[Unit] = {
    val commit = withConsumer.blocking(_.commitSync(request.offsets.asJava, settings.commitTimeout.toJava))
    commit.attempt >>= request.callback
  }

  def offsetCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    runCommitAsync(offsets)(cb => requests.offer(Request.Commit(offsets, cb)))

  private[this] def runCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata])(k: (Either[Throwable, Unit] => Unit) => F[Unit]): F[Unit] =
    Async[F]
      .async[Unit]((cb: Either[Throwable, Unit] => Unit) => k(cb).as(None))
      .timeoutTo(
        settings.commitTimeout,
        CommitTimeoutException(settings.commitTimeout, offsets)
          .raiseError[F, Unit]
      )

  private[this] def committableConsumerRecord(record: ConsumerRecord[K, V], partition: TopicPartition): CommittableConsumerRecord[F, K, V] =
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

  private def pollRecords: F[ConsumerRecords] =
    state2.get.flatMap {
      case state if state.subscribed =>
        for {
          partition <- state.partitionGroupState.partition { case (_, s) => s.spillover.isEmpty }.pure[F]
          batch     <- withConsumer.blocking { consumer =>
                         val assigned = consumer.assignment().asScala.toSet
                         val toResume = partition._1.keys.flatten.filter(assigned.contains)
                         val toPause  = partition._2.keys.flatten.filter(assigned.contains)
                         if (toPause.nonEmpty) consumer.pause(toPause.toList.asJava)
                         if (toResume.nonEmpty) consumer.resume(toResume.toList.asJava)
                         consumer.poll(pollTimeout)
                       }
          records   <- records(batch)
        } yield records
      case _                         => Map.empty.pure[F]
    }

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
}
