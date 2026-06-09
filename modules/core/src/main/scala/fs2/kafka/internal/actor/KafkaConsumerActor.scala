/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.time.Duration
import java.util

import scala.collection.immutable.SortedSet
import scala.util.matching.Regex

import cats.data.NonEmptyList
import cats.data.NonEmptySet
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.*
import cats.syntax.all.*
import cats.Reducible
import fs2.concurrent.SignallingRef
import fs2.kafka.*
import fs2.kafka.instances.*
import fs2.kafka.internal.{LogEntry, Logging, WithConsumer}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.LogEntry.*
import fs2.Chunk
import fs2.Stream

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * [[KafkaConsumerActor]] wraps a Java `KafkaConsumer` and works like a traditional actor: it
  * receives requests one-at-a-time via a queue, handled by `handle`.
  *
  * Its goal is to stream records for a topic(s) as fast as possible while keeping parallelism
  * bounded: partitions are bundled into at most `maxParallel` groups (see
  * [[PartitionGroupingCalculator]]), and each group is drained by its own stream.
  *
  * ==Consumption flow==
  *
  * With `subscribe` (topic or pattern) the call simply proxies the underlying consumer, and the
  * actual partitions are discovered later through the rebalance listener. With `assign` the call is
  * also proxied, but we additionally seed the internal state so it already reflects the manual
  * assignment.
  *
  * Once the user starts consuming, scheduled `Poll` requests drive everything. On each poll we
  * first apply backpressure: partitions whose group queue is backed up (has spillover) are paused,
  * and the rest are resumed. Then `KafkaConsumer#poll` is called. If we got here via `subscribe`,
  * that poll is what fires the rebalance listeners, so the internal state ends up with the queues
  * it needs; if we got here via `assign`, the state was already in place. Fetched records are
  * handed to the queue of their owning group, and whatever does not fit is parked in that group's
  * spillover list to be retried on the next poll.
  *
  * ==Rebalance flow==
  *
  * A rebalance invokes the rebalance listeners, and assigning or revoking partitions follows the
  * same path: we compute the new assignment by adding/removing the affected partitions, then ask
  * [[PartitionGroupingCalculator]] for the target grouping. Groups whose partitions all survive in
  * the new grouping are left untouched; any group that loses a partition (or gets folded into a
  * different group) is revoked.
  */
final private[kafka] class KafkaConsumerActor[F[_], K, V](
  settings: ConsumerSettings[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  requests: Queue[F, Request[F, K, V]],
  assignment: Queue[F, Option[Map[Set[TopicPartition], PartitionGroupState[F, K, V]]]],
  currentAssignmentRef: SignallingRef[F, Option[SortedSet[TopicPartition]]],
  state: AtomicCell[F, State[F, K, V]],
  withConsumer: WithConsumer[F],
  maxParallel: Int
)(implicit
  F: Async[F],
  dispatcher: Dispatcher[F],
  logging: Logging[F],
  jitter: Jitter[F]
) {

  private[this] type ConsumerRecords = Chunk[CommittableConsumerRecord[F, K, V]]

  private[this] type ConsumerRecordsMap =
    Map[TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]]]

  private type GroupState = Map[Set[TopicPartition], PartitionGroupState[F, K, V]]
  private[this] val pollTimeout: Duration = settings.pollTimeout.toJava
  private[this] val maxPrefetch: Int      = settings.maxPrefetchBatches

  val consumerRebalanceListener: ConsumerRebalanceListener = new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
      dispatcher.unsafeRunSync {
        state.evalUpdate { state =>
          val currentAssignment = state.partitionGroupState.keys.toList.flatten.toSet
          val targetAssignment  = currentAssignment -- partitions.asScala
          alignPartitionState(targetAssignment, state.partitionGroupState).map(state.withGroupState)
        }
      }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
      dispatcher.unsafeRunSync {
        val assigned = SortedSet(partitions.asScala.toList: _*)
        state.evalUpdate { state =>
          val currentAssignment = state.partitionGroupState.keys.toList.flatten.toSet
          val targetAssignment  = currentAssignment ++ assigned
          for {
            groups  <- alignPartitionState(targetAssignment, state.partitionGroupState)
            newState = state.withGroupState(groups)
            _       <- logging.log(AssignedPartitions(assigned, newState))
          } yield newState
        }
      }
  }

  /**
    * Emits the current partition-group assignment as a stream of record streams.
    *
    * Each element is a `Map` from a partition group (`Set[TopicPartition]`) to a stream of `Chunk`s
    * of [[CommittableConsumerRecord]]s for that group. The outer stream advances whenever the
    * assignment is realigned (subscribe, rebalance).
    *
    * Partition groups
    */
  def consume()
    : Stream[F, Map[Set[TopicPartition], Stream[F, Chunk[CommittableConsumerRecord[F, K, V]]]]] =
    for {
      _ <-
        Stream.eval(
          state.get.map(_.subscribed).ifM(().pure[F], NotSubscribedException().raiseError[F, Unit])
        )
      _ <- Stream.resource(
             Resource.make(state.update(_.withStreaming()))(_ => state.update(_.withNotStreaming()))
           )
      assignments0      = Stream.eval(state.get.map(_.partitionGroupState)).filter(_.nonEmpty)
      assignmentUpdates = Stream.fromQueueNoneTerminated(assignment)
      assignment       <- (assignments0 ++ assignmentUpdates).map { assignment =>
                      assignment.map { case (k, state) =>
                        k ->
                          (for {
                            hasPermit <- Stream.resource(state.groupSemaphore.tryPermit)
                            result    <- if (hasPermit) Stream.fromQueueUnterminated(state.queue)
                                      else Stream.empty
                          } yield result).interruptWhen(state.interrupt)
                      }
                    }
    } yield assignment

  def assignments: Stream[F, SortedSet[TopicPartition]] =
    currentAssignmentRef.discrete.takeWhile(_.isDefined).unNone

  def assign(partitions: NonEmptySet[TopicPartition]): F[Unit] =
    F.uncancelable { _ =>
      withConsumer.blocking(_.assign(partitions.toSortedSet.toList.asJava))
    } *> state.evalUpdate { s =>
      for {
        groups <- alignPartitionState(partitions.toSortedSet, Map.empty)
        next    = s.withGroupState(groups).withSubscribed()
        _      <- logging.log(ManuallyAssignedPartitions(partitions, next))
      } yield next
    }

  def subscribe(regex: Regex): F[Unit] =
    for {
      _ <- withConsumer
             .blocking {
               _.subscribe(regex.pattern, consumerRebalanceListener)
             }
             .uncancelable
      newState <- state.updateAndGet(_.withSubscribed())
      _        <- logging.log(SubscribedPattern(regex.pattern, newState))
    } yield ()

  def subscribe[G[_]](topics: G[String])(implicit G: Reducible[G]): F[Unit] =
    for {
      _ <- withConsumer
             .blocking {
               _.subscribe(topics.toList.asJava, consumerRebalanceListener)
             }
             .uncancelable
      newState <- state.updateAndGet(_.withSubscribed())
      _        <- logging.log(SubscribedTopics(NonEmptyList.fromListUnsafe(topics.toList), newState))
    } yield ()

  def handle(request: Request[F, K, V]): F[Unit] =
    request match {
      case Request.Poll() =>
        for {
          records <- pollRecords
          _       <- state.evalUpdate { s =>
                 for {
                   stateAfterEnqueued <- enqueueRecords(records, s.partitionGroupState)
                   spillOverEnqueued  <- enqueueSpillOver(stateAfterEnqueued)
                 } yield s.withGroupState(spillOverEnqueued)
               }
        } yield ()
      case request @ Request.Commit(_, _) =>
        commit(request)
      case request @ Request.ManualCommitSync(_, _) =>
        manualCommitSync(request)
      case Request.WithPermit(fa, cb) =>
        fa.attempt >>= cb
    }

  def unsubscribe(): F[Unit] =
    state.evalUpdate { state =>
      for {
        _ <- assignment.offer(None)
        _ <- state
               .partitionGroupState
               .values
               .toList
               .parTraverse(group =>
                 group.interrupt.complete(().asRight) *> group.groupSemaphore.acquire
               )
        newState = state.withUnsubscribed()
        _       <- withConsumer.blocking(_.unsubscribe())
        _       <- logging.log(Unsubscribed(newState))
      } yield newState
    }

  /**
    * Realigns partition-group state with `targetAssignment`.
    *
    * If the assigned partitions are unchanged, returns `state` as-is. Otherwise:
    *
    *   1. Computes the target grouping via [[PartitionGroupingCalculator.align]].
    *   2. For each existing group whose partition set is not a key in `groupGoal`, interrupts its
    *      record stream, acquires `groupSemaphore` (so dequeuing has stopped), and drains its queue
    *      and `spillover` buffer.
    *   3. Keeps existing groups whose partition set is a key in `groupGoal` unchanged.
    *   4. From drained records, keeps only chunks whose partition is still in `targetAssignment`,
    *      groups them by `TopicPartition`, and carries them into newly created groups.
    *   5. For each partition set in `groupGoal` that is not already in `state`, creates a new
    *      [[PartitionGroupState]] and enqueues carried-over chunks into its queue; chunks that do
    *      not fit remain in `spillover` (and may be retried by [[enqueueSpillOver]] on poll).
    *   6. Publishes `newState` to the `assignment` queue so [[consume]] can expose the updated
    *      streams.
    *
    * When several revoked groups contribute records to the same new group, that group's `spillover`
    * list may temporarily hold more than one chunk until the queue accepts them.
    */
  private def alignPartitionState(
    targetAssignment: Set[TopicPartition],
    state: GroupState
  ): F[GroupState] = {
    val currentAssignment = state.keys.toSet
    if (targetAssignment == currentAssignment.flatten) {
      state.pure[F]
    } else {
      for {
        groupGoal <- PartitionGroupingCalculator
                       .align(targetAssignment, currentAssignment, maxParallel)
                       .pure[F]
        toRevoke   = state -- groupGoal
        toKeep     = state -- (toRevoke.keys)
        _         <- logging.log(RevokedPartitions(toRevoke.keySet, toRevoke)).whenA(toRevoke.nonEmpty)
        spillover <- toRevoke
                       .toList
                       .parFlatTraverse { case (group, state) =>
                         revokePartitionGroup(targetAssignment, group, state)
                       }
        spilloverMap = spillover
                         .map(_.toList)
                         .map(_.groupByNel(_.offset.topicPartition).toMap)
                         .map(_.map { case (k, v) => k -> Chunk.from(v.toList) })
                         .foldLeft(Map.empty[TopicPartition, List[ConsumerRecords]]) {
                           case (acc, elem) =>
                             elem
                               .toList
                               .foldLeft(acc) { case (acc, (tp, records)) =>
                                 acc.updated(tp, acc.getOrElse(tp, List.empty) ++ List(records))
                               }
                         }
        needsAdding    = groupGoal.diff(currentAssignment)
        newPartitions <- needsAdding
                           .toList
                           .traverse { group =>
                             for {
                               semaphore <- Semaphore[F](1)
                               deferred  <- Deferred[F, Either[Throwable, Unit]]
                               queue     <- Queue.bounded[F, Chunk[CommittableConsumerRecord[F, K, V]]](
                                          maxPrefetch
                                        )
                               carryOver =
                                 group.toList.foldMap(tp => spilloverMap.get(tp).toList.flatten)
                               spillover <- queue.tryOfferN(carryOver.map(_.toList).map(Chunk.from))
                             } yield group -> PartitionGroupState(
                               semaphore,
                               deferred,
                               queue,
                               spillover
                             )
                           }
        newState = newPartitions.toMap ++ toKeep
        _       <- newPartitions
               .toMap
               .some
               .filter(_.nonEmpty)
               .map(_.some)
               .traverse(assignment.offer)
               .whenA(newState.keySet != state.keySet)
        newStateSet = SortedSet(newState.keys.toList.flatten: _*)
        _          <- currentAssignmentRef.set(newStateSet.some)
      } yield newState
    }
  }

  private def revokePartitionGroup(
    targetAssignment: Set[TopicPartition],
    group: Set[TopicPartition],
    state: PartitionGroupState[F, K, V]
  ): F[List[ConsumerRecords]] =
    for {
      _                    <- state.interrupt.complete(().asRight)
      safeToDiscard         = group.forall(!targetAssignment.contains(_))
      isEager               = settings.rebalanceRevokeMode == RebalanceRevokeMode.EagerMode
      skipWaitForCompletion = isEager && safeToDiscard
      result               <- if (!skipWaitForCompletion) {
                  for {
                    _ <- state
                           .groupSemaphore
                           .acquire
                           .timeoutTo(
                             settings.sessionTimeout,
                             logging.log(LogEntry.RevokeTimeoutOccurred(group, state)) // TODO: throw exception here ?
                           )
                    queued   <- state.queue.tryTakeN(none)
                    spillover = state.spillover
                    records   =
                      (queued ++ spillover)
                        .map(_.filter(r => targetAssignment.contains(r.offset.topicPartition)))
                        .filter(_.nonEmpty)
                  } yield records
                } else Nil.pure[F]
    } yield result

  private def enqueueSpillOver(groupState: GroupState): F[GroupState] =
    groupState
      .toList
      .traverse { case (group, state) =>
        for {
          newSpillover <- state.queue.tryOfferN(state.spillover)
          newState      = state.copy(spillover = newSpillover)
        } yield group -> newState
      }
      .map(_.toMap)

  private def enqueueRecords(records: ConsumerRecordsMap, groupState: GroupState): F[GroupState] = {
    val groupMap = groupState.keys.flatMap(set => set.map(tp => tp -> set)).toMap
    for {
      enqueueResult <- records
                         .toList
                         .traverse { case (tp, records) =>
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
      toOverride = enqueueResult.flatten.toMap
    } yield groupState ++ toOverride
  }

  private[this] val committer: KafkaCommitter[F] =
    KafkaCommitter(
      commitOffsets = resilientOffsetCommitAsync,
      consumerGroupMetadata = withConsumer.blocking(_.groupMetadata())
    )

  private[this] def resilientOffsetCommitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit] =
    offsetCommitAsync(offsets).handleErrorWith {
      settings.commitRecovery.recoverCommitWith(offsets, offsetCommitAsync(offsets))
    }

  private[this] def commitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata],
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
    commitAsync(request.offsets, request.callback)

  private[this] def manualCommitSync(request: Request.ManualCommitSync[F]): F[Unit] = {
    val commit =
      withConsumer.blocking(_.commitSync(request.offsets.asJava, settings.commitTimeout.toJava))
    commit.attempt >>= request.callback
  }

  def offsetCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    runCommitAsync(offsets)(cb => requests.offer(Request.Commit(offsets, cb)))

  private[this] def runCommitAsync(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  )(k: (Either[Throwable, Unit] => Unit) => F[Unit]): F[Unit] =
    Async[F]
      .async[Unit]((cb: Either[Throwable, Unit] => Unit) => k(cb).as(None))
      .timeoutTo(
        settings.commitTimeout,
        CommitTimeoutException(settings.commitTimeout, offsets).raiseError[F, Unit]
      )

  private[this] def committableConsumerRecord(
    record: ConsumerRecord[K, V],
    partition: TopicPartition
  ): CommittableConsumerRecord[F, K, V] =
    CommittableConsumerRecord(
      record = record,
      offset = CommittableOffset(
        topicPartition = partition,
        offsetAndMetadata = new OffsetAndMetadata(
          record.offset + 1L,
          settings.recordMetadata(record)
        ),
        committer = committer
      )
    )

  private def pollRecords: F[ConsumerRecordsMap] =
    state
      .get
      .flatMap {
        case state if state.subscribed =>
          for {
            partition <-
              state.partitionGroupState.partition { case (_, s) => s.spillover.isEmpty }.pure[F]
            batch <- withConsumer.blocking { consumer =>
                       val assigned = consumer.assignment().asScala.toSet
                       val toResume = partition._1.keys.flatten.filter(assigned.contains)
                       val toPause  = partition._2.keys.flatten.filter(assigned.contains)
                       if (toPause.nonEmpty) consumer.pause(toPause.toList.asJava)
                       if (toResume.nonEmpty) consumer.resume(toResume.toList.asJava)
                       consumer.poll(pollTimeout)
                     }
            records <- records(batch)
          } yield records
        case _ => Map.empty.pure[F]
      }

  private[this] def records(batch: KafkaByteConsumerRecords): F[ConsumerRecordsMap] =
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
