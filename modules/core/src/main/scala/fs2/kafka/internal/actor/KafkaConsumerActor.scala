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
import fs2.kafka.internal.LogEntry.*
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
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      dispatcher.unsafeRunSync {
        state2.evalUpdate { state =>
          val currentAssignment = state.partitionGroupState.keys.toList.flatten.toSet
          val targetAssignment  = currentAssignment -- partitions.asScala
          alignPartitionState(targetAssignment, state.partitionGroupState)
            .map(state.withGroupState)
        }
      }
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
      dispatcher.unsafeRunSync {
        state2.evalUpdate { state =>
          val currentAssignment = state.partitionGroupState.keys.toList.flatten.toSet
          val targetAssignment  = currentAssignment ++ partitions.asScala
          alignPartitionState(targetAssignment, state.partitionGroupState).map(state.withGroupState)
        }
      }
  }

  /** Method that will receive all group partition updates.
    *
    * Turns all new issued partition group state into a stream that will be interrupted whenever the group is revoked.
    */
  def consume(): Stream[F, Map[Set[TopicPartition], Stream[F, Chunk[CommittableConsumerRecord[F, K, V]]]]] =
    for {
      _          <- Stream.resource(Resource.make(state2.update(_.withStreaming()))(_ => state2.update(_.withNotStreaming())))
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
                         targetAssignment  <- withConsumer.blocking(_.assignment()).map(_.asScala.toSet)
                         alignedGroups     <- alignPartitionState(targetAssignment, s.partitionGroupState)
                         enqueuedRecords   <- enqueueRecords(records, alignedGroups)
                         spillOverEnqueued <- enqueueSpillOver(enqueuedRecords)
                       } yield s.withGroupState(spillOverEnqueued)
                     }
        } yield ()
      case request @ Request.Commit(_, _)           => commit(request)
      case request @ Request.ManualCommitSync(_, _) => manualCommitSync(request)
      case Request.WithPermit(fa, cb)               => fa.attempt >>= cb
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

  private def alignPartitionState(targetAssignment: Set[TopicPartition], state: GroupState): F[GroupState] = {
    val currentAssignment = state.keys.toSet
    if (targetAssignment == currentAssignment) {
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
          (_, exception) => callback(Option(exception).toLeft(()))
        )
      }
      .handleErrorWith(e => F.delay(callback(Left(e))))

  private[this] def commit(request: Request.Commit[F]): F[Unit] =
    state2.modify { state =>
      val isRebalanceOrHasPending = state.rebalancing || state.pendingCommits.nonEmpty
      val commitAsyncF            = commitAsync(request.offsets, request.callback)
      if (isRebalanceOrHasPending) {
        val newState = state.withPendingCommit(commitAsyncF)
        newState -> logging.log(CommittedPendingCommit(request))
      } else (state, commitAsyncF)
    }.flatten

  private[this] def manualCommitSync(request: Request.ManualCommitSync[F]): F[Unit] = {
    val commit = withConsumer.blocking(_.commitSync(request.offsets.asJava, settings.commitTimeout.toJava))
    commit.attempt >>= request.callback
  }

  def offsetCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    runCommitAsync(offsets)(cb => requests.offer(Request.Commit(offsets, cb)))

  private[this] def runCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata])(k: (Either[Throwable, Unit] => Unit) => F[Unit]): F[Unit] =
    Async[F]
      .async((cb: Either[Throwable, Unit] => Unit) => k(cb).as(Some(F.unit)))
      .timeoutTo(
        settings.commitTimeout,
        F.defer(F.raiseError[Unit] {
          CommitTimeoutException(
            settings.commitTimeout,
            offsets
          )
        })
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

  def pollRecords: F[ConsumerRecords] =
    state2.get.flatMap {
      case state if state.subscribed && state.streaming =>
        for {
          partition <- state.partitionGroupState.partition { case (k, state) => state.spillover.isEmpty }.pure[F]
          toResume   = partition._1.keys
          toPause    = partition._2.keys
          batch     <- withConsumer.blocking { consumer =>
                         if (toPause.nonEmpty) consumer.pause(toPause.flatten.toList.asJava)
                         if (toResume.nonEmpty) consumer.resume(toResume.flatten.toList.asJava)
                         consumer.poll(pollTimeout)
                       }
          records   <- records(batch)
        } yield records
      case _                                            => Map.empty.pure[F]
    }

}
