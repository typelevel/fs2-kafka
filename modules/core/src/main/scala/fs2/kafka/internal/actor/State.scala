/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.Monad
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Ref
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.kafka.CommittableConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka] object State {

  def empty[F[_]: Async, K, V] =
    State[F, K, V](
      Map.empty,
      false,
      false,
      Map.empty
    )

}

/**
  * Records of a partition group, waiting to be processed by the streams of the group.
  *
  * Fetched records are offered to the bounded `queue`, and records which do not fit are kept in
  * `spillover`. Streams dequeue records in chunks, which are kept `inFlight` until the stream has
  * emitted them and the downstream has pulled past them. Streams are interrupted when the group is
  * revoked, so records in any of these places might not have been processed.
  *
  * Each stream holds a permit of the `groupSemaphore` while running. Revoking the group acquires all
  * permits, which waits for running streams to finish, and stops new streams from starting. Records
  * which have not been processed by then can be carried over to new partition groups.
  */
final private[kafka] case class PartitionGroupState[F[_], K, V](
  groupSemaphore: Semaphore[F],
  interrupt: Deferred[F, Either[Throwable, Unit]],
  queue: Queue[F, Chunk[CommittableConsumerRecord[F, K, V]]],
  inFlight: Ref[F, Vector[Chunk[CommittableConsumerRecord[F, K, V]]]],
  spillover: List[Chunk[CommittableConsumerRecord[F, K, V]]]
) {

  def withSpillover(record: Chunk[CommittableConsumerRecord[F, K, V]]) =
    copy(spillover = spillover :+ record)

  def setSpillover(newSpillover: List[Chunk[CommittableConsumerRecord[F, K, V]]]) =
    copy(spillover = newSpillover)

  def clearSpillover(): PartitionGroupState[F, K, V] = copy(spillover = Nil)

  /**
    * Returns a stream of records for all partitions in the group.
    */
  def records(implicit F: Async[F]): Stream[F, CommittableConsumerRecord[F, K, V]] =
    dequeue.flatMap(chunk => Stream.chunk(chunk) ++ Stream.exec(acknowledge(chunk)))

  /**
    * Returns a stream of records for each of the specified partitions in the group.
    * The first stream to start dequeues records for all of the streams, so all of
    * the streams have to be consumed.
    */
  def partitionRecords(partitions: Set[TopicPartition])(implicit
    F: Async[F]
  ): F[Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]] =
    for {
      stop <- Deferred[F, Either[Throwable, Unit]]
      started <- Deferred[F, Unit]
      queues <- partitions.toList
        .traverse { partition =>
          Queue.bounded[F, Chunk[CommittableConsumerRecord[F, K, V]]](1).tupleLeft(partition)
        }
        .map(_.toMap)
    } yield {
      val distribute: Stream[F, Nothing] =
        Stream.eval(started.complete(())).flatMap { isFirst =>
          if (isFirst)
            dequeue
              .evalMap(chunk => queues(chunk.head.get.offset.topicPartition).offer(chunk))
              .drain
              .onFinalize(stop.complete(().asRight).void)
          else Stream.empty
        }

      queues.map { case (partition, partitionQueue) =>
        partition -> withPermit {
          Stream
            .fromQueueUnterminated(partitionQueue)
            .flatMap(chunk => Stream.chunk(chunk) ++ Stream.exec(acknowledge(chunk)))
            .concurrently(distribute)
        }.interruptWhen(stop).onFinalize(stop.complete(().asRight).void)
      }
    }

  /**
    * Waits for running streams to finish, and stops new streams from starting.
    */
  def awaitStreams: F[Unit] =
    groupSemaphore.acquireN(PartitionGroupState.StreamPermits)

  /**
    * Returns the records which have not been processed, in the order they
    * were fetched. Assumes streams have finished, see [[awaitStreams]].
    */
  def unprocessed(implicit F: Monad[F]): F[List[Chunk[CommittableConsumerRecord[F, K, V]]]] =
    for {
      queued <- queue.tryTakeN(None)
      dequeued <- inFlight.get
    } yield dequeued.toList ++ queued ++ spillover

  private def dequeue(implicit F: Async[F]): Stream[F, Chunk[CommittableConsumerRecord[F, K, V]]] =
    withPermit(Stream.repeatEval(lease).flatMap(Stream.emits(_))).interruptWhen(interrupt)

  /**
    * Dequeues all queued chunks, waiting for at least one, and keeps them
    * in-flight until they are acknowledged. A chunk is dequeued if and
    * only if it is kept in-flight.
    */
  private def lease(implicit F: Async[F]): F[List[Chunk[CommittableConsumerRecord[F, K, V]]]] =
    F.uncancelable { poll =>
      queue
        .tryTakeN(None)
        .flatMap {
          case Nil => poll(queue.take).map(List(_))
          case chunks => F.pure(chunks)
        }
        .flatTap(chunks => inFlight.update(_ ++ chunks))
    }

  private def acknowledge(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] =
    inFlight.update { chunks =>
      val index = chunks.indexWhere(_ eq chunk)
      if (index >= 0) chunks.patch(index, Nil, 1) else chunks
    }

  private def withPermit[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, A] =
    Stream.resource(groupSemaphore.tryPermit).flatMap { hasPermit =>
      if (hasPermit) stream else Stream.empty
    }

}

private[kafka] object PartitionGroupState {

  /**
    * Streams hold one permit each, so acquiring all permits waits for all streams to finish.
    */
  private val StreamPermits: Long = Long.MaxValue

  def create[F[_], K, V](queueSize: Int)(implicit F: Async[F]): F[PartitionGroupState[F, K, V]] =
    for {
      groupSemaphore <- Semaphore[F](StreamPermits)
      interrupt <- Deferred[F, Either[Throwable, Unit]]
      queue <- Queue.bounded[F, Chunk[CommittableConsumerRecord[F, K, V]]](queueSize)
      inFlight <- Ref.of[F, Vector[Chunk[CommittableConsumerRecord[F, K, V]]]](Vector.empty)
    } yield PartitionGroupState(groupSemaphore, interrupt, queue, inFlight, Nil)

}

final private[kafka] case class State[F[_], K, V](
  partitionGroupState: Map[Set[TopicPartition], PartitionGroupState[F, K, V]],
  subscribed: Boolean,
  streaming: Boolean,
  requestedCommitOffsets: Map[TopicPartition, OffsetAndMetadata]
)(implicit
  F: Async[F]
) {

  def withGroupState(s: Map[Set[TopicPartition], PartitionGroupState[F, K, V]]) =
    copy(partitionGroupState = s)

  def withUnsubscribed(): State[F, K, V] =
    copy(
      partitionGroupState = Map.empty,
      subscribed = false,
      streaming = false,
      requestedCommitOffsets = Map.empty
    )

  def withSubscribed(): State[F, K, V] = copy(subscribed = true)

  def withStreaming(): State[F, K, V] =
    copy(streaming = true)

  def assignedPartitions: Set[TopicPartition] =
    partitionGroupState.keySet.flatten

  /**
    * Records offsets to be committed on revoke. Offsets for partitions which are not currently
    * assigned are ignored: they come from commit requests handled after their partition was
    * revoked, and committing them on a later revoke would rewind the consumer group.
    */
  def withRequestedCommitOffsets(
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): State[F, K, V] = {
    val assigned = assignedPartitions
    copy(requestedCommitOffsets = offsets.foldLeft(requestedCommitOffsets) {
      case (acc, (partition, offsetAndMetadata)) =>
        val isNewer = acc.get(partition).forall(_.offset < offsetAndMetadata.offset)
        if (assigned.contains(partition) && isNewer) acc.updated(partition, offsetAndMetadata)
        else acc
    })
  }

  /**
    * Forgets requested offsets of newly assigned partitions, which may be left from an earlier
    * assignment of the same partitions.
    */
  def withoutRequestedCommitOffsets(partitions: Set[TopicPartition]): State[F, K, V] =
    copy(requestedCommitOffsets = requestedCommitOffsets -- partitions)

  def removeRequestedCommitOffsets(
    revoked: Set[TopicPartition]
  ): (State[F, K, V], Map[TopicPartition, OffsetAndMetadata]) = {
    val (toCommit, retained) =
      requestedCommitOffsets.partition { case (partition, _) => revoked.contains(partition) }
    (copy(requestedCommitOffsets = retained), toCommit)
  }

  override def toString: String =
    s"State(partitionGroupState = $partitionGroupState, subscribed = $subscribed, streaming = $streaming)"

}
