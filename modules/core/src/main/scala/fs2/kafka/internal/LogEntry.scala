/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.util.regex.Pattern

import scala.collection.immutable.SortedSet

import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.all.*
import fs2.kafka.instances.*
import fs2.kafka.internal.actor.{PartitionGroupState, State}
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.LogLevel.*
import fs2.kafka.CommittableConsumerRecord

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

sealed abstract private[kafka] class LogEntry {

  def level: LogLevel

  def message: String

}

private[kafka] object LogEntry {

  final case class SubscribedTopics[F[_]](topics: NonEmptyList[String], state: State[F, ?, ?])
      extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer subscribed to topics [${topics.toList.mkString(", ")}]. Current state [$state]."

  }

  final case class ManuallyAssignedPartitions[F[_]](
    partitions: NonEmptySet[TopicPartition],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer manually assigned partitions [${partitions.toSortedSet.toList.mkString(", ")}]. Current state [$state]."

  }

  final case class SubscribedPattern[F[_]](
    pattern: Pattern,
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer subscribed to pattern [$pattern]. Current state [$state]."

  }

  final case class Unsubscribed[F[_]](state: State[F, ?, ?]) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer unsubscribed from all partitions. Current state [$state]."

  }

  final case class AssignedPartitions[F[_]](
    partitions: SortedSet[TopicPartition],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Assigned partitions [${partitions.mkString(", ")}]. Current state [$state]."

  }

  final case class RevokedPartitions[F[_], K, V](
    partitions: Set[Set[TopicPartition]],
    partitionState: Map[Set[TopicPartition], PartitionGroupState[F, K, V]]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String = {
      var message = s"Revoked partition groups [${partitions.map(_.mkString(", ")).mkString(", ")}]"

      if (partitionState.nonEmpty) {
        val withSpillover = partitionState
          .view
          .filter(_._2.spillover.nonEmpty)
          .map(kv => kv._1 -> kv._2.spillover.flatMap(_.toList))
          .toMap

        message += s", dropped record queues [${partitionState.keys.mkString(", ")}]"
        if (withSpillover.nonEmpty)
          message += s", dropped spillover records [${recordsString(withSpillover)}]"
      }

      message
    }

  }

  final case class RevokeTimeoutOccurred[F[_], K, V](
    group: Set[TopicPartition],
    groupState: PartitionGroupState[F, K, V]
  ) extends LogEntry {

    override def level: LogLevel = Info

    override def message: String =
      s"Consuming streams did not signal processing completion of [$group]. Current state [$groupState]."

  }

  final case class CommittedOffsetsOnRevoke(
    revoked: Set[TopicPartition],
    offsets: Map[TopicPartition, OffsetAndMetadata],
    result: Either[Throwable, Unit]
  ) extends LogEntry {

    override def level: LogLevel = Info

    override def message: String =
      result match {
        case Right(()) =>
          s"Committed offsets [${offsets
              .mkString(", ")}] synchronously on revoke of partitions [${revoked.mkString(", ")}]."
        case Left(e) =>
          s"Failed to commit offsets [${offsets
              .mkString(", ")}] synchronously on revoke of partitions [${revoked.mkString(", ")}]: $e."
      }

  }

  def recordsString[F[_]](
    records: Map[Set[TopicPartition], List[CommittableConsumerRecord[F, ?, ?]]]
  ): String =
    records
      .values
      .flatten
      .groupBy(_.offset.topicPartition)
      .toList
      .sortBy { case (tp, _) => tp }
      .mkStringAppend { case (append, (tp, chunk)) =>
        append(tp.show)
        append(" -> { first: ")
        append(chunk.head.offset.show)
        append(", last: ")
        append(chunk.last.offset.show)
        append(" }")
      }("", ", ", "")

}
