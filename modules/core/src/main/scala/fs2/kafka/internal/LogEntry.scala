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
import fs2.kafka.internal.actor.{PartitionState, Request, State2}
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.LogLevel.*
import fs2.kafka.CommittableConsumerRecord

import org.apache.kafka.common.TopicPartition

sealed abstract private[kafka] class LogEntry {

  def level: LogLevel

  def message: String

}

private[kafka] object LogEntry {

  final case class SubscribedTopics[F[_]](topics: NonEmptyList[String], state: State2[F, ?, ?])
      extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer subscribed to topics [${topics.toList.mkString(", ")}]. Current state [$state]."

  }

  final case class ManuallyAssignedPartitions[F[_]](
    partitions: NonEmptySet[TopicPartition],
    state: State2[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer manually assigned partitions [${partitions.toSortedSet.toList.mkString(", ")}]. Current state [$state]."

  }

  final case class SubscribedPattern[F[_]](
    pattern: Pattern,
    state: State2[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer subscribed to pattern [$pattern]. Current state [$state]."

  }

  final case class Unsubscribed[F[_]](state: State2[F, ?, ?]) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer unsubscribed from all partitions. Current state [$state]."

  }

  final case class AssignedPartitions[F[_]](
    partitions: SortedSet[TopicPartition],
    state: State2[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Assigned partitions [${partitions.mkString(", ")}]. Current state [$state]."

  }

  final case class RevokedPartitions[F[_], K, V](
    partitions: Set[Set[TopicPartition]],
    partitionState: Map[Set[TopicPartition], PartitionState[F, K, V]],
    state: State2[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String = {
      var message = s"Revoked partition groups [${partitions.map(_.mkString(", ")).mkString(", ")}]"

      if (partitionState.nonEmpty) {
        val withSpillover = partitionState
          .view
          .filter(_._2.isQueueFull)
          .map(kv => kv._1 -> kv._2.spillover.flatMap(_.toList))
          .toMap

        message += s", dropped record queues [${partitionState.keys.mkString(", ")}]"
        if (withSpillover.nonEmpty)
          message += s", dropped spillover records [${recordsString(withSpillover)}]"
      }
      message += s". Current state [$state]"
      message
    }

  }

  final case class StoredPendingCommit[F[_]](
    commit: Request.Commit[F],
    state: State2[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored pending commit [$commit] as rebalance is in-progress. Current state [$state]."

  }

  final case class CommittedPendingCommit[F[_]](pendingCommit: Request.Commit[F]) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String = s"Committed pending commit [$pendingCommit]."

  }

  final case class RevokeTimeoutOccurred[F[_]](
    revoked: Set[TopicPartition],
    state: State2[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Info

    override def message: String =
      s"Consuming streams did not signal processing completion of [$revoked]. Current state [$state]."

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
        append(chunk.head.offset.show)
        append(" }")
      }("", ", ", "")

}
