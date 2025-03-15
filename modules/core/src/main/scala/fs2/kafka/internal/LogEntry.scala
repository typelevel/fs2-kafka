/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.util.regex.Pattern

import scala.collection.immutable.SortedSet

import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.all.*
import fs2.kafka.instances.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.KafkaConsumerActor.*
import fs2.kafka.internal.LogLevel.*
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk

import org.apache.kafka.common.TopicPartition

sealed abstract private[kafka] class LogEntry {

  def level: LogLevel

  def message: String

}

private[kafka] object LogEntry {

  final case class SubscribedTopics[F[_]](
    topics: NonEmptyList[String],
    state: State[F, ?, ?]
  ) extends LogEntry {

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
      s"Consumer manually assigned partitions [${partitions.toList.mkString(", ")}]. Current state [$state]."

  }

  final case class SubscribedPattern[F[_]](
    pattern: Pattern,
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer subscribed to pattern [$pattern]. Current state [$state]."

  }

  final case class Unsubscribed[F[_]](
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer unsubscribed from all partitions. Current state [$state]."

  }

  final case class StoredOnRebalance[F[_]](
    onRebalance: OnRebalance[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored OnRebalance [$onRebalance]. Current state [$state]."

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
    partitions: Set[TopicPartition],
    partitionState: Map[TopicPartition, PartitionState[F, K, V]],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String = {
      var message = s"Revoked partitions [${partitions.mkString(", ")}]"

      if (partitionState.nonEmpty) {
        val withSpillover =
          partitionState.view.filter(_._2.isQueueFull).map(kv => kv._1 -> kv._2.spillover).toMap

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
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored pending commit [$commit] as rebalance is in-progress. Current state [$state]."

  }

  final case class CommittedPendingCommit[F[_]](pendingCommit: Request.Commit[F]) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String = s"Committed pending commits [$pendingCommit]."

  }

  def recordsString[F[_]](
    records: Map[TopicPartition, Chunk[CommittableConsumerRecord[F, ?, ?]]]
  ): String =
    records
      .toList
      .sortBy { case (tp, _) => tp }
      .mkStringAppend { case (append, (tp, chunk)) =>
        append(tp.show)
        append(" -> { first: ")
        append(chunk.head.get.offset.show)
        append(", last: ")
        append(chunk.last.get.offset.show)
        append(" }")
      }("", ", ", "")

}
