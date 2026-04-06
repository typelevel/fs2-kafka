/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.all.*
import cats.Eq
import cats.Foldable
import cats.Parallel
import cats.Show
import fs2.kafka.instances.*
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * Offsets and metadata for topics and partitions which can be committed.
  */
sealed abstract class CommittableOffsetBatch[F[_]] {

  /**
    * Returns a new batch with the specified offset included.
    */
  def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F]

  /**
    * Returns a new batch with the specified offsets included.
    */
  def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F]

  /**
    * Returns the offsets and metadata included in the batch.
    */
  def offsets: Map[KafkaCommitter[F], Map[TopicPartition, OffsetAndMetadata]]

  /**
    * Commits the offsets and metadata for the topics and partitions.
    */
  def commit: F[Unit]

}

object CommittableOffsetBatch {

  final private[this] case class CommittableOffsetBatchImpl[F[_]: Parallel](
    override val offsets: Map[KafkaCommitter[F], Map[TopicPartition, OffsetAndMetadata]]
  ) extends CommittableOffsetBatch[F] {

    override def updated(that: CommittableOffset[F]): CommittableOffsetBatch[F] = {
      copy(offsets.updatedUsing(that.committer) {
        case Some(offsets) =>
          Some(offsets.updatedUsing(that.topicPartition) {
            case existing @ Some(offset) if offset >= that.offsetAndMetadata =>
              existing
            case Some(_) | None =>
              Some(that.offsetAndMetadata)
          })
        case None =>
          Some(Map(that.topicPartition -> that.offsetAndMetadata))
      })
    }

    override def updated(that: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
      if (that.offsets.isEmpty) this
      else if (this.offsets.isEmpty) that
      else
        copy(
          this.offsets ++ that
            .offsets
            .map { case (committer, thatOffsets) =>
              (
                committer,
                this.offsets.get(committer) match {
                  case Some(thisOffsets) =>
                    thisOffsets ++ thatOffsets.map { case (topicPartition, thatOffset) =>
                      (
                        topicPartition,
                        thisOffsets.get(topicPartition) match {
                          case Some(thisOffset) if thisOffset >= thatOffset => thisOffset
                          case Some(_) | None                               => thatOffset
                        }
                      )
                    }
                  case None =>
                    thatOffsets
                }
              )
            }
        )

    override def commit: F[Unit] =
      offsets.toList.parTraverse_ { case (committer, offsets) => committer.commit(offsets) }

    override def toString: String =
      show"CommittableOffsetBatch($offsets)"

  }

  /**
    * Returns a new offset batch from the specified foldable offsets.
    */
  def fromFoldable[F[_]: Parallel, G[_]: Foldable](
    offsets: G[CommittableOffset[F]]
  ): CommittableOffsetBatch[F] =
    fromFoldableMap(offsets)(identity)

  /**
    * Returns a new offset batch from the specified foldable elements.
    *
    * The provided function is used on each element to retrieve the offset.
    */
  def fromFoldableMap[F[_]: Parallel, G[_]: Foldable, A](
    ga: G[A]
  )(f: A => CommittableOffset[F]): CommittableOffsetBatch[F] =
    ga.foldLeft(empty) { case (batch, a) => batch.updated(f(a)) }

  /**
    * Returns a new offset batch from the specified optional offsets.
    */
  def fromFoldableOption[F[_]: Parallel, G[_]: Foldable](
    offsets: G[Option[CommittableOffset[F]]]
  ): CommittableOffsetBatch[F] = {
    offsets.foldLeft(empty) {
      case (batch, Some(offset)) => batch.updated(offset)
      case (batch, None)         => batch
    }
  }

  /**
    * Returns an empty batch without any offsets or metadata.
    */
  def empty[F[_]: Parallel]: CommittableOffsetBatch[F] =
    CommittableOffsetBatchImpl(Map.empty)

  implicit def committableOffsetBatchEq[F[_]]: Eq[CommittableOffsetBatch[F]] =
    Eq.fromUniversalEquals

  implicit def committableOffsetBatchShow[F[_]]: Show[CommittableOffsetBatch[F]] =
    Show.fromToString

}
