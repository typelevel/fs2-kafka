/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.all.*
import cats.Eq
import cats.Show
import fs2.kafka.instances.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * An offset and metadata for a topic and partition which can be committed.
  *
  * Note offsets are most often committed in batches for performance reasons.
  */
sealed abstract class CommittableOffset[F[_]] {

  /**
    * The topic and partition for the offset and metadata.
    */
  def topicPartition: TopicPartition

  /**
    * The offset and metadata for the topic and partition.
    */
  def offsetAndMetadata: OffsetAndMetadata

  /**
    * The committer used to commit the offset and metadata.
    */
  def committer: KafkaCommitter[F]

  /**
    * Commits the offset and metadata for the topic and partition.
    */
  def commit: F[Unit]

}

object CommittableOffset {

  final private[this] case class CommittableOffsetImpl[F[_]](
    override val topicPartition: TopicPartition,
    override val offsetAndMetadata: OffsetAndMetadata,
    override val committer: KafkaCommitter[F]
  ) extends CommittableOffset[F] {

    override def commit: F[Unit] =
      committer.commit(Map(topicPartition -> offsetAndMetadata))

    override def toString: String =
      show"CommittableOffset($topicPartition -> $offsetAndMetadata, $committer)"

  }

  private[kafka] def apply[F[_]](
    topicPartition: TopicPartition,
    offsetAndMetadata: OffsetAndMetadata,
    committer: KafkaCommitter[F]
  ): CommittableOffset[F] =
    CommittableOffsetImpl(topicPartition, offsetAndMetadata, committer)

  implicit def committableOffsetEq[F[_]]: Eq[CommittableOffset[F]] =
    Eq.fromUniversalEquals

  implicit def committableOffsetShow[F[_]]: Show[CommittableOffset[F]] =
    Show.fromToString

}
