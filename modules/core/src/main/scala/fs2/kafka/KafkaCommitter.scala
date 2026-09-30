/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.~>
import cats.Eq
import cats.Show

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * Describes commit-related capabilities for a particular consumer instance.
  */
sealed abstract class KafkaCommitter[F[_]] { self =>

  /**
    * Commits the specified offsets and metadata.
    */
  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /**
    * Returns the current consumer group metadata.
    */
  def metadata: F[ConsumerGroupMetadata]

  /**
    * Creates a new [[KafkaCommitter]] in which the effect type has been changed using the specified
    * `FunctionK`.
    *
    * The resulting committer is equal to this one, so offsets committed through either of them can
    * still be merged into a single commit by [[CommittableOffsetBatch]].
    */
  final def mapK[G[_]](f: F ~> G): KafkaCommitter[G] =
    KafkaCommitter.create(source, offsets => f(self.commit(offsets)), f(self.metadata))

  /**
    * The committer this one was derived from through [[mapK]], or this committer itself. Committers
    * with the same source commit through the same consumer, so this is what equality is based on.
    */
  private[kafka] def source: AnyRef

  final override def equals(that: Any): Boolean =
    that match {
      case that: KafkaCommitter[_] => source eq that.source
      case _                       => false
    }

  final override def hashCode(): Int =
    System.identityHashCode(source)

  final override def toString: String =
    "KafkaCommitter$" + System.identityHashCode(source)

}

object KafkaCommitter {

  private[kafka] def apply[F[_]](
    commitOffsets: Map[TopicPartition, OffsetAndMetadata] => F[Unit],
    consumerGroupMetadata: F[ConsumerGroupMetadata]
  ): KafkaCommitter[F] =
    new KafkaCommitter[F] {
      override def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
        commitOffsets(offsets)

      override val metadata: F[ConsumerGroupMetadata] =
        consumerGroupMetadata

      override val source: AnyRef =
        this

    }

  private def create[F[_]](
    committerSource: AnyRef,
    commitOffsets: Map[TopicPartition, OffsetAndMetadata] => F[Unit],
    consumerGroupMetadata: F[ConsumerGroupMetadata]
  ): KafkaCommitter[F] =
    new KafkaCommitter[F] {
      override def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
        commitOffsets(offsets)

      override val metadata: F[ConsumerGroupMetadata] =
        consumerGroupMetadata

      override val source: AnyRef =
        committerSource

    }

  implicit def kafkaCommitterEq[F[_]]: Eq[KafkaCommitter[F]] =
    Eq.fromUniversalEquals

  implicit def kafkaCommitterShow[F[_]]: Show[KafkaCommitter[F]] =
    Show.fromToString

}
