/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Eq
import cats.Show

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
  * Describes commit-related capabilities for a particular consumer instance.
  */
sealed abstract class KafkaCommitter[F[_]] {

  /**
    * Commits the specified offsets and metadata.
    */
  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /**
    * Returns the current consumer group metadata.
    */
  def metadata: F[ConsumerGroupMetadata]

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

      override def toString: String =
        "KafkaCommitter$" + System.identityHashCode(this)

    }

  implicit def kafkaCommitterEq[F[_]]: Eq[KafkaCommitter[F]] =
    Eq.fromUniversalEquals

  implicit def kafkaCommitterShow[F[_]]: Show[KafkaCommitter[F]] =
    Show.fromToString

}
