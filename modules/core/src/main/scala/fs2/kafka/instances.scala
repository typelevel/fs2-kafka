/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Order, Show}
import cats.instances.int.*
import cats.instances.long.*
import cats.instances.string.*
import cats.instances.tuple.*
import cats.syntax.show.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

object instances {

  implicit val fs2KafkaOffsetAndMetadataOrder: Order[OffsetAndMetadata] =
    Order.by(oam => (oam.offset, oam.metadata))

  implicit val fs2KafkaOffsetAndMetadataOrdering: Ordering[OffsetAndMetadata] =
    Order[OffsetAndMetadata].toOrdering

  implicit val fs2KafkaOffsetAndMetadataShow: Show[OffsetAndMetadata] =
    Show.show { oam =>
      if (oam.metadata.nonEmpty)
        show"(${oam.offset}, ${oam.metadata})"
      else oam.offset.show
    }

  implicit val fs2KafkaRecordMetadataShow: Show[RecordMetadata] =
    Show.show(rm => show"${rm.topic}-${rm.partition}@${rm.offset}")

  implicit val fs2KafkaTopicPartitionOrder: Order[TopicPartition] =
    Order.by(tp => (tp.topic, tp.partition))

  implicit val fs2KafkaTopicPartitionOrdering: Ordering[TopicPartition] =
    Order[TopicPartition].toOrdering

  implicit val fs2KafkaTopicPartitionShow: Show[TopicPartition] =
    Show.show(tp => show"${tp.topic}-${tp.partition}")

}
