/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

trait KafkaCommit[F[_]] {

  /**
    * Commit the specified offsets for the specified list of topics and partitions to Kafka.<br><br>
    *
    * This commits offsets to Kafka. The offsets committed using this API will be used on the first
    * fetch after every rebalance and also on startup. As such, if you need to store offsets in
    * anything other than Kafka, this API should not be used. The committed offset should be the
    * next message your application will consume, i.e. lastProcessedMessageOffset + 1. If automatic
    * group management with subscribe is used, then the committed offsets must belong to the
    * currently auto-assigned partitions.<br><br>
    *
    * Offsets committed through multiple calls to this API are guaranteed to be sent in the same
    * order as the invocations. Additionally note that offsets committed through this API are
    * guaranteed to complete before a subsequent call to [[commitSync]] (and variants)
    * returns.<br><br>
    *
    * Note, that the recommended way for committing offsets in fs2-kafka is to use `commit` on
    * [[CommittableConsumerRecord]], [[CommittableOffset]] or [[CommittableOffsetBatch]].
    * [[commitAsync]] and [[commitSync]] usually needs only for some custom scenarios.
    *
    * @param offsets
    *   A map of offsets by partition with associate metadata.
    * @see
    *   org.apache.kafka.clients.consumer.KafkaConsumer#commitAsync
    */
  def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /**
    * Commit the specified offsets for the specified list of topics and partitions.<br><br>
    *
    * This commits offsets to Kafka. The offsets committed using this API will be used on the first
    * fetch after every rebalance and also on startup. As such, if you need to store offsets in
    * anything other than Kafka, this API should not be used. The committed offset should be the
    * next message your application will consume, i.e. lastProcessedMessageOffset + 1. If automatic
    * group management with subscribe is used, then the committed offsets must belong to the
    * currently auto-assigned partitions.<br><br>
    *
    * Despite of it's name, this method is not blocking. But it's based on a blocking
    * org.apache.kafka.clients.consumer.KafkaConsumer#commitSync method.<br><br>
    *
    * Note, that the recommended way for committing offsets in fs2-kafka is to use `commit` on
    * [[CommittableConsumerRecord]], [[CommittableOffset]] or [[CommittableOffsetBatch]].
    * [[commitAsync]] and [[commitSync]] usually needs only for some custom scenarios.
    *
    * @param offsets
    *   A map of offsets by partition with associated metadata
    * @see
    *   org.apache.kafka.clients.consumer.KafkaConsumer#commitSync
    */
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

}
