/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition

/**
  * [[RecordNotInFlightException]] indicates that an acknowledgement was for a delivery which the
  * share consumer no longer holds: its acknowledgement was already sent, `process` returned, its
  * renewal failed, the consumer acknowledged it after the max processing time, or it belongs to an
  * earlier delivery of the record. Unless its acknowledgement was sent or it got a `Reject`, the
  * record is delivered again.
  */
sealed abstract class RecordNotInFlightException(topicPartition: TopicPartition, offset: Long)
  extends KafkaException(
    s"record at offset $offset of $topicPartition is not in flight on this share consumer"
  )

private[kafka] object RecordNotInFlightException {

  def apply(topicPartition: TopicPartition, offset: Long): RecordNotInFlightException =
    new RecordNotInFlightException(topicPartition, offset) {

      override def toString: String =
        s"fs2.kafka.RecordNotInFlightException: $getMessage"

    }

}
