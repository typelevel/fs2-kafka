/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * Exception raised with [[Deserializer#failWith]] when deserialization was unable to complete
  * successfully.
  */
sealed abstract class DeserializationException(message: String) extends KafkaException(message)

private[kafka] object DeserializationException {

  def apply(message: String): DeserializationException =
    new DeserializationException(message) {

      override def toString: String =
        s"fs2.kafka.DeserializationException: $getMessage"

    }

}
