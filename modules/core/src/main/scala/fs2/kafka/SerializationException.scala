/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * Exception raised with [[Serializer#failWith]] when serialization was unable to complete
  * successfully.
  */
sealed abstract class SerializationException(message: String) extends KafkaException(message)

private[kafka] object SerializationException {

  def apply(message: String): SerializationException =
    new SerializationException(message) {

      override def toString: String =
        s"fs2.kafka.SerializationException: $getMessage"

    }

}
