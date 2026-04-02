/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * [[UnexpectedTopicException]] is raised when serialization or deserialization occurred for an
  * unexpected topic which isn't supported by the [[Serializer]] or [[Deserializer]].
  */
sealed abstract class UnexpectedTopicException(topic: String)
    extends KafkaException(s"unexpected topic [$topic]")

private[kafka] object UnexpectedTopicException {

  def apply(topic: String): UnexpectedTopicException =
    new UnexpectedTopicException(topic) {

      override def toString: String =
        s"fs2.kafka.UnexpectedTopicException: $getMessage"

    }

}
