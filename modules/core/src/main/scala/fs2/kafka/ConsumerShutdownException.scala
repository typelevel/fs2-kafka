/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * [[ConsumerShutdownException]] indicates that a request could not be completed because the
  * consumer has already shutdown.
  */
sealed abstract class ConsumerShutdownException
    extends KafkaException("consumer has already shutdown")

private[kafka] object ConsumerShutdownException {

  def apply(): ConsumerShutdownException =
    new ConsumerShutdownException {

      override def toString: String =
        s"fs2.kafka.ConsumerShutdownException: $getMessage"

    }

}
