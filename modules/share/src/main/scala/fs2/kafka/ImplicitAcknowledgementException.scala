/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * [[ImplicitAcknowledgementException]] indicates `consume` on a share consumer in implicit
  * acknowledgement mode, where records cannot be acknowledged: `KafkaShareConsumer.autoConsume`
  * accepts a poll once `process` returned instead.
  */
sealed abstract class ImplicitAcknowledgementException
  extends KafkaException(
    "records cannot be acknowledged with share.acknowledgement.mode=implicit: use autoConsume"
  )

private[kafka] object ImplicitAcknowledgementException {

  def apply(): ImplicitAcknowledgementException =
    new ImplicitAcknowledgementException {

      override def toString: String =
        s"fs2.kafka.ImplicitAcknowledgementException: $getMessage"

    }

}
