/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * [[NotSubscribedException]] indicates that a `Stream` was started in [[KafkaConsumer]] even
  * though the consumer had not been subscribed to any topics or assigned any partitions before
  * starting.
  */
sealed abstract class NotSubscribedException
    extends KafkaException("consumer is not subscribed to any topics or assigned any partitions")

private[kafka] object NotSubscribedException {

  def apply(): NotSubscribedException =
    new NotSubscribedException {

      override def toString: String =
        s"fs2.kafka.NotSubscribedException: $getMessage"

    }

}
