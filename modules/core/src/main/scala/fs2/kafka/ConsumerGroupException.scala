/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * Indicates that one or more of the following conditions occurred while attempting to commit
  * offsets.<br><br>
  *   - There were [[CommittableOffset]]s without a consumer group ID.<br>
  *   - There were [[CommittableOffset]]s for multiple consumer group IDs.
  */
sealed abstract class ConsumerGroupException(groupIds: Set[String])
    extends KafkaException({
      val groupIdsString = groupIds.toList.sorted.mkString(", ")
      s"multiple or missing consumer group ids [$groupIdsString]"
    })

private[kafka] object ConsumerGroupException {

  def apply(groupIds: Set[String]): ConsumerGroupException =
    new ConsumerGroupException(groupIds) {

      override def toString: String =
        s"fs2.kafka.ConsumerGroupException: $getMessage"

    }

}
