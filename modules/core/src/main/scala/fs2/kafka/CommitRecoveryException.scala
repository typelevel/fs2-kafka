/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.instances.list.*
import cats.syntax.show.*
import fs2.kafka.instances.*
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

/**
  * [[CommitRecoveryException]] indicates that offset commit recovery was attempted `attempts` times
  * for `offsets`, but that it wasn't able to complete successfully. The last encountered exception
  * is provided as `lastException`.<br><br>
  *
  * Use [[CommitRecoveryException#apply]] to create a new instance.
  */
sealed abstract class CommitRecoveryException(
  attempts: Int,
  lastException: Throwable,
  offsets: Map[TopicPartition, OffsetAndMetadata]
) extends KafkaException({
      offsets
        .toList
        .sorted
        .mkStringAppend { case (append, (tp, oam)) =>
          append(tp.show)
          append(" -> ")
          append(oam.show)
        }(
          start =
            s"offset commit is still failing after $attempts attempts${if (offsets.nonEmpty) " for offsets: "
              else ""}",
          sep = ", ",
          end = s"; last exception was: $lastException"
        )
    })

object CommitRecoveryException {

  /**
    * Creates a new [[CommitRecoveryException]] indicating that offset commit recovery was attempted
    * `attempts` times for `offsets` but that it wasn't able to complete successfully. The last
    * exception encountered was `lastException`.
    */
  def apply(
    attempts: Int,
    lastException: Throwable,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): CommitRecoveryException =
    new CommitRecoveryException(attempts, lastException, offsets) {

      override def toString: String =
        s"fs2.kafka.CommitRecoveryException: $getMessage"

    }

}
