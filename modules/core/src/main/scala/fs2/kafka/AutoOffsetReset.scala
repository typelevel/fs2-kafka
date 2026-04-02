/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

/**
  * The available options for [[ConsumerSettings#withAutoOffsetReset]].<br><br>
  *
  * Available options include:<br>
  *   - [[AutoOffsetReset#Earliest]] to reset to the earliest offsets,<br>
  *   - [[AutoOffsetReset#Latest]] to reset to the latest offsets,<br>
  *   - [[AutoOffsetReset#None]] to fail if no offsets are available.
  */
sealed abstract class AutoOffsetReset

object AutoOffsetReset {

  private[kafka] case object EarliestOffsetReset extends AutoOffsetReset {
    override def toString: String = "Earliest"
  }

  private[kafka] case object LatestOffsetReset extends AutoOffsetReset {
    override def toString: String = "Latest"
  }

  private[kafka] case object NoneOffsetReset extends AutoOffsetReset {
    override def toString: String = "None"
  }

  /**
    * Option to reset to the earliest available offsets if no initial or current offsets exist for
    * the consumer group.
    */
  val Earliest: AutoOffsetReset = EarliestOffsetReset

  /**
    * Option to reset to the latest available offsets if no initial or current offsets exist for the
    * consumer group.
    */
  val Latest: AutoOffsetReset = LatestOffsetReset

  /**
    * Option to fail the consumer if there are no offsets available for the consumer group.
    */
  val None: AutoOffsetReset = NoneOffsetReset

}
