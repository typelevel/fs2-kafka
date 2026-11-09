/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

/**
  * How the share consumer acquires records, the `share.acquire.mode` property, set with
  * [[ShareConsumerSettings.withAcquireMode]].
  */
sealed abstract class ShareAcquireMode

object ShareAcquireMode {

  private[kafka] case object RecordLimitAcquire extends ShareAcquireMode {
    override def toString: String = "RecordLimit"
  }

  private[kafka] case object BatchOptimizedAcquire extends ShareAcquireMode {
    override def toString: String = "BatchOptimized"
  }

  /**
    * A poll returns at most `max.poll.records` records. The default here.
    */
  val RecordLimit: ShareAcquireMode = RecordLimitAcquire

  /**
    * A poll may return more than `max.poll.records` records, to align with the broker's record
    * batches. Java's default.
    */
  val BatchOptimized: ShareAcquireMode = BatchOptimizedAcquire

}
