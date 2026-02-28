/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

/**
  * The available options for [[ConsumerSettings#withIsolationLevel]].<br><br>
  *
  * Available options include:<br>
  *   - [[IsolationLevel#ReadCommitted]] to only read committed records,<br>
  *   - [[IsolationLevel#ReadUncommitted]] to also read uncommitted records.
  */
sealed abstract class IsolationLevel

object IsolationLevel {

  private[kafka] case object ReadCommittedIsolationLevel extends IsolationLevel {
    override def toString: String = "ReadCommitted"
  }

  private[kafka] case object ReadUncommittedIsolationLevel extends IsolationLevel {
    override def toString: String = "ReadUncommitted"
  }

  /**
    * Option to only read committed records.
    */
  val ReadCommitted: IsolationLevel = ReadCommittedIsolationLevel

  /**
    * Option to read both committed and uncommitted records.
    */
  val ReadUncommitted: IsolationLevel = ReadUncommittedIsolationLevel

}
