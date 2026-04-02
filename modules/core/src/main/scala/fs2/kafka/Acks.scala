/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

/**
  * The available options for [[ProducerSettings#withAcks]].<br><br>
  *
  * Available options include:<br>
  *   - [[Acks#Zero]] to not wait for any acknowledgement from the server,<br>
  *   - [[Acks#One]] to only wait for acknowledgement from the leader node,<br>
  *   - [[Acks#All]] to wait for acknowledgement from all in-sync replicas.
  */
sealed abstract class Acks

object Acks {

  private[kafka] case object ZeroAcks extends Acks {
    override def toString: String = "Zero"
  }

  private[kafka] case object OneAcks extends Acks {
    override def toString: String = "One"
  }

  private[kafka] case object AllAcks extends Acks {
    override def toString: String = "All"
  }

  /**
    * Option to not wait for any acknowledgement from the server when producing records.
    */
  val Zero: Acks = ZeroAcks

  /**
    * Option to only wait for acknowledgement from the leader node when producing records.
    */
  val One: Acks = OneAcks

  /**
    * Option to wait for acknowledgement from all in-sync replicas when producing records.
    */
  val All: Acks = AllAcks

}
