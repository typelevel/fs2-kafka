/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.~>
import fs2.Chunk
import fs2.kafka.AcknowledgeType
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import org.apache.kafka.common.TopicPartition

/**
  * An acknowledgement for one delivery of a record.
  */
final private[kafka] case class Acknowledgement(
  offset: TopicPartitionOffset,
  deliveryCount: Short,
  acknowledgeType: AcknowledgeType
)

private[kafka] object Acknowledgement {

  /**
    * A record's offset in its topic partition, the same for every delivery of the record.
    */
  type TopicPartitionOffset = (TopicPartition, Long)

}

/**
  * Gives acknowledgements to the consumer holding their records.<br><br>
  *
  * Acknowledgers are equal when they share a `source`, so that `acknowledgeChunks` can acknowledge
  * a run of records through one of them. The consumer records every acknowledgement of a chunk
  * before raising a `RecordNotInFlightException` for the first one it doesn't hold.
  */
private[kafka] trait Acknowledger[F[_]] {

  def acknowledge(acks: Chunk[Acknowledgement]): F[Unit]

  /**
    * The acknowledger this one was derived from with `mapK`, or this one.
    */
  def source: AnyRef = this

  final override def equals(that: Any): Boolean =
    that match {
      case that: Acknowledger[_] => source eq that.source
      case _ => false
    }

  final override def hashCode(): Int =
    System.identityHashCode(source)

}

private[kafka] object Acknowledger {

  final private class Mapped[F[_], G[_]](underlying: Acknowledger[F], f: F ~> G) extends Acknowledger[G] {

    override def acknowledge(acks: Chunk[Acknowledgement]): G[Unit] =
      f(underlying.acknowledge(acks))

    override val source: AnyRef = underlying.source

    override def toString: String = underlying.toString

  }

  def mapK[F[_], G[_]](acknowledger: Acknowledger[F], f: F ~> G): Acknowledger[G] =
    new Mapped(acknowledger, f)

}
