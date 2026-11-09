/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Eq
import cats.Show
import cats.syntax.all.*
import cats.~>
import fs2.Chunk
import fs2.kafka.instances.*
import fs2.kafka.internal.Acknowledgement
import fs2.kafka.internal.Acknowledger
import org.apache.kafka.common.TopicPartition

/**
  * The offset of a record held by a [[KafkaShareConsumer]], with the delivery it belongs to.
  */
sealed abstract class AcknowledgeableOffset[F[_]] { self =>

  /**
    * The topic and partition of the record.
    */
  def topicPartition: TopicPartition

  /**
    * The offset for the topic and partition.
    */
  def offset: Long

  /**
    * How many times the record has been delivered, including this delivery.
    */
  def deliveryCount: Short

  private[kafka] def acknowledger: Acknowledger[F]

  /**
    * Records the acknowledgement in the consumer, as Java's `acknowledge` does. It is sent with the
    * next poll or renewal, after the [[ShareConsumerSettings.maxAcknowledgementDelay]], at once
    * while paused, or when consuming ends; until then, a later acknowledgement replaces it. An
    * acknowledgement after it was sent, or for a delivery the consumer no longer holds, fails with
    * a [[RecordNotInFlightException]].<br><br>
    *
    * A `Renew` is sent at once and doesn't wait for the broker: an acknowledgement made meanwhile
    * is kept and sent once the record is back, and if the broker rejects the renewal, the record is
    * delivered again.
    */
  final def acknowledge(acknowledgeType: AcknowledgeType): F[Unit] =
    acknowledger.acknowledge(Chunk.singleton(ack(acknowledgeType)))

  /**
    * Acknowledges with `Accept`: the record was processed. See [[acknowledge]].
    */
  final def accept: F[Unit] = acknowledge(AcknowledgeType.Accept)

  /**
    * Acknowledges with `Release`: the record is delivered again. See [[acknowledge]].
    */
  final def release: F[Unit] = acknowledge(AcknowledgeType.Release)

  /**
    * Acknowledges with `Reject`: the record is not delivered again. See [[acknowledge]].
    */
  final def reject: F[Unit] = acknowledge(AcknowledgeType.Reject)

  /**
    * Renews the record's acquisition lock; it needs another acknowledgement later. See
    * [[acknowledge]].
    */
  final def renew: F[Unit] = acknowledge(AcknowledgeType.Renew)

  private[kafka] def ack(acknowledgeType: AcknowledgeType): Acknowledgement =
    Acknowledgement(topicPartition -> offset, deliveryCount, acknowledgeType)

  /**
    * Creates a new [[AcknowledgeableOffset]] in which the effect type has been changed using the
    * specified `FunctionK`.
    */
  final def mapK[G[_]](f: F ~> G): AcknowledgeableOffset[G] =
    AcknowledgeableOffset(topicPartition, offset, deliveryCount, Acknowledger.mapK(acknowledger, f))

}

object AcknowledgeableOffset {

  final private[this] case class AcknowledgeableOffsetImpl[F[_]](
    override val topicPartition: TopicPartition,
    override val offset: Long,
    override val deliveryCount: Short,
    override private[kafka] val acknowledger: Acknowledger[F]
  ) extends AcknowledgeableOffset[F] {

    override def toString: String =
      s"AcknowledgeableOffset(${topicPartition.show} -> $offset, deliveryCount = ${deliveryCount.toInt})"

  }

  private[kafka] def apply[F[_]](
    topicPartition: TopicPartition,
    offset: Long,
    deliveryCount: Short,
    acknowledger: Acknowledger[F]
  ): AcknowledgeableOffset[F] =
    AcknowledgeableOffsetImpl(topicPartition, offset, deliveryCount, acknowledger)

  implicit def acknowledgeableOffsetEq[F[_]]: Eq[AcknowledgeableOffset[F]] =
    Eq.fromUniversalEquals

  implicit def acknowledgeableOffsetShow[F[_]]: Show[AcknowledgeableOffset[F]] =
    Show.fromToString

}
