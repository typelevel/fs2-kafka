/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Eq
import cats.Show
import org.apache.kafka.clients.consumer.AcknowledgeType as KafkaAcknowledgeType

/**
  * The type of acknowledgement of a share consumer record, matching Java's `AcknowledgeType`.
  * `Accept`, `Release` and `Reject` settle a delivery, while `Renew` extends its acquisition lock
  * and leaves it to be settled later.
  */
sealed abstract class AcknowledgeType {

  /**
    * The corresponding Java `AcknowledgeType`.
    */
  def asJava: KafkaAcknowledgeType

}

object AcknowledgeType {

  private[kafka] case object AcceptAcknowledgeType extends AcknowledgeType {

    override val asJava: KafkaAcknowledgeType = KafkaAcknowledgeType.ACCEPT
    override def toString: String = "Accept"

  }

  private[kafka] case object ReleaseAcknowledgeType extends AcknowledgeType {

    override val asJava: KafkaAcknowledgeType = KafkaAcknowledgeType.RELEASE
    override def toString: String = "Release"

  }

  private[kafka] case object RejectAcknowledgeType extends AcknowledgeType {

    override val asJava: KafkaAcknowledgeType = KafkaAcknowledgeType.REJECT
    override def toString: String = "Reject"

  }

  private[kafka] case object RenewAcknowledgeType extends AcknowledgeType {

    override val asJava: KafkaAcknowledgeType = KafkaAcknowledgeType.RENEW
    override def toString: String = "Renew"

  }

  /**
    * The record was consumed successfully.
    */
  val Accept: AcknowledgeType = AcceptAcknowledgeType

  /**
    * The record was not consumed successfully. Release it for another delivery attempt.
    */
  val Release: AcknowledgeType = ReleaseAcknowledgeType

  /**
    * The record was not consumed successfully. Reject it and do not release it for another delivery
    * attempt.
    */
  val Reject: AcknowledgeType = RejectAcknowledgeType

  /**
    * The record is still being processed. Renew the acquisition lock so processing can continue.
    */
  val Renew: AcknowledgeType = RenewAcknowledgeType

  implicit val acknowledgeTypeEq: Eq[AcknowledgeType] =
    Eq.fromUniversalEquals

  implicit val acknowledgeTypeShow: Show[AcknowledgeType] =
    Show.fromToString

}
