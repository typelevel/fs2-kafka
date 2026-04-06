/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Eq
import cats.Show

import org.apache.kafka.common.record.TimestampType

/**
  * [[Timestamp]] is an optional timestamp value.
  */
sealed abstract class Timestamp {

  /**
    * Returns the timestamp value if it is present; otherwise `None`.
    */
  def toOption: Option[Long]

  /**
    * Returns the timestamp type if it is present; otherwise `None`.
    */
  def timestampType: Option[TimestampType]

  /**
    * Returns `true` if there is no timestamp value; otherwise `false`.
    */
  def isEmpty: Boolean

  /**
    * Returns `true` if there is a timestamp value; otherwise `false`.
    */
  final def nonEmpty: Boolean =
    !isEmpty

}

object Timestamp {

  /**
    * Creates a new [[Timestamp]] instance from the specified timestamp value representing the time
    * when the record was created.
    */
  def createTime(value: Long): Timestamp =
    CreateTime(value)

  /**
    * Creates a new [[Timestamp]] instance from the specified timestamp value representing the time
    * when the record was appended to the log.
    */
  def logAppendTime(value: Long): Timestamp =
    LogAppendTime(value)

  /**
    * Creates a new [[Timestamp]] instance from the specified timestamp value, when the timestamp
    * type is unknown.
    */
  def unknownTime(value: Long): Timestamp =
    UnknownTime(value)

  /**
    * The [[Timestamp]] instance without any timestamp values.
    */
  val none: Timestamp =
    NoTimestamp

  final case class CreateTime(value: Long) extends Timestamp {

    override val toOption: Option[Long] = Some(value)

    override val timestampType: Option[TimestampType] =
      Some(TimestampType.CREATE_TIME)

    override val isEmpty: Boolean = false

  }

  final case class LogAppendTime(value: Long) extends Timestamp {

    override val toOption: Option[Long] = Some(value)

    override val timestampType: Option[TimestampType] =
      Some(TimestampType.LOG_APPEND_TIME)

    override val isEmpty: Boolean = false

  }

  final case class UnknownTime(value: Long) extends Timestamp {

    override val toOption: Option[Long] = Some(value)

    override val timestampType: Option[TimestampType] =
      Some(TimestampType.NO_TIMESTAMP_TYPE)

    override val isEmpty: Boolean = false

  }

  case object NoTimestamp extends Timestamp {

    override val toOption: Option[Long] = None

    override val timestampType: Option[TimestampType] = None

    override val isEmpty: Boolean = true

  }

  implicit val timestampEq: Eq[Timestamp] =
    Eq.fromUniversalEquals

  implicit val timestampShow: Show[Timestamp] =
    Show.fromToString

}
