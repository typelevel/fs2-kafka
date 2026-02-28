/*
 * Copyright 2018-2026 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Eq, Show}
import cats.instances.boolean.*
import cats.instances.long.*
import cats.instances.option.*
import cats.syntax.eq.*

/**
  * [[Timestamp]] is an optional timestamp value representing a [[createTime]], [[logAppendTime]],
  * [[unknownTime]], or no timestamp at all.
  */
sealed abstract class Timestamp {

  /**
    * Returns the timestamp value, if the timestamp is representing the time when a record was
    * created.
    */
  def createTime: Option[Long]

  /**
    * Returns the timestamp value, if the timestamp is representing the time when a record was
    * appended to the log.
    */
  def logAppendTime: Option[Long]

  /**
    * Returns the timestamp value, if there is a timestamp, but the type is unknown.
    */
  def unknownTime: Option[Long]

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
    new Timestamp {

      override val createTime: Option[Long]    = Some(value)
      override val logAppendTime: Option[Long] = None
      override val unknownTime: Option[Long]   = None
      override val isEmpty: Boolean            = false
      override def toString: String            = s"Timestamp(createTime = $value)"

    }

  /**
    * Creates a new [[Timestamp]] instance from the specified timestamp value representing the time
    * when the record was appended to the log.
    */
  def logAppendTime(value: Long): Timestamp =
    new Timestamp {

      override val createTime: Option[Long]    = None
      override val logAppendTime: Option[Long] = Some(value)
      override val unknownTime: Option[Long]   = None
      override val isEmpty: Boolean            = false
      override def toString: String            = s"Timestamp(logAppendTime = $value)"

    }

  /**
    * Creates a new [[Timestamp]] instance from the specified timestamp value, when the timestamp
    * type is unknown.
    */
  def unknownTime(value: Long): Timestamp =
    new Timestamp {

      override val createTime: Option[Long]    = None
      override val logAppendTime: Option[Long] = None
      override val unknownTime: Option[Long]   = Some(value)
      override val isEmpty: Boolean            = false
      override def toString: String            = s"Timestamp(unknownTime = $value)"

    }

  /**
    * The [[Timestamp]] instance without any timestamp values.
    */
  val none: Timestamp =
    new Timestamp {

      override val createTime: Option[Long]    = None
      override val logAppendTime: Option[Long] = None
      override val unknownTime: Option[Long]   = None
      override val isEmpty: Boolean            = true
      override def toString: String            = "Timestamp()"

    }

  implicit val timestampShow: Show[Timestamp] =
    Show.fromToString

  implicit val timestampEq: Eq[Timestamp] =
    Eq.instance { case (l, r) =>
      l.createTime === r.createTime &&
      l.logAppendTime === r.logAppendTime &&
      l.unknownTime === r.unknownTime &&
      l.isEmpty === r.isEmpty
    }

}
