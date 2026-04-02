/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

sealed abstract private[kafka] class LogLevel

private[kafka] object LogLevel {

  case object Error extends LogLevel
  case object Warn  extends LogLevel
  case object Info  extends LogLevel
  case object Debug extends LogLevel

}
