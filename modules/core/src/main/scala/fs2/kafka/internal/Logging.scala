/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Sync
import cats.syntax.all.*

import org.slf4j.LoggerFactory

sealed abstract private[kafka] class Logging[F[_]] {
  def log(entry: LogEntry): F[Unit]
}

private[kafka] object Logging {

  def default[F[_]](id: Int)(implicit F: Sync[F]): F[Logging[F]] =
    Logging.create[F]("fs2.kafka.KafkaConsumer$" + id)

  def create[F[_]](name: String)(implicit F: Sync[F]): F[Logging[F]] =
    F.delay(LoggerFactory.getLogger(name))
      .map { logger =>
        new Logging[F] {

          override def log(entry: LogEntry): F[Unit] =
            F.delay {
              entry.level match {
                case LogLevel.Error =>
                  if (logger.isErrorEnabled)
                    logger.error(entry.message)
                case LogLevel.Warn =>
                  if (logger.isWarnEnabled)
                    logger.warn(entry.message)
                case LogLevel.Info =>
                  if (logger.isInfoEnabled)
                    logger.info(entry.message)
                case LogLevel.Debug =>
                  if (logger.isDebugEnabled)
                    logger.debug(entry.message)
              }
            }

        }
      }

}
