/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Sync
import fs2.kafka.ShareConsumerSettings
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.concurrent.duration.*
import scala.util.Try

/**
  * What a share consumer reads from its settings and the Java properties, resolved once.
  *
  * @param implicitMode
  *   whether Java accepts a poll's records with the next poll
  */
final private[kafka] case class ShareConsumerConfig[F[_], K, V](
  settings: ShareConsumerSettings[F, K, V],
  maxPollInterval: FiniteDuration,
  apiTimeout: FiniteDuration,
  implicitMode: Boolean
) {

  /**
    * The lock minus the renewal margin, or half the lock, within half of `max.poll.interval.ms`.
    */
  def renewalInterval(lock: Option[FiniteDuration]): FiniteDuration =
    lock
      .fold(ShareConsumerConfig.unknownLockRenewalInterval) { lock =>
        if (settings.renewalMargin < lock) lock - settings.renewalMargin else lock / 2
      }
      .min(maxPollInterval / 2)

}

private[kafka] object ShareConsumerConfig {

  /**
    * Half of the shortest lock a broker allows, until the broker reports the lock.
    */
  val unknownLockRenewalInterval: FiniteDuration = 500.millis

  def from[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Sync[F]): F[ShareConsumerConfig[F, K, V]] =
    F.delay {
      // A value Java rejects is left to it, so that its error names the property.
      def property[A](key: String, default: A)(parse: String => A): A =
        settings.properties.get(key).flatMap(v => Try(parse(v.trim)).toOption).getOrElse(default)

      val implicitMode =
        property(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, true)(
          _ == "implicit"
        )

      ShareConsumerConfig(
        settings = settings,
        maxPollInterval = property(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000L)(_.toLong).millis,
        apiTimeout = property(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000L)(_.toLong).millis,
        implicitMode = implicitMode
      )
    }

}
