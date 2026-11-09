/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import fs2.kafka.internal.ShareConsumerConfig
import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global

final class ShareConsumerConfigSpec extends BaseSpec {

  private val settings = ShareConsumerSettings[IO, String, String]

  private def config(s: ShareConsumerSettings[IO, String, String]) =
    ShareConsumerConfig.from(s).unsafeRunSync()

  describe("ShareConsumerConfig") {
    it("should read the library's defaults") {
      val c = config(settings)
      assert(!c.implicitMode)
      assert(c.maxPollInterval == 5.minutes && c.apiTimeout == 1.minute)
    }

    it("should read the mode as Java does") {
      val c = config(settings.withProperty("share.acknowledgement.mode", " implicit\n"))
      assert(c.implicitMode)
    }

    it("should read the poll and API timeouts") {
      val c = config(
        settings
          .withProperty("max.poll.interval.ms", " 1000 ")
          .withProperty("default.api.timeout.ms", "2000")
      )
      assert(c.maxPollInterval == 1.second && c.apiTimeout == 2.seconds)
    }

    it("should renew the margin before the lock ends, or at half the lock") {
      val c = config(settings)
      assert(c.renewalInterval(Some(30.seconds)) == 25.seconds)
      assert(
        config(settings.withRenewalMargin(40.seconds)).renewalInterval(Some(30.seconds)) == 15.seconds
      )
      assert(c.renewalInterval(None) == ShareConsumerConfig.unknownLockRenewalInterval)
    }

    it("should renew within half of max.poll.interval.ms") {
      val c = config(settings.withProperty("max.poll.interval.ms", "20000"))
      assert(c.renewalInterval(Some(30.seconds)) == 10.seconds)
      assert(config(settings.withProperty("max.poll.interval.ms", "800")).renewalInterval(None) == 400.millis)
    }

    it("should leave a value Java rejects to Java") {
      val c = config(settings.withProperty("max.poll.interval.ms", "5m"))
      assert(c.maxPollInterval == 5.minutes)
    }
  }

}
