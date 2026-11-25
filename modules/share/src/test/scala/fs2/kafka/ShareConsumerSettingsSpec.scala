/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import fs2.concurrent.Signal
import fs2.kafka.security.KafkaCredentialStore
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global

final class ShareConsumerSettingsSpec extends BaseSpec {

  describe("ShareConsumerSettings") {
    it("should default to explicit acknowledgement and record_limit acquisition") {
      settings.properties shouldBe Map(
        ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG -> "explicit",
        ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG -> "record_limit"
      )
    }

    it("should provide withBootstrapServers") {
      settings
        .withBootstrapServers("localhost")
        .properties(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) shouldBe "localhost"
    }

    it("should provide withClientId") {
      settings.withClientId("client").properties(ConsumerConfig.CLIENT_ID_CONFIG) shouldBe "client"
    }

    it("should provide withGroupId") {
      settings.withGroupId("group").properties(ConsumerConfig.GROUP_ID_CONFIG) shouldBe "group"
    }

    it("should provide withMaxPollRecords") {
      settings.withMaxPollRecords(10).properties(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) shouldBe "10"
    }

    it("should provide withAcknowledgementMode") {
      settings
        .withAcknowledgementMode(ShareAcknowledgementMode.Implicit)
        .properties(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG) shouldBe "implicit"
      settings
        .withAcknowledgementMode(ShareAcknowledgementMode.Explicit)
        .properties(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG) shouldBe "explicit"
    }

    it("should provide withAcquireMode") {
      settings
        .withAcquireMode(ShareAcquireMode.BatchOptimized)
        .properties(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG) shouldBe "batch_optimized"
      settings
        .withAcquireMode(ShareAcquireMode.RecordLimit)
        .properties(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG) shouldBe "record_limit"
    }

    it("should provide withMaxPollInterval") {
      settings
        .withMaxPollInterval(1.minute)
        .properties(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG) shouldBe "60000"
    }

    it("should provide withRequestTimeout") {
      settings
        .withRequestTimeout(10.seconds)
        .properties(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG) shouldBe "10000"
    }

    it("should provide withDefaultApiTimeout") {
      settings
        .withDefaultApiTimeout(20.seconds)
        .properties(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG) shouldBe "20000"
    }

    it("should provide withAllowAutoCreateTopics") {
      settings
        .withAllowAutoCreateTopics(false)
        .properties(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG) shouldBe "false"
    }

    it("should provide withClientRack") {
      settings.withClientRack("rack").properties(ConsumerConfig.CLIENT_RACK_CONFIG) shouldBe "rack"
    }

    it("should provide withProperty") {
      settings.withProperty("a", "b").properties("a") shouldBe "b"
    }

    it("should provide withProperties") {
      settings.withProperties("a" -> "b").properties("a") shouldBe "b"
      settings.withProperties(Map("a" -> "b")).properties("a") shouldBe "b"
    }

    it("should provide withCredentials") {
      val credentials = KafkaCredentialStore.fromPemStrings("ca", "key", "cert")

      settings.withCredentials(credentials).properties shouldBe
        settings.properties ++ credentials.properties
    }

    it("should provide withCloseTimeout") {
      settings.closeTimeout shouldBe 20.seconds
      settings.withCloseTimeout(1.second).closeTimeout shouldBe 1.second
      intercept[IllegalArgumentException](settings.withCloseTimeout((-1).second))
    }

    it("should provide withCommitTimeout") {
      settings.commitTimeout shouldBe 15.seconds
      settings.withCommitTimeout(1.second).commitTimeout shouldBe 1.second
      intercept[IllegalArgumentException](settings.withCommitTimeout(Duration.Zero))
    }

    it("should provide withPollTimeout") {
      settings.pollTimeout shouldBe 50.millis
      settings.withPollTimeout(1.second).pollTimeout shouldBe 1.second
      intercept[IllegalArgumentException](settings.withPollTimeout(Duration.Zero))
    }

    it("should provide withAutoRenewal") {
      settings.autoRenewal shouldBe false
      settings.withAutoRenewal(true).autoRenewal shouldBe true
    }

    it("should provide withRenewalMargin") {
      settings.renewalMargin shouldBe 5.seconds
      settings.withRenewalMargin(10.seconds).renewalMargin shouldBe 10.seconds
      intercept[IllegalArgumentException](settings.withRenewalMargin(Duration.Zero))
    }

    it("should provide withMaxProcessingTime") {
      settings.maxProcessingTime shouldBe None
      settings.withMaxProcessingTime(1.minute).maxProcessingTime shouldBe Some(1.minute)
      settings.withMaxProcessingTime(1.minute).withoutMaxProcessingTime.maxProcessingTime shouldBe None
      intercept[IllegalArgumentException](settings.withMaxProcessingTime(Duration.Zero))
      intercept[IllegalArgumentException](settings.withMaxProcessingTime(366.days))
    }

    it("should provide withFailureAcknowledgement") {
      settings.failureAcknowledgement shouldBe AcknowledgeType.Release
      settings
        .withFailureAcknowledgement(AcknowledgeType.Reject)
        .failureAcknowledgement shouldBe AcknowledgeType.Reject
      intercept[IllegalArgumentException](settings.withFailureAcknowledgement(AcknowledgeType.Accept))
      intercept[IllegalArgumentException](settings.withFailureAcknowledgement(AcknowledgeType.Renew))
    }

    it("should provide withMaxAcknowledgementDelay") {
      settings.maxAcknowledgementDelay shouldBe Some(1.second)
      settings.withMaxAcknowledgementDelay(Duration.Zero).maxAcknowledgementDelay shouldBe
        Some(Duration.Zero)
      settings.withoutMaxAcknowledgementDelay.maxAcknowledgementDelay shouldBe None
      intercept[IllegalArgumentException](settings.withMaxAcknowledgementDelay((-1).second))
      intercept[IllegalArgumentException](settings.withMaxAcknowledgementDelay(366.days))
    }

    it("should provide withPauseWhen") {
      val signal = Signal.constant[IO, Boolean](true)

      settings.pauseWhen shouldBe None
      settings.withPauseWhen(signal).pauseWhen shouldBe Some(signal)
      settings.withPauseWhen(signal).withoutPauseWhen.pauseWhen shouldBe None
    }

    it("should provide withDeserializers") {
      val replaced = settings.withDeserializers(
        Resource.pure[IO, KeyDeserializer[IO, Int]](Deserializer[IO, Int]),
        Resource.pure[IO, ValueDeserializer[IO, Long]](Deserializer[IO, Long])
      )

      replaced.properties shouldBe settings.properties
      replaced.closeTimeout shouldBe settings.closeTimeout
    }

    it("should be able to create with deserializers and with deserializer creation effects") {
      val deserializerInstance =
        Deserializer[IO, String].map(identity)

      implicit val deserializer: Resource[IO, Deserializer[IO, String]] =
        Resource.pure(deserializerInstance)

      ShareConsumerSettings(Deserializer[IO, String], Deserializer[IO, String])
      ShareConsumerSettings[IO, String, String].keyDeserializer
        .use(IO.pure)
        .unsafeRunSync() shouldBe deserializerInstance
    }

    it("should have a Show instance and matching toString") {
      assert {
        settings.show == "ShareConsumerSettings(closeTimeout = 20 seconds, pollTimeout = 50 milliseconds, commitTimeout = 15 seconds, autoRenewal = false, renewalMargin = 5 seconds, maxProcessingTime = None, failureAcknowledgement = Release, maxAcknowledgementDelay = Some(1 second), pauseWhen = None)" &&
        settings.show == settings.toString
      }
    }
  }

  val settings =
    ShareConsumerSettings(
      keyDeserializer = Deserializer[IO, String],
      valueDeserializer = Deserializer[IO, String]
    )

}
