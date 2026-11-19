/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import fs2.kafka.internal.converters.collection.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.FeatureUpdate
import org.apache.kafka.clients.admin.UpdateFeaturesOptions
import org.apache.kafka.common.config.ConfigResource
import org.scalatest.Assertion
import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global

abstract class ShareConsumerBaseKafkaSpec extends BaseKafkaSpec {

  // The shortest lock the broker allows, set on each share group, to keep the lock tests short.
  protected lazy val lock: FiniteDuration = 15.seconds

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ShareConsumerBaseKafkaSpec.enableShareGroups
  }

  protected def withAdmin[A](f: Admin => A): A = {
    val admin = Admin.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava
    )
    try f(admin)
    finally admin.close()
  }

  // Settings for a new share group, which reads topics from the beginning, with the shortest lock.
  protected def shareConsumerSettings: ShareConsumerSettings[IO, String, String] = {
    val group = s"share-group-${UUID.randomUUID()}"
    withAdmin { admin =>
      val operations = List(
        "share.auto.offset.reset" -> "earliest",
        "share.record.lock.duration.ms" -> lock.toMillis.toString
      ).map { case (key, value) =>
        new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)
      }
      admin
        .incrementalAlterConfigs(
          Map(
            new ConfigResource(ConfigResource.Type.GROUP, group) ->
              (operations.asJava: java.util.Collection[AlterConfigOp])
          ).asJava
        )
        .all()
        .get(20, TimeUnit.SECONDS)
    }
    ShareConsumerSettings[IO, String, String]
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      .withCommitTimeout(5.seconds)
  }

  protected def publish(topic: String, values: String*): Unit =
    sendRecords(topic, values.map(value => value -> value))

  protected def run[A](io: IO[A]): A = io.timeout(2.minutes).unsafeRunSync()

  // Checks that the topic has no records left to deliver: a new record is the next one read.
  protected def assertSettled(
    topic: String,
    settings: ShareConsumerSettings[IO, String, String]
  ): Assertion = {
    publish(topic, "after")
    readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
  }

  // Accepts every record up to and including `marker`, and the rest of its poll, returning values
  // and delivery counts.
  protected def readUntil(
    topic: String,
    settings: ShareConsumerSettings[IO, String, String],
    marker: String
  ): Vector[(String, Int)] =
    run(
      Ref
        .of[IO, Vector[(String, Int)]](Vector.empty)
        .flatMap { seen =>
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_.traverse_ { r =>
                  r.accept *> seen
                    .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                    .flatMap(s => consumer.stopConsuming.whenA(s.exists(_._1 == marker)))
                })
            } *> seen.get
        }
    )

}

private object ShareConsumerBaseKafkaSpec {

  // Share groups are enabled by the share.version feature, which may be off on a new cluster: once
  // for the shared broker.
  lazy val enableShareGroups: Unit = {
    val admin = Admin.create(
      Map[String, AnyRef](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> BaseKafkaSpec.bootstrapServers
      ).asJava
    )
    try {
      val features = admin.describeFeatures().featureMetadata().get(20, TimeUnit.SECONDS)
      val maxLevel = features.supportedFeatures().asScala("share.version").maxVersion()
      if (
        features
          .finalizedFeatures()
          .asScala
          .get("share.version")
          .forall(_.maxVersionLevel() < maxLevel)
      )
        admin
          .updateFeatures(
            Map(
              "share.version" -> new FeatureUpdate(maxLevel, FeatureUpdate.UpgradeType.UPGRADE)
            ).asJava,
            new UpdateFeaturesOptions
          )
          .all()
          .get(20, TimeUnit.SECONDS): Unit
    } finally admin.close()
  }

}
