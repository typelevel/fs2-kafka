/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.Sync
import com.github.dockerjava.api.exception.NotFoundException
import fs2.kafka.internal.converters.collection.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer as KConsumer
import org.apache.kafka.clients.producer.KafkaProducer as KProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord as KProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.images.RemoteDockerImage
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*

abstract class BaseKafkaSpec extends BaseAsyncSpec with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    val _ = bootstrapServers
    super.beforeAll()
  }

  final def bootstrapServers: String =
    BaseKafkaSpec.bootstrapServers

  final val transactionTimeoutInterval: FiniteDuration =
    BaseKafkaSpec.transactionAbortCleanupInterval

  final val testGroupId: String =
    s"test-group-${UUID.randomUUID()}"

  final def adminClientSettings: AdminClientSettings =
    AdminClientSettings(bootstrapServers = bootstrapServers)

  final def defaultConsumerProperties: Map[String, String] =
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> testGroupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

  private def defaultProducerProperties: Map[String, String] =
    Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ProducerConfig.MAX_BLOCK_MS_CONFIG -> "10000",
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "1000",
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "false"
    )

  final def consumerSettings[F[_]](implicit F: Sync[F]): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withProperties(defaultConsumerProperties)
      .withRecordMetadata(_.timestamp.toString)

  final def producerSettings[F[_]](implicit F: Sync[F]): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String].withProperties(defaultProducerProperties)

  final def producerSettingsTransactional[F[_]](implicit
    F: Sync[F]
  ): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String]
      .withProperties(defaultProducerProperties)
      .withEnableIdempotence(true)

  final def withTopic[A](f: String => A): A =
    f(s"topic-${UUID.randomUUID()}")

  final def eventually[A](check: IO[A], attempts: Int = 100): IO[A] =
    check.handleErrorWith { error =>
      if (attempts > 1) IO.sleep(100.millis) >> eventually(check, attempts - 1)
      else IO.raiseError(error)
    }

  final def withKafkaConsumer(
    nativeSettings: Map[String, AnyRef]
  ): WithKafkaConsumer =
    new WithKafkaConsumer(nativeSettings)

  final class WithKafkaConsumer(
    nativeSettings: Map[String, AnyRef]
  ) {

    def apply[A](f: KConsumer[Array[Byte], Array[Byte]] => A): A = {
      val consumer: KConsumer[Array[Byte], Array[Byte]] =
        new KConsumer[Array[Byte], Array[Byte]](
          nativeSettings.asJava,
          new ByteArrayDeserializer,
          new ByteArrayDeserializer
        )

      try f(consumer)
      finally consumer.close()
    }

  }

  final def createTopic(topic: String, partitions: Int = 1): Unit = {
    val admin = Admin.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava
    )

    try {
      val newTopic = new NewTopic(topic, partitions, 1.toShort)
      val _ = admin.createTopics(List(newTopic).asJava).all.get(10, TimeUnit.SECONDS)
    } finally admin.close()
  }

  final def sendRecords(topic: String, records: Seq[(String, String)]): Unit =
    sendRecords(records.map { case (key, value) => ProducerRecord(topic, key, value) })

  final def sendRecords(records: Seq[ProducerRecord[String, String]]): Unit = {
    val producer = new KProducer[String, String](
      (defaultProducerProperties: Map[String, AnyRef]).asJava,
      new StringSerializer,
      new StringSerializer
    )

    try {
      val sent = records.map { record =>
        val partition = record.partition.map(Int.box).orNull
        producer.send(new KProducerRecord(record.topic, partition, record.key, record.value))
      }

      sent.foreach(_.get(10, TimeUnit.SECONDS))
    } finally producer.close()
  }

  final def readRecords(
    topic: String,
    count: Int,
    readCommitted: Boolean = false
  ): List[(String, String)] = {
    val isolationLevel = if (readCommitted) "read_committed" else "read_uncommitted"

    val consumer = new KConsumer[String, String](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ISOLATION_LEVEL_CONFIG -> isolationLevel
      ).asJava,
      new StringDeserializer,
      new StringDeserializer
    )

    try {
      val partitions = consumer
        .partitionsFor(topic)
        .asScala
        .map { info =>
          new TopicPartition(info.topic, info.partition)
        }

      consumer.assign(partitions.asJava)

      val records = ListBuffer.empty[(String, String)]
      val deadline = 10.seconds.fromNow

      while (records.size < count && deadline.hasTimeLeft()) {
        consumer
          .poll(java.time.Duration.ofMillis(100))
          .asScala
          .foreach { record =>
            records += (record.key -> record.value)
          }
      }

      if (records.size < count)
        throw new TimeoutException(
          s"Read ${records.size} of $count records from $topic before timing out"
        )

      records.take(count).toList
    } finally consumer.close()
  }

}

private object BaseKafkaSpec {

  val transactionAbortCleanupInterval: FiniteDuration = 1.second

  /** Shared broker by all test suites. Cleaned up by Testcontainers when the JVM exits. */
  lazy val bootstrapServers: String = {
    val container =
      new KafkaContainer(kafkaImage)
        .withEnv(
          "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
          transactionAbortCleanupInterval.toMillis.toString
        )
        .withEnv(
          "KAFKA_AUTHORIZER_CLASS_NAME",
          "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
        )
        .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")

    container.start()
    container.getBootstrapServers
  }

  private def kafkaImage: DockerImageName = {
    val image =
      DockerImageName.parse(
        sys.props
          .getOrElse(
            "fs2.kafka.test.image",
            sys.error("fs2.kafka.test.image is not set, run the tests via sbt")
          )
      )

    try {
      val _ = new RemoteDockerImage(image).get()
      image
    } catch {
      case e: Exception if isImageNotFound(e) =>
        val fallback = image.withTag("latest")
        println(s"$image is not available, running Kafka tests against $fallback instead")
        fallback
    }
  }

  private def isImageNotFound(e: Throwable): Boolean =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[NotFoundException])

}
