/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.UUID

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Try

import cats.effect.Sync
import fs2.kafka.internal.converters.collection.*

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.clients.consumer.KafkaConsumer as KConsumer
import org.apache.kafka.clients.producer.{
  KafkaProducer as KProducer,
  ProducerConfig,
  ProducerRecord as KProducerRecord
}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.Args
import org.testcontainers.utility.DockerImageName

abstract class BaseKafkaSpec extends BaseAsyncSpec with ForAllTestContainer {

  final val adminClientCloseTimeout: FiniteDuration    = 2.seconds
  final val transactionTimeoutInterval: FiniteDuration = 1.second

  final val consumerPollingTimeout: FiniteDuration     = 1.second
  protected val producerPublishTimeout: FiniteDuration = 10.seconds

  override def runTest(testName: String, args: Args) = super.runTest(testName, args)

  private val imageVersion = "7.2.0"

  private lazy val imageName = "confluentinc/cp-kafka"

  override val container: KafkaContainer =
    new KafkaContainer(DockerImageName.parse(s"$imageName:$imageVersion")).configure { container =>
      container
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv(
          "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
          transactionTimeoutInterval.toMillis.toString
        )
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
        .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")

      ()
    }

  implicit final val stringSerializer: KafkaSerializer[String] = new StringSerializer

  implicit final val stringDeserializer: KafkaDeserializer[String] = new StringDeserializer

  def createCustomTopic(
    topic: String,
    topicConfig: Map[String, String] = Map.empty,
    partitions: Int = 1,
    replicationFactor: Int = 1
  ): Try[Unit] = {
    val newTopic =
      new NewTopic(topic, partitions, replicationFactor.toShort).configs(topicConfig.asJava)

    withAdminClient { adminClient =>
      adminClient.createTopics(Seq(newTopic).asJava).all.get(2, TimeUnit.SECONDS)
    }.map(_ => ())
  }

  protected def withAdminClient[T](
    body: AdminClient => T
  ): Try[T] = {
    val adminClient = AdminClient.create(
      Map[String, Object](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG       -> container.bootstrapServers,
        AdminClientConfig.CLIENT_ID_CONFIG               -> "test-kafka-admin-client",
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG      -> "10000",
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> "10000"
      ).asJava
    )

    val res = Try(body(adminClient))
    adminClient.close(java.time.Duration.ofMillis(adminClientCloseTimeout.toMillis))

    res
  }

  final def adminClientSettings: AdminClientSettings =
    AdminClientSettings(bootstrapServers = container.bootstrapServers)

  final def defaultConsumerProperties: Map[String, String] =
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG  -> container.bootstrapServers,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG  -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG           -> "test-group-id",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

  final def consumerSettings[F[_]](implicit F: Sync[F]): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withProperties(defaultConsumerProperties)
      .withRecordMetadata(_.timestamp.toString)

  final def producerSettings[F[_]](implicit F: Sync[F]): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String].withProperties(defaultProducerConfig)

  final def withTopic[A](f: String => A): A =
    f(nextTopicName())

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

  private[this] def nextTopicName(): String =
    s"topic-${UUID.randomUUID()}"

  def consumeFirstKeyedMessageFrom[K, V](
    topic: String,
    customProperties: Map[String, Object] = Map.empty
  )(implicit
    keyDeserializer: KafkaDeserializer[K],
    valueDeserializer: KafkaDeserializer[V]
  ): (K, V) =
    consumeNumberKeyedMessagesFrom[K, V](topic, 1, customProperties = customProperties)(
      keyDeserializer,
      valueDeserializer
    ).head

  def consumeNumberKeyedMessagesFrom[K, V](
    topic: String,
    number: Int,
    customProperties: Map[String, Object] = Map.empty
  )(implicit
    keyDeserializer: KafkaDeserializer[K],
    valueDeserializer: KafkaDeserializer[V]
  ): List[(K, V)] =
    consumeNumberKeyedMessagesFromTopics(
      Set(topic),
      number,
      customProperties = customProperties
    )(
      keyDeserializer,
      valueDeserializer
    )(topic)

  def consumeNumberKeyedMessagesFromTopics[K, V](
    topics: Set[String],
    number: Int,
    timeout: Duration = 10.seconds,
    resetTimeoutOnEachMessage: Boolean = true,
    customProperties: Map[String, Object] = Map.empty
  )(implicit
    keyDeserializer: KafkaDeserializer[K],
    valueDeserializer: KafkaDeserializer[V]
  ): Map[String, List[(K, V)]] = {
    val consumerProperties = defaultConsumerProperties ++ customProperties

    var timeoutNanoTime = System.nanoTime + timeout.toNanos
    val consumer        = new KConsumer[K, V](
      consumerProperties.asJava,
      keyDeserializer,
      valueDeserializer
    )

    val messages = Try {
      val messagesBuffers = topics.map(_ -> ListBuffer.empty[(K, V)]).toMap
      var messagesRead    = 0
      consumer.subscribe(topics.asJava)
      topics.foreach(consumer.partitionsFor)

      while (messagesRead < number && System.nanoTime < timeoutNanoTime) {
        val recordIter =
          consumer.poll(java.time.Duration.ofMillis(consumerPollingTimeout.toMillis)).iterator
        if (resetTimeoutOnEachMessage && recordIter.hasNext) {
          timeoutNanoTime = System.nanoTime + timeout.toNanos
        }
        while (recordIter.hasNext && messagesRead < number) {
          val record = recordIter.next
          messagesBuffers(record.topic) += (record.key -> record.value)
          val tp = new TopicPartition(record.topic, record.partition)
          val om = new OffsetAndMetadata(record.offset + 1)
          consumer.commitSync(Map(tp -> om).asJava)
          messagesRead += 1
        }
      }
      if (messagesRead < number) {
        throw new TimeoutException(
          s"Unable to retrieve $number message(s) from Kafka in $timeout - got $messagesRead"
        )
      }
      messagesBuffers.view.map { case (k, v) => (k, v.toList) }.toMap
    }

    consumer.close()
    messages.recover { case ex: KafkaException => throw new Exception("Kafka unavailable", ex) }.get
  }

  private def defaultProducerConfig =
    Map[String, String](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> container.bootstrapServers,
      ProducerConfig.MAX_BLOCK_MS_CONFIG      -> 10000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG  -> 1000.toString
    )

  def publishToKafka[T](
    topic: String,
    message: T
  )(implicit serializer: KafkaSerializer[T]): Unit =
    publishToKafka(
      new KProducer(
        (defaultProducerConfig: Map[String, Object]).asJava,
        new StringSerializer(),
        serializer
      ),
      new KProducerRecord[String, T](topic, message)
    )

  private def publishToKafka[K, T](
    kafkaProducer: KProducer[K, T],
    record: KProducerRecord[K, T]
  ): Unit = {
    val sendFuture = kafkaProducer.send(record)
    val sendResult = Try {
      sendFuture.get(producerPublishTimeout.length, producerPublishTimeout.unit)
    }

    kafkaProducer.close()

    sendResult match {
      case Failure(ex) => throw new Exception("Kafka unavailable", ex)
      case _           => // OK
    }
  }

  def publishToKafka[K, T](topic: String, messages: Seq[(K, T)])(implicit
    keySerializer: KafkaSerializer[K],
    serializer: KafkaSerializer[T]
  ): Unit = {
    val producer =
      new KProducer(
        defaultProducerConfig.asInstanceOf[Map[String, Object]].asJava,
        keySerializer,
        serializer
      )

    val tupleToRecord =
      (new KProducerRecord(topic, _: K, _: T)).tupled

    val futureSend = tupleToRecord.andThen(producer.send)

    val futures = messages.map(futureSend)

    // Assure all messages sent before returning, and fail on first send error
    val records =
      futures.map(f => Try(f.get(producerPublishTimeout.length, producerPublishTimeout.unit)))

    producer.close()

    val _ = records.collectFirst { case Failure(ex) =>
      throw new Exception("Kafka unavailable", ex)
    }
  }

}
