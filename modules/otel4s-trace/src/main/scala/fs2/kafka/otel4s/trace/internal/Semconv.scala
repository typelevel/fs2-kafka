/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace
package internal

import fs2.kafka._
import fs2.kafka.internal.{ConsumerMetadata, ProducerMetadata}
import fs2.Chunk

import org.apache.kafka.clients.producer.RecordMetadata
import org.typelevel.otel4s.{Attribute, AttributeKey, Attributes}

/**
  * Messaging span semantic conventions:
  * [[https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/]]
  *
  * Kafka-specific messaging conventions:
  * [[https://opentelemetry.io/docs/specs/semconv/messaging/kafka/]]
  */
private[otel4s] object Semconv {

  object Const {
    val MessagingSystem = Attribute("messaging.system", "kafka")
  }

  object Keys {

    val DestinationName        = AttributeKey[String]("messaging.destination.name")
    val DestinationPartitionId = AttributeKey[String]("messaging.destination.partition.id")
    val OperationName          = AttributeKey[String]("messaging.operation.name")
    val OperationType          = AttributeKey[String]("messaging.operation.type")
    val ClientId               = AttributeKey[String]("messaging.client.id")
    val ConsumerGroupName      = AttributeKey[String]("messaging.consumer.group.name")
    val KafkaMessageKey        = AttributeKey[String]("messaging.kafka.message.key")
    val KafkaMessageTombstone  = AttributeKey[Boolean]("messaging.kafka.message.tombstone")
    val KafkaOffset            = AttributeKey[Long]("messaging.kafka.offset")
    val BatchMessageCount      = AttributeKey[Long]("messaging.batch.message_count")

  }

  def sendSpanContext[K: KafkaMessageKey, V](
    metadata: ProducerMetadata,
    records: ProducerRecords[K, V]
  ): SendSpanContext =
    SendSpanContext(
      topics = records.iterator.map(_.topic).toSet,
      partitions = records.iterator.flatMap(_.partition).toSet,
      recordCount = records.size,
      messageKey = producerSingleRecordMessageKey(records),
      clientId = metadata.clientId
    )

  def receiveSpanContext[K: KafkaMessageKey, V](
    metadata: ConsumerMetadata,
    records: Chunk[ConsumerRecord[K, V]]
  ): ReceiveSpanContext =
    ReceiveSpanContext(
      topics = records.iterator.map(_.topic).toSet,
      partitions = records.iterator.map(_.partition).toSet,
      recordCount = records.size,
      messageKey = consumerSingleRecordMessageKey(records),
      offset = consumerSingleRecordOffset(records),
      clientId = metadata.clientId,
      groupId = metadata.groupId
    )

  def processSpanContext[K: KafkaMessageKey, V](
    metadata: ConsumerMetadata,
    record: ConsumerRecord[K, V]
  ): ProcessSpanContext =
    ProcessSpanContext(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      messageKey = KafkaMessageKey[K].toMessageKey(record.key),
      clientId = metadata.clientId,
      groupId = metadata.groupId
    )

  def sendAttributes[K, V](ctx: SendSpanContext, records: ProducerRecords[K, V]): Attributes = {
    val builder = baseBuilder(
      operationName = "send",
      operationType = "send",
      topic = singleton(ctx.topics),
      clientId = ctx.clientId,
      consumerGroupName = None
    )

    // For producer send spans, span-level record attributes are only valid when the batch really
    // describes one logical message or one logical topic-partition. We intentionally omit
    // `messaging.destination.partition.id` for mixed-topic batches such as topic-a/0 + topic-b/0.
    // See:
    // - messaging spans: destination attributes belong to the operation the span actually describes
    // - Kafka semconv: partition is a topic-partition concept, not a bare partition number
    builder.addAll(
      Keys.DestinationPartitionId.maybe(producerSingleLogicalPartition(records).map(_.toString))
    )

    // `messaging.kafka.message.key` and `messaging.kafka.message.tombstone` are attached at the
    // span level only when this send span still represents exactly one produced message. For true
    // batches, per-message detail moves to links instead of collapsing multiple values onto the span.
    // See:
    // https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
    builder.addAll(Keys.KafkaMessageKey.maybe(ctx.messageKey))
    builder.addAll(Keys.KafkaMessageTombstone.maybe(producerSingleRecordTombstone(records)))

    // `messaging.batch.message_count` is emitted only for actual batches, following the messaging
    // span guidance that batch-only metadata should not be set on single-message operations.
    builder.addAll(Keys.BatchMessageCount.maybe(Option.when(ctx.recordCount > 1)(ctx.recordCount)))

    builder.result()
  }

  def createSpanName(topic: String): String =
    s"create $topic"

  def createAttributes[K: KafkaMessageKey, V](
    metadata: ProducerMetadata,
    record: ProducerRecord[K, V]
  ): Attributes = {
    val builder = baseBuilder(
      operationName = "create",
      operationType = "create",
      topic = Some(record.topic),
      clientId = metadata.clientId,
      consumerGroupName = None
    )

    // A create span always describes exactly one message, so Kafka message attributes can be
    // attached directly to the span. The partition is only available when the caller explicitly
    // selected one on the producer record; otherwise Kafka chooses it later and we must not invent it.
    // See:
    // - messaging spans: single-message spans may carry per-message attributes directly
    // - Kafka semconv: partition is optional until known
    builder.addAll(Keys.DestinationPartitionId.maybe(record.partition.map(_.toString)))
    builder.addAll(Keys.KafkaMessageKey.maybe(KafkaMessageKey[K].toMessageKey(record.key)))
    builder.addAll(Keys.KafkaMessageTombstone.maybe(tombstoneAttribute(record.value)))

    builder.result()
  }

  def receiveAttributes[K, V](
    ctx: ReceiveSpanContext,
    records: Chunk[ConsumerRecord[K, V]]
  ): Attributes = {
    val builder = baseBuilder(
      operationName = "poll",
      operationType = "receive",
      topic = singleton(ctx.topics),
      clientId = ctx.clientId,
      consumerGroupName = ctx.groupId
    )

    // For receive spans, span-level partition attribution is only valid when the delivered chunk
    // covers one logical topic-partition. Mixed-topic batches with the same numeric partition must
    // not collapse to a fake singleton partition id.
    builder.addAll(
      Keys.DestinationPartitionId.maybe(consumerSingleLogicalPartition(records).map(_.toString))
    )

    // Message-level attributes stay on the receive span only when the delivered chunk contains one
    // record. For larger chunks, the spec model is to keep the span generic and put per-message
    // detail on links instead.
    builder.addAll(Keys.KafkaMessageKey.maybe(ctx.messageKey))
    builder.addAll(Keys.KafkaMessageTombstone.maybe(consumerSingleRecordTombstone(records)))
    builder.addAll(Keys.KafkaOffset.maybe(ctx.offset))

    // Batch count is only meaningful once the receive span represents more than one delivered
    // message.
    builder.addAll(Keys.BatchMessageCount.maybe(Option.when(ctx.recordCount > 1)(ctx.recordCount)))

    builder.result()
  }

  def processAttributes[K, V](ctx: ProcessSpanContext, record: ConsumerRecord[K, V]): Attributes = {
    val builder = baseBuilder(
      operationName = "process",
      operationType = "process",
      topic = Some(ctx.topic),
      clientId = ctx.clientId,
      consumerGroupName = ctx.groupId
    )

    // A process span is always per-record, so the topic-partition and offset are known and belong
    // directly on the span rather than on links.
    builder.addOne(Keys.DestinationPartitionId(ctx.partition.toString))
    builder.addAll(Keys.KafkaMessageKey.maybe(ctx.messageKey))
    builder.addAll(Keys.KafkaMessageTombstone.maybe(tombstoneAttribute(record.value)))
    builder.addOne(Keys.KafkaOffset(ctx.offset))

    builder.result()
  }

  def receiveLinkAttributes[K: KafkaMessageKey, V](record: ConsumerRecord[K, V]): Attributes = {
    val builder = Attributes.newBuilder

    // Receive links carry the per-message Kafka detail that cannot always live on a batch receive
    // span. That includes topic, partition, key, tombstone, and offset for the specific consumed
    // record represented by the link.
    builder.addOne(Keys.DestinationName(record.topic))
    builder.addOne(Keys.DestinationPartitionId(record.partition.toString))
    builder.addAll(Keys.KafkaMessageKey.maybe(KafkaMessageKey[K].toMessageKey(record.key)))
    builder.addAll(Keys.KafkaMessageTombstone.maybe(tombstoneAttribute(record.value)))
    builder.addOne(Keys.KafkaOffset(record.offset))

    builder.result()
  }

  def sendLinkAttributes[K: KafkaMessageKey, V](record: ProducerRecord[K, V]): Attributes = {
    val builder = Attributes.newBuilder

    // Send links describe the message creation context or create span associated with one produced
    // record. We keep per-record Kafka details on the link so batch send spans do not need to
    // collapse multiple destinations or keys into one span-level value.
    builder.addOne(Keys.DestinationName(record.topic))
    builder.addAll(Keys.DestinationPartitionId.maybe(record.partition.map(_.toString)))
    builder.addAll(Keys.KafkaMessageKey.maybe(KafkaMessageKey[K].toMessageKey(record.key)))
    builder.addAll(Keys.KafkaMessageTombstone.maybe(tombstoneAttribute(record.value)))

    builder.result()
  }

  def sendResultAttributes[K, V](result: ProducerResult[K, V]): Attributes =
    Option
      .when(result.size == 1)(result.head.get)
      .map { case (_, metadata) => singleRecordSendResultAttributes(metadata) }
      .getOrElse(Attributes.empty)

  private def baseBuilder(
    operationName: String,
    operationType: String,
    topic: Option[String],
    clientId: Option[String],
    consumerGroupName: Option[String]
  ): Attributes.Builder = {
    val builder = Attributes.newBuilder

    // These are the common messaging span attributes shared by send / create / receive / process:
    // system, operation type, operation name, destination name, producer/consumer client id, and
    // consumer group when applicable.
    // See:
    // - messaging spans: operation naming and generic messaging attributes
    // - Kafka semconv: `messaging.client.id` and `messaging.consumer.group.name`
    builder.addOne(Const.MessagingSystem)
    builder.addOne(Keys.OperationType(operationType))
    builder.addOne(Keys.OperationName(operationName))
    builder.addAll(Keys.DestinationName.maybe(topic))
    builder.addAll(Keys.ClientId.maybe(clientId))
    builder.addAll(Keys.ConsumerGroupName.maybe(consumerGroupName))

    builder
  }

  // if there are more than 1 entry, we must return None
  private def singleton[A](values: Set[A]): Option[A] =
    Option.when(values.size == 1)(values.head)

  private def producerSingleRecordMessageKey[K: KafkaMessageKey, V](
    records: ProducerRecords[K, V]
  ): Option[String] =
    Option
      .when(records.size == 1)(records.head.get)
      .flatMap(record => KafkaMessageKey[K].toMessageKey(record.key))

  private def consumerSingleRecordMessageKey[K: KafkaMessageKey, V](
    records: Chunk[ConsumerRecord[K, V]]
  ): Option[String] =
    Option
      .when(records.size == 1)(records.head)
      .flatten
      .flatMap(record => KafkaMessageKey[K].toMessageKey(record.key))

  private def producerSingleRecordTombstone[K, V](
    records: ProducerRecords[K, V]
  ): Option[Boolean] =
    Option
      .when(records.size == 1)(records.head.get)
      .flatMap(record => tombstoneAttribute(record.value))

  private def consumerSingleRecordTombstone[K, V](
    records: Chunk[ConsumerRecord[K, V]]
  ): Option[Boolean] =
    Option
      .when(records.size == 1)(records.head)
      .flatten
      .flatMap(record => tombstoneAttribute(record.value))

  private def consumerSingleRecordOffset[K, V](
    records: Chunk[ConsumerRecord[K, V]]
  ): Option[Long] =
    Option.when(records.size == 1)(records.head).flatten.map(_.offset)

  private def producerSingleLogicalPartition[K, V](
    records: ProducerRecords[K, V]
  ): Option[Int] = {
    val topicPartitions = records
      .iterator
      .map(record => record.partition.map(record.topic -> _))
      .toList

    Option
      .when(topicPartitions.nonEmpty && topicPartitions.forall(_.isDefined))(
        topicPartitions.flatten.toSet
      )
      .flatMap(singleton)
      .map(_._2)
  }

  private def consumerSingleLogicalPartition[K, V](
    records: Chunk[ConsumerRecord[K, V]]
  ): Option[Int] =
    singleton(records.iterator.map(record => record.topic -> record.partition).toSet).map(_._2)

  private def tombstoneAttribute(value: Any): Option[Boolean] =
    Option.when(value == null)(true)

  private def singleRecordSendResultAttributes(metadata: RecordMetadata): Attributes = {
    val builder = Attributes.newBuilder

    // Producer result metadata is only available after Kafka acknowledges a single produced record.
    // At that point the chosen partition and broker-assigned offset become known and can be added to
    // the send span as result attributes.
    builder.addOne(Keys.DestinationPartitionId(metadata.partition.toString))
    builder.addOne(Keys.KafkaOffset(metadata.offset))

    builder.result()
  }

}
