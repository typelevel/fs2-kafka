/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

/**
  * Consumer-side metadata used to derive a tracing `poll` / `receive` span.
  *
  * This context describes a delivered chunk of consumed records. It combines dynamic chunk
  * information such as topics, partitions, and record count with static consumer metadata captured
  * from the bound Kafka consumer.
  */
sealed trait ReceiveSpanContext {

  /**
    * Topics covered by the delivered chunk.
    */
  def topics: Set[String]

  /**
    * Partitions covered by the delivered chunk.
    */
  def partitions: Set[Int]

  /**
    * Number of records in the delivered chunk.
    */
  def recordCount: Int

  /**
    * Canonical string form of the Kafka message key when this span describes a single record and
    * the key can be represented safely.
    */
  def messageKey: Option[String]

  /**
    * Kafka record offset when this span describes exactly one record.
    */
  def offset: Option[Long]

  /**
    * Kafka `client.id` captured from the bound consumer, when configured.
    */
  def clientId: Option[String]

  /**
    * Kafka `group.id` captured from the bound consumer, when configured.
    */
  def groupId: Option[String]

}

object ReceiveSpanContext {

  private[otel4s] def apply(
    topics: Set[String],
    partitions: Set[Int],
    recordCount: Int,
    messageKey: Option[String],
    offset: Option[Long],
    clientId: Option[String],
    groupId: Option[String]
  ): ReceiveSpanContext =
    Impl(
      topics,
      partitions,
      recordCount,
      messageKey,
      offset,
      clientId,
      groupId
    )

  final private case class Impl(
    topics: Set[String],
    partitions: Set[Int],
    recordCount: Int,
    messageKey: Option[String],
    offset: Option[Long],
    clientId: Option[String],
    groupId: Option[String]
  ) extends ReceiveSpanContext

}
