/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

/**
  * Producer-side metadata used to derive a tracing `send` span.
  *
  * This context describes the produced batch at the point where `fs2-kafka-otel4s` is about to
  * create a producer span. It combines dynamic record information such as topics, partitions, and
  * batch size with static producer metadata captured from the bound Kafka producer.
  */
sealed trait SendSpanContext {

  /**
    * Topics covered by the produced records.
    */
  def topics: Set[String]

  /**
    * Partitions covered by the produced records, when explicitly set on the records.
    */
  def partitions: Set[Int]

  /**
    * Number of records in the produced batch.
    */
  def recordCount: Int

  /**
    * Canonical string form of the Kafka message key when this span describes a single record and
    * the key can be represented safely.
    */
  def messageKey: Option[String]

  /**
    * Kafka `client.id` captured from the bound producer, when configured.
    */
  def clientId: Option[String]

}

object SendSpanContext {

  private[otel4s] def apply(
    topics: Set[String],
    partitions: Set[Int],
    recordCount: Int,
    messageKey: Option[String],
    clientId: Option[String]
  ): SendSpanContext =
    Impl(topics, partitions, recordCount, messageKey, clientId)

  final private case class Impl(
    topics: Set[String],
    partitions: Set[Int],
    recordCount: Int,
    messageKey: Option[String],
    clientId: Option[String]
  ) extends SendSpanContext

}
