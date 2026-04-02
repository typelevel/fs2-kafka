/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Apply, Bitraverse, Eq, Eval, Show, Traverse}
import cats.syntax.all.*
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.ConsumerRecord.{NO_TIMESTAMP, NULL_SIZE}
import org.apache.kafka.common.record.TimestampType.{CREATE_TIME, LOG_APPEND_TIME}

/**
  * [[ConsumerRecord]] represents a record which has been consumed from Kafka. At the very least,
  * this includes a key of type `K`, value of type `V`, and the topic, partition, and offset of the
  * consumed record.<br><br>
  *
  * To create a new instance, use [[ConsumerRecord#apply]]
  */
sealed abstract class ConsumerRecord[+K, +V] {

  /**
    * The topic from which the record has been consumed.
    */
  def topic: String

  /**
    * The partition from which the record has been consumed.
    */
  def partition: Int

  /**
    * The offset from which the record has been consumed.
    */
  def offset: Long

  /**
    * The record key.
    */
  def key: K

  /**
    * The record value.
    */
  def value: V

  /**
    * The record headers.
    */
  def headers: Headers

  /**
    * The record timestamp.
    */
  def timestamp: Timestamp

  /**
    * The serialized key size if available.
    */
  def serializedKeySize: Option[Int]

  /**
    * The serialized value size if available.
    */
  def serializedValueSize: Option[Int]

  /**
    * The leader epoch if available.
    */
  def leaderEpoch: Option[Int]

  /**
    * Creates a new [[ConsumerRecord]] instance with the specified headers as the headers for the
    * record.
    */
  def withHeaders(headers: Headers): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the specified timestamp as the timestamp for
    * the record.
    */
  def withTimestamp(timestamp: Timestamp): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the specified key size as the key size for the
    * record.
    */
  def withSerializedKeySize(serializedKeySize: Int): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the specified value size as the size for the
    * record.
    */
  def withSerializedValueSize(serializedValueSize: Int): ConsumerRecord[K, V]

  /**
    * Creates a new [[ConsumerRecord]] instance with the specified leader epoch as the epoch for the
    * record.
    */
  def withLeaderEpoch(leaderEpoch: Int): ConsumerRecord[K, V]

  private[kafka] def withValue[V2](v: V2): ConsumerRecord[K, V2]
  private[kafka] def withKeyValue[K2, V2](k: K2, v: V2): ConsumerRecord[K2, V2]

}

object ConsumerRecord {

  final private[this] case class ConsumerRecordImpl[+K, +V](
    override val topic: String,
    override val partition: Int,
    override val offset: Long,
    override val key: K,
    override val value: V,
    override val headers: Headers,
    override val timestamp: Timestamp,
    override val serializedKeySize: Option[Int],
    override val serializedValueSize: Option[Int],
    override val leaderEpoch: Option[Int]
  ) extends ConsumerRecord[K, V] {

    override def withHeaders(headers: Headers): ConsumerRecord[K, V] =
      copy(headers = headers)

    override def withTimestamp(timestamp: Timestamp): ConsumerRecord[K, V] =
      copy(timestamp = timestamp)

    override def withSerializedKeySize(serializedKeySize: Int): ConsumerRecord[K, V] =
      copy(serializedKeySize = Some(serializedKeySize))

    override def withSerializedValueSize(serializedValueSize: Int): ConsumerRecord[K, V] =
      copy(serializedValueSize = Some(serializedValueSize))

    override def withLeaderEpoch(leaderEpoch: Int): ConsumerRecord[K, V] =
      copy(leaderEpoch = Some(leaderEpoch))

    override def toString: String = {
      val b = new java.lang.StringBuilder("ConsumerRecord(")
      b.append("topic = ").append(topic)
      b.append(", partition = ").append(partition)
      b.append(", offset = ").append(offset)
      b.append(", key = ").append(key)
      b.append(", value = ").append(value)
      if (headers.nonEmpty)
        b.append(", headers = ").append(headers)
      if (timestamp.nonEmpty)
        b.append(", timestamp = ").append(timestamp)
      if (serializedKeySize.nonEmpty)
        b.append(", serializedKeySize = ").append(serializedKeySize.get)
      if (serializedValueSize.nonEmpty)
        b.append(", serializedValueSize = ").append(serializedValueSize.get)
      if (leaderEpoch.nonEmpty)
        b.append(", leaderEpoch = ").append(leaderEpoch.get)
      b.append(")").toString
    }

    override private[kafka] def withValue[V2](v: V2): ConsumerRecord[K, V2] = copy(value = v)

    override private[kafka] def withKeyValue[K2, V2](k: K2, v: V2): ConsumerRecord[K2, V2] =
      copy(key = k, value = v)

  }

  /**
    * Creates a new [[ConsumerRecord]] instance using the specified key and value, and the topic,
    * partition, and offset of the record.
    */
  def apply[K, V](
    topic: String,
    partition: Int,
    offset: Long,
    key: K,
    value: V
  ): ConsumerRecord[K, V] =
    ConsumerRecordImpl(
      topic = topic,
      partition = partition,
      offset = offset,
      key = key,
      value = value,
      headers = Headers.empty,
      timestamp = Timestamp.none,
      serializedKeySize = None,
      serializedValueSize = None,
      leaderEpoch = None
    )

  private[this] def deserializeFromBytes[F[_], K, V](
    record: KafkaByteConsumerRecord,
    headers: Headers,
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V]
  )(implicit F: Apply[F]): F[(K, V)] = {
    val key   = keyDeserializer.deserialize(record.topic, headers, record.key)
    val value = valueDeserializer.deserialize(record.topic, headers, record.value)
    key.product(value)
  }

  private[kafka] def fromJava[F[_], K, V](
    record: KafkaByteConsumerRecord,
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V]
  )(implicit F: Apply[F]): F[ConsumerRecord[K, V]] = {
    val headers = record.headers.asScala
    deserializeFromBytes(record, headers, keyDeserializer, valueDeserializer).map {
      case (key, value) =>
        ConsumerRecordImpl(
          topic = record.topic,
          partition = record.partition,
          offset = record.offset,
          key = key,
          value = value,
          headers = headers,
          timestamp = {
            if (record.timestamp != NO_TIMESTAMP) {
              record.timestampType match {
                case CREATE_TIME     => Timestamp.createTime(record.timestamp)
                case LOG_APPEND_TIME => Timestamp.logAppendTime(record.timestamp)
                case _               => Timestamp.unknownTime(record.timestamp)
              }
            } else {
              Timestamp.none
            }
          },
          serializedKeySize =
            if (record.serializedKeySize != NULL_SIZE)
              Some(record.serializedKeySize)
            else None,
          serializedValueSize =
            if (record.serializedValueSize != NULL_SIZE)
              Some(record.serializedValueSize)
            else None,
          leaderEpoch =
            if (record.leaderEpoch.isPresent)
              Some(record.leaderEpoch.get)
            else None
        )
    }
  }

  implicit def consumerRecordShow[K: Show, V: Show]: Show[ConsumerRecord[K, V]] =
    Show.show { record =>
      val b = new java.lang.StringBuilder("ConsumerRecord(")
      b.append("topic = ").append(record.topic)
      b.append(", partition = ").append(record.partition)
      b.append(", offset = ").append(record.offset)
      b.append(", key = ").append(record.key.show)
      b.append(", value = ").append(record.value.show)
      if (record.headers.nonEmpty)
        b.append(", headers = ").append(record.headers)
      if (record.timestamp.nonEmpty)
        b.append(", timestamp = ").append(record.timestamp)
      if (record.serializedKeySize.nonEmpty)
        b.append(", serializedKeySize = ").append(record.serializedKeySize.get)
      if (record.serializedValueSize.nonEmpty)
        b.append(", serializedValueSize = ").append(record.serializedValueSize.get)
      if (record.leaderEpoch.nonEmpty)
        b.append(", leaderEpoch = ").append(record.leaderEpoch.get)
      b.append(")").toString
    }

  implicit def consumerRecordEq[K: Eq, V: Eq]: Eq[ConsumerRecord[K, V]] =
    Eq.instance { case (l, r) =>
      l.topic === r.topic &&
      l.partition === r.partition &&
      l.offset === r.offset &&
      l.key === r.key &&
      l.value === r.value &&
      l.headers === r.headers &&
      l.timestamp === r.timestamp &&
      l.serializedKeySize === r.serializedKeySize &&
      l.serializedValueSize === r.serializedValueSize &&
      l.leaderEpoch === r.leaderEpoch
    }

  implicit val consumerRecordBitraverse: Bitraverse[ConsumerRecord] =
    new Bitraverse[ConsumerRecord] {

      override def bitraverse[G[_], A, B, C, D](
        fab: ConsumerRecord[A, B]
      )(f: A => G[C], g: B => G[D])(implicit G: Applicative[G]): G[ConsumerRecord[C, D]] =
        G.product(f(fab.key), g(fab.value))
          .map { case (c, d) =>
            fab.withKeyValue(c, d)
          }

      override def bifoldLeft[A, B, C](
        fab: ConsumerRecord[A, B],
        c: C
      )(f: (C, A) => C, g: (C, B) => C): C =
        g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](
        fab: ConsumerRecord[A, B],
        c: Eval[C]
      )(f: (A, Eval[C]) => Eval[C], g: (B, Eval[C]) => Eval[C]): Eval[C] =
        g(fab.value, f(fab.key, c))

    }

  implicit def consumerRecordTraverse[K]: Traverse[ConsumerRecord[K, *]] =
    new Traverse[ConsumerRecord[K, *]] {

      override def traverse[G[_], A, B](
        fa: ConsumerRecord[K, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[ConsumerRecord[K, B]] =
        f(fa.value).map { b =>
          fa.withValue(b)
        }

      override def foldLeft[A, B](fa: ConsumerRecord[K, A], b: B)(f: (B, A) => B): B =
        f(b, fa.value)

      override def foldRight[A, B](fa: ConsumerRecord[K, A], lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]
      ): Eval[B] =
        f(fa.value, lb)

    }

}
