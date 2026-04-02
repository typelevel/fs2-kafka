/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2

import scala.concurrent.duration.FiniteDuration

import cats.effect.*
import cats.Traverse

import org.apache.kafka.clients.producer.RecordMetadata

package object kafka {

  type Id[+A] = A

  /**
    * Alias for Java Kafka `Consumer[Array[Byte], Array[Byte]]`.
    */
  type KafkaByteConsumer =
    org.apache.kafka.clients.consumer.Consumer[Array[Byte], Array[Byte]]

  /**
    * Alias for Java Kafka `Producer[Array[Byte], Array[Byte]]`.
    */
  type KafkaByteProducer =
    org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]]

  /**
    * Alias for Java Kafka `Deserializer[A]`.
    */
  type KafkaDeserializer[A] =
    org.apache.kafka.common.serialization.Deserializer[A]

  /**
    * Alias for Java Kafka `Serializer[A]`.
    */
  type KafkaSerializer[A] =
    org.apache.kafka.common.serialization.Serializer[A]

  /**
    * Alias for Java Kafka `Header`.
    */
  type KafkaHeader =
    org.apache.kafka.common.header.Header

  /**
    * Alias for Java Kafka `Headers`.
    */
  type KafkaHeaders =
    org.apache.kafka.common.header.Headers

  /**
    * Alias for Java Kafka `ConsumerRecords[Array[Byte], Array[Byte]]`.
    */
  type KafkaByteConsumerRecords =
    org.apache.kafka.clients.consumer.ConsumerRecords[Array[Byte], Array[Byte]]

  /**
    * Alias for Java Kafka `ConsumerRecord[Array[Byte], Array[Byte]]`.
    */
  type KafkaByteConsumerRecord =
    org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]]

  /**
    * Alias for Java Kafka `ProducerRecord[Array[Byte], Array[Byte]]`.
    */
  type KafkaByteProducerRecord =
    org.apache.kafka.clients.producer.ProducerRecord[Array[Byte], Array[Byte]]

  type ProducerRecords[K, V] = Chunk[ProducerRecord[K, V]]

  type TransactionalProducerRecords[F[_], +K, +V] = Chunk[CommittableProducerRecords[F, K, V]]

  type ProducerResult[K, V] = Chunk[(ProducerRecord[K, V], RecordMetadata)]

  /**
    * Commits offsets in batches of every `n` offsets or time window of length `d`, whichever
    * happens first. If there are no offsets to commit within a time window, no attempt will be made
    * to commit offsets for that time window.
    */
  def commitBatchWithin[F[_]](n: Int, d: FiniteDuration)(implicit
    F: Temporal[F]
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  type Serializer[F[_], A]      = GenericSerializer[KeyOrValue, F, A]
  type KeySerializer[F[_], A]   = GenericSerializer[Key, F, A]
  type ValueSerializer[F[_], A] = GenericSerializer[Value, F, A]
  val Serializer: GenericSerializer.type = GenericSerializer

  type Deserializer[F[_], A]      = GenericDeserializer[KeyOrValue, F, A]
  type KeyDeserializer[F[_], A]   = GenericDeserializer[Key, F, A]
  type ValueDeserializer[F[_], A] = GenericDeserializer[Value, F, A]
  val Deserializer: GenericDeserializer.type = GenericDeserializer

}

package kafka {

  /**
    * Phantom types to indicate whether a [[Serializer]]/[[Deserializer]] if for keys, values, or
    * both
    */
  sealed trait KeyOrValue
  sealed trait Key   extends KeyOrValue
  sealed trait Value extends KeyOrValue

}

package kafka {

  import cats.Foldable

  object ProducerRecords {

    def apply[F[+_], K, V](
      records: F[ProducerRecord[K, V]]
    )(implicit
      F: Traverse[F]
    ): ProducerRecords[K, V] = Chunk.from(Foldable[F].toIterable(records))

    def one[K, V](record: ProducerRecord[K, V]): ProducerRecords[K, V] =
      Chunk.singleton(record)

  }

  object TransactionalProducerRecords {

    @deprecated("this is now an identity operation", "3.0.0-M5")
    def apply[F[_], K, V](
      chunk: Chunk[CommittableProducerRecords[F, K, V]]
    ): Chunk[CommittableProducerRecords[F, K, V]] = chunk

    /**
      * Creates a new [[TransactionalProducerRecords]] for producing exactly one
      * [[CommittableProducerRecords]]
      */
    def one[F[_], K, V](
      record: CommittableProducerRecords[F, K, V]
    ): TransactionalProducerRecords[F, K, V] =
      Chunk.singleton(record)

  }

}
