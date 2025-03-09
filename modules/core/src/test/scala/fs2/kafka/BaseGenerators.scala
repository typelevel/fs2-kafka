/*
 * Copyright 2018-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.nio.charset.*
import java.util.UUID

import cats.{ApplicativeError, ApplicativeThrow}
import cats.data.Chain
import cats.effect.*
import cats.laws.discipline.arbitrary.*
import cats.syntax.all.*
import fs2.Chunk

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalacheck.rng.Seed
import org.scalacheck.Arbitrary.arbitrary

trait BaseGenerators {

  implicit def chunkCogen[A: Cogen]: Cogen[Chunk[A]] = Cogen.it(_.iterator)

  val genTopic: Gen[String] = arbitrary[String]

  val genPartition: Gen[Int] = Gen.chooseNum(0, Int.MaxValue)

  val genTopicPartition: Gen[TopicPartition] =
    for {
      topic     <- genTopic
      partition <- genPartition
    } yield new TopicPartition(topic, partition)

  implicit val arbTopicPartition: Arbitrary[TopicPartition] =
    Arbitrary(genTopicPartition)

  implicit val cogenTopicPartition: Cogen[TopicPartition] =
    Cogen { (seed: Seed, topicPartition: TopicPartition) =>
      Cogen.perturbPair(seed, (topicPartition.topic, topicPartition.partition()))
    }

  val genOffsetAndMetadata: Gen[OffsetAndMetadata] =
    for {
      offset   <- Gen.chooseNum(1L, Long.MaxValue)
      metadata <- arbitrary[String]
    } yield new OffsetAndMetadata(offset, metadata)

  implicit val arbOffsetAndMetadata: Arbitrary[OffsetAndMetadata] =
    Arbitrary(genOffsetAndMetadata)

  implicit val cogenOffsetAndMetadata: Cogen[OffsetAndMetadata] =
    Cogen { (seed: Seed, offset: OffsetAndMetadata) =>
      Cogen.perturbPair(seed, (offset.offset, offset.metadata()))
    }

  def genCommittableOffset[F[_]](implicit
    F: ApplicativeError[F, Throwable]
  ): Gen[CommittableOffset[F]] =
    for {
      topicPartition    <- genTopicPartition
      offsetAndMetadata <- genOffsetAndMetadata
      groupId           <- arbitrary[Option[String]]
    } yield CommittableOffset[F](
      topicPartition = topicPartition,
      offsetAndMetadata = offsetAndMetadata,
      consumerGroupId = groupId,
      commit = _ => F.unit
    )

  implicit def arbCommittableOffset[F[_]](implicit
    F: ApplicativeError[F, Throwable]
  ): Arbitrary[CommittableOffset[F]] =
    Arbitrary(genCommittableOffset[F])

  implicit def cogenCommittableOffset[F[_]]: Cogen[CommittableOffset[F]] =
    Cogen { (seed: Seed, offset: CommittableOffset[F]) =>
      (Cogen
        .perturb(_: Seed, offset.topicPartition))
        .andThen(Cogen.perturb(_, offset.offsetAndMetadata))
        .andThen(Cogen.perturb(_, offset.consumerGroupId))
        .apply(seed)
    }

  def genCommittableOffsetBatch[F[_]](implicit
    F: ApplicativeError[F, Throwable]
  ): Gen[CommittableOffsetBatch[F]] =
    arbitrary[Map[TopicPartition, OffsetAndMetadata]].map(
      CommittableOffsetBatch[F](_, Set.empty, consumerGroupIdsMissing = false, _ => F.unit)
    )

  implicit def arbCommittableOffsetBatch[F[_]](implicit
    F: ApplicativeError[F, Throwable]
  ): Arbitrary[CommittableOffsetBatch[F]] =
    Arbitrary(genCommittableOffsetBatch[F])

  val genUUID: Gen[UUID] =
    arbitrary[Array[Byte]].map(UUID.nameUUIDFromBytes)

  implicit val arbUUID: Arbitrary[UUID] =
    Arbitrary(genUUID)

  val genCharset: Gen[Charset] =
    Gen.oneOf(
      StandardCharsets.US_ASCII,
      StandardCharsets.ISO_8859_1,
      StandardCharsets.UTF_8,
      StandardCharsets.UTF_16BE,
      StandardCharsets.UTF_16LE,
      StandardCharsets.UTF_16
    )

  implicit val arbCharset: Arbitrary[Charset] =
    Arbitrary(genCharset)

  def genDeserializerString[F[_]](implicit F: Sync[F]): Gen[Deserializer[F, String]] =
    genCharset.map(Deserializer.string[F])

  implicit def arbDeserializerString[F[_]](implicit
    F: Sync[F]
  ): Arbitrary[Deserializer[F, String]] =
    Arbitrary(genDeserializerString)

  implicit def arbDeserializerCombine[F[_], A, B](implicit
    F: Sync[F],
    arbB: Arbitrary[Deserializer[F, B]],
    arbABA: Arbitrary[(A, B) => A]
  ): Arbitrary[Deserializer[F, A => A]] =
    Arbitrary {
      for {
        deserializer <- arbitrary[Deserializer[F, B]]
        combine      <- arbitrary[(A, B) => A]
      } yield {
        Deserializer.instance { (topic, headers, bytes) =>
          deserializer.deserialize(topic, headers, bytes).map(b => (a: A) => combine(a, b))
        }
      }
    }

  def genSerializerString[F[_]](implicit F: Sync[F]): Gen[Serializer[F, String]] =
    genCharset.map(Serializer.string[F])

  implicit def arbSerializerString[F[_]](implicit
    F: Sync[F]
  ): Arbitrary[Serializer[F, String]] =
    Arbitrary(genSerializerString)

  val genHeader: Gen[Header] =
    for {
      key   <- arbitrary[String]
      value <- arbitrary[Array[Byte]]
    } yield Header(key, value)

  implicit val arbHeader: Arbitrary[Header] =
    Arbitrary(genHeader)

  implicit val cogenHeader: Cogen[Header] =
    Cogen { (seed: Seed, header: Header) =>
      Cogen.perturbPair(seed, (header.key, header.value))
    }

  val genHeaders: Gen[Headers] =
    Gen.listOf(genHeader).map(Headers.fromSeq)

  implicit val arbHeaders: Arbitrary[Headers] =
    Arbitrary(genHeaders)

  implicit val cogenHeaders: Cogen[Headers] =
    Cogen[Chain[Header]].contramap(_.toChain)

  val genHeaderSerializerString: Gen[HeaderSerializer[String]] =
    genCharset.map(HeaderSerializer.string)

  implicit val arbHeaderSerializerString: Arbitrary[HeaderSerializer[String]] =
    Arbitrary(genHeaderSerializerString)

  val genHeaderDeserializerString: Gen[HeaderDeserializer[String]] =
    genCharset.map(HeaderDeserializer.string)

  implicit val arbHeaderDeserializerString: Arbitrary[HeaderDeserializer[String]] =
    Arbitrary(genHeaderDeserializerString)

  implicit def arbHeaderDeserializerCombine[A, B](implicit
    arbB: Arbitrary[HeaderDeserializer[B]],
    arbABA: Arbitrary[(A, B) => A]
  ): Arbitrary[HeaderDeserializer[A => A]] =
    Arbitrary {
      for {
        deserializer <- arbitrary[HeaderDeserializer[B]]
        combine      <- arbitrary[(A, B) => A]
      } yield {
        HeaderDeserializer.instance { bytes => (a: A) =>
          combine(a, deserializer.deserialize(bytes))
        }
      }
    }

  val genTimestamp: Gen[Timestamp] = for {
    long <- Gen.choose(1L, Long.MaxValue)
    timestamp <- Gen.oneOf(
                   Seq(
                     Timestamp.createTime(long),
                     Timestamp.logAppendTime(long),
                     Timestamp.unknownTime(long),
                     Timestamp.none
                   )
                 )
  } yield timestamp

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary(genTimestamp)

  implicit val cogenTimestamp: Cogen[Timestamp] =
    Cogen { (seed: Seed, timestamp: Timestamp) =>
      (Cogen
        .perturb(_: Seed, timestamp.createTime))
        .andThen(Cogen.perturb(_, timestamp.logAppendTime))
        .andThen(Cogen.perturb(_, timestamp.unknownTime))
        .andThen(Cogen.perturb(_, timestamp.isEmpty))
        .apply(seed)
    }

  def genConsumerRecord[K: Arbitrary, V: Arbitrary]: Gen[ConsumerRecord[K, V]] =
    for {
      k              <- Arbitrary.arbitrary[K]
      v              <- Arbitrary.arbitrary[V]
      topicPartition <- genTopicPartition
      offset         <- genOffsetAndMetadata
      headers        <- Arbitrary.arbitrary[Option[Headers]]
      timestamp      <- Arbitrary.arbitrary[Option[Timestamp]]
      leaderEpoch    <- Arbitrary.arbitrary[Option[Int]]
    } yield {
      val record = ConsumerRecord(
        topic = topicPartition.topic,
        partition = topicPartition.partition,
        offset = offset.offset,
        key = k,
        value = v
      ).withHeaders(headers.getOrElse(Headers.empty))
        .withTimestamp(timestamp.getOrElse(Timestamp.none))

      leaderEpoch.fold(record)(record.withLeaderEpoch)
    }

  implicit def arbConsumerRecord[K: Arbitrary, V: Arbitrary]: Arbitrary[ConsumerRecord[K, V]] =
    Arbitrary(genConsumerRecord[K, V])

  implicit def cogenConsumerRecord[K: Cogen, V: Cogen]: Cogen[ConsumerRecord[K, V]] =
    Cogen { (seed: Seed, record: ConsumerRecord[K, V]) =>
      (Cogen
        .perturb(_: Seed, record.topic))
        .andThen(Cogen.perturb(_, record.partition))
        .andThen(Cogen.perturb(_, record.offset))
        .andThen(Cogen.perturb(_, record.key))
        .andThen(Cogen.perturb(_, record.value))
        .andThen(Cogen.perturb(_, record.headers))
        .andThen(Cogen.perturb(_, record.timestamp))
        .andThen(Cogen.perturb(_, record.serializedKeySize))
        .andThen(Cogen.perturb(_, record.serializedValueSize))
        .andThen(Cogen.perturb(_, record.leaderEpoch))
        .apply(seed)
    }

  def genProducerRecord[K: Arbitrary, V: Arbitrary]: Gen[ProducerRecord[K, V]] =
    for {
      k         <- Arbitrary.arbitrary[K]
      v         <- Arbitrary.arbitrary[V]
      topic     <- genTopic
      partition <- Gen.option(genPartition)
      headers   <- Arbitrary.arbitrary[Option[Headers]]
      timestamp <- Gen.option(Gen.choose(1L, Long.MaxValue))
    } yield {
      val record = ProducerRecord(
        topic = topic,
        key = k,
        value = v
      ).withHeaders(headers.getOrElse(Headers.empty))

      val withPartition = partition.fold(record)(record.withPartition)

      timestamp.fold(withPartition)(withPartition.withTimestamp)
    }

  implicit def arbProducerRecord[K: Arbitrary, V: Arbitrary]: Arbitrary[ProducerRecord[K, V]] =
    Arbitrary(genProducerRecord[K, V])

  implicit def cogenProducerRecord[K: Cogen, V: Cogen]: Cogen[ProducerRecord[K, V]] =
    Cogen { (seed: Seed, record: ProducerRecord[K, V]) =>
      (Cogen
        .perturb(_: Seed, record.key))
        .andThen(Cogen.perturb(_, record.value))
        .andThen(Cogen.perturb(_, record.topic))
        .andThen(Cogen.perturb(_, record.partition))
        .andThen(Cogen.perturb(_, record.timestamp))
        .andThen(Cogen.perturb(_, record.headers))
        .apply(seed)
    }

  def genCommittableConsumerRecord[
    F[_]: ApplicativeThrow,
    K: Arbitrary,
    V: Arbitrary
  ]: Gen[CommittableConsumerRecord[F, K, V]] =
    for {
      record <- Arbitrary.arbitrary[ConsumerRecord[K, V]]
      offset <- Arbitrary.arbitrary[CommittableOffset[F]]
    } yield CommittableConsumerRecord[F, K, V](record, offset)

  implicit def arbCommittableConsumerRecord[
    F[_]: ApplicativeThrow,
    K: Arbitrary,
    V: Arbitrary
  ]: Arbitrary[CommittableConsumerRecord[F, K, V]] =
    Arbitrary(genCommittableConsumerRecord[F, K, V])

  implicit def cogenCommittableConsumerRecord[F[_], K: Cogen, V: Cogen]
    : Cogen[CommittableConsumerRecord[F, K, V]] =
    Cogen { (seed: Seed, record: CommittableConsumerRecord[F, K, V]) =>
      Cogen.perturbPair(seed, (record.record, record.offset))
    }

  def genCommittableProducerRecords[
    F[_]: ApplicativeThrow,
    K: Arbitrary,
    V: Arbitrary
  ]: Gen[CommittableProducerRecords[F, K, V]] =
    for {
      records <- Arbitrary.arbitrary[List[ProducerRecord[K, V]]]
      offset  <- Arbitrary.arbitrary[CommittableOffset[F]]
    } yield CommittableProducerRecords(records, offset)

  implicit def arbCommittableProducerRecords[
    F[_]: ApplicativeThrow,
    K: Arbitrary,
    V: Arbitrary
  ]: Arbitrary[CommittableProducerRecords[F, K, V]] =
    Arbitrary(genCommittableProducerRecords[F, K, V])

  implicit def cogenCommittableProducerRecords[F[_], K: Cogen, V: Cogen]
    : Cogen[CommittableProducerRecords[F, K, V]] =
    Cogen { (seed: Seed, records: CommittableProducerRecords[F, K, V]) =>
      Cogen.perturbPair(seed, (records.records, records.offset))
    }

}
