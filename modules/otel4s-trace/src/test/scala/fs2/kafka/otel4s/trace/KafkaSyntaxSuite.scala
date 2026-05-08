/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.{IO, Ref}
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.syntax._
import fs2.Chunk
import fs2.Stream

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.otel4s.context.propagation.TextMapGetter

final class KafkaSyntaxSuite extends KafkaTracingTestSupport {

  test("ProducerRecord.injectTracingHeaders delegates to TracedKafkaProducer.injectHeaders") {
    for {
      injectedRecord <- Ref[IO].of(Option.empty[ProducerRecord[String, String]])
      injectedBatch  <- emptyRef[ProducerRecords[String, String]]
      awaitedBatch   <- emptyRef[ProducerRecords[String, String]]
      awaitedSingle  <- emptyRef[ProducerRecord[String, String]]
      producer       <-
        IO(new ProducerSyntaxProbe(injectedRecord, injectedBatch, awaitedBatch, awaitedSingle))
      record  = ProducerRecord("topic", "key", "value")
      result <- record.injectTracingHeaders(producer)
      seen   <- injectedRecord.get
    } yield {
      assertEquals(seen, Some(record))
      assertEquals(TextMapGetter[Headers].get(result.headers, "x-syntax"), Some("record"))
    }
  }

  test("ProducerRecords.injectTracingHeaders delegates to the batch inject overload") {
    for {
      injectedRecord <- emptyRef[ProducerRecord[String, String]]
      injectedBatch  <- Ref[IO].of(Option.empty[ProducerRecords[String, String]])
      awaitedBatch   <- emptyRef[ProducerRecords[String, String]]
      awaitedSingle  <- emptyRef[ProducerRecord[String, String]]
      producer       <-
        IO(new ProducerSyntaxProbe(injectedRecord, injectedBatch, awaitedBatch, awaitedSingle))
      records = ProducerRecords.one(ProducerRecord("topic", "key", "value"))
      result <- records.injectTracingHeaders(producer)
      seen   <- injectedBatch.get
    } yield {
      assertEquals(seen, Some(records))
      assertEquals(result, records)
    }
  }

  test("TracedKafkaProducer.produceTraced delegates to produceAwaited") {
    for {
      injectedRecord <- emptyRef[ProducerRecord[String, String]]
      injectedBatch  <- emptyRef[ProducerRecords[String, String]]
      awaitedBatch   <- Ref[IO].of(Option.empty[ProducerRecords[String, String]])
      awaitedSingle  <- emptyRef[ProducerRecord[String, String]]
      producer       <-
        IO(new ProducerSyntaxProbe(injectedRecord, injectedBatch, awaitedBatch, awaitedSingle))
      records = ProducerRecords.one(ProducerRecord("topic", "key", "value"))
      result <- producer.produceTraced(records)
      seen   <- awaitedBatch.get
    } yield {
      assertEquals(seen, Some(records))
      assertEquals(result, Chunk.empty)
    }
  }

  test("TracedKafkaProducer.produceOneTraced delegates to produceOneAwaited") {
    for {
      injectedRecord <- emptyRef[ProducerRecord[String, String]]
      injectedBatch  <- emptyRef[ProducerRecords[String, String]]
      awaitedBatch   <- emptyRef[ProducerRecords[String, String]]
      awaitedSingle  <- Ref[IO].of(Option.empty[ProducerRecord[String, String]])
      producer       <-
        IO(new ProducerSyntaxProbe(injectedRecord, injectedBatch, awaitedBatch, awaitedSingle))
      record  = ProducerRecord("topic", "key", "value")
      result <- producer.produceOneTraced(record)
      seen   <- awaitedSingle.get
    } yield {
      assertEquals(seen, Some(record))
      assertEquals(result, Chunk.empty)
    }
  }

  test("Chunk[ConsumerRecord].traceReceive delegates to receive") {
    for {
      receiveCommittableChunk <- emptyRef[Chunk[CommittableConsumerRecord[IO, String, String]]]
      receiveChunk            <- Ref[IO].of(Option.empty[Chunk[ConsumerRecord[String, String]]])
      processedRecord         <- emptyRef[ConsumerRecord[String, String]]
      processedCommittable    <- emptyRef[CommittableConsumerRecord[IO, String, String]]
      producer                <- IO(
                    new ConsumerSyntaxProbe(
                      receiveChunk,
                      receiveCommittableChunk,
                      processedRecord,
                      processedCommittable
                    )
                  )
      record  = ConsumerRecord("topic", 0, 42L, "key", "value")
      chunk   = Chunk.singleton(record)
      result <- chunk.traceReceive(IO.pure("ok"))(producer)
      seen   <- receiveChunk.get
    } yield {
      assertEquals(seen, Some(chunk))
      assertEquals(result, "ok")
    }
  }

  test("Chunk[CommittableConsumerRecord].traceReceive delegates to receiveCommittable") {
    val record       = ConsumerRecord("topic", 0, 42L, "key", "value")
    val committable  = committableRecord(record)
    val committables = Chunk.singleton(committable)

    for {
      receiveCommittableChunk <-
        Ref[IO].of(
          Option.empty[Chunk[CommittableConsumerRecord[IO, String, String]]]
        )
      receiveChunk         <- emptyRef[Chunk[ConsumerRecord[String, String]]]
      processedRecord      <- emptyRef[ConsumerRecord[String, String]]
      processedCommittable <- emptyRef[CommittableConsumerRecord[IO, String, String]]
      consumer             <- IO(
                    new ConsumerSyntaxProbe(
                      receiveChunk,
                      receiveCommittableChunk,
                      processedRecord,
                      processedCommittable
                    )
                  )
      result <- committables.traceReceive(IO.pure("ok"))(consumer)
      seen   <- receiveCommittableChunk.get
    } yield {
      assertEquals(seen.map(_.map(_.record).toList), Some(List(record)))
      assertEquals(result, "ok")
    }
  }

  test("ConsumerRecord.traceProcess delegates to process") {
    for {
      receiveChunk            <- emptyRef[Chunk[ConsumerRecord[String, String]]]
      receiveCommittableChunk <- emptyRef[Chunk[CommittableConsumerRecord[IO, String, String]]]
      processedRecord         <- Ref[IO].of(Option.empty[ConsumerRecord[String, String]])
      processedCommittable    <- emptyRef[CommittableConsumerRecord[IO, String, String]]
      consumer                <- IO(
                    new ConsumerSyntaxProbe(
                      receiveChunk,
                      receiveCommittableChunk,
                      processedRecord,
                      processedCommittable
                    )
                  )
      record  = ConsumerRecord("topic", 0, 42L, "key", "value")
      result <- record.traceProcess(IO.pure("ok"))(consumer)
      seen   <- processedRecord.get
    } yield {
      assertEquals(seen, Some(record))
      assertEquals(result, "ok")
    }
  }

  test("CommittableConsumerRecord.traceProcess delegates to the committable process overload") {
    val record      = ConsumerRecord("topic", 0, 42L, "key", "value")
    val committable = committableRecord(record)

    for {
      receiveChunk            <- emptyRef[Chunk[ConsumerRecord[String, String]]]
      receiveCommittableChunk <- emptyRef[Chunk[CommittableConsumerRecord[IO, String, String]]]
      processedRecord         <- emptyRef[ConsumerRecord[String, String]]
      processedCommittable    <- Ref[IO].of(
                                Option.empty[CommittableConsumerRecord[IO, String, String]]
                              )
      consumer <- IO(
                    new ConsumerSyntaxProbe(
                      receiveChunk,
                      receiveCommittableChunk,
                      processedRecord,
                      processedCommittable
                    )
                  )
      result <- committable.traceProcess(IO.pure("ok"))(consumer)
      seen   <- processedCommittable.get
    } yield {
      assertEquals(seen.map(_.record), Some(record))
      assertEquals(result, "ok")
    }
  }

  private def emptyRef[A]: IO[Ref[IO, Option[A]]] =
    Ref[IO].of(Option.empty[A])

  final private class ProducerSyntaxProbe(
    onInjectedRecord: Ref[IO, Option[ProducerRecord[String, String]]],
    onInjectedBatch: Ref[IO, Option[ProducerRecords[String, String]]],
    onAwaitedBatch: Ref[IO, Option[ProducerRecords[String, String]]],
    onAwaitedSingle: Ref[IO, Option[ProducerRecord[String, String]]]
  ) extends TracedKafkaProducer[IO, String, String] {

    override val underlying: KafkaProducer[IO, String, String] =
      StubKafkaProducer.metadataOnly()

    override def injectHeaders(
      record: ProducerRecord[String, String]
    ): IO[ProducerRecord[String, String]] =
      onInjectedRecord
        .set(Some(record))
        .as(record.withHeaders(Headers(Header("x-syntax", "record"))))

    override def injectHeaders(
      records: ProducerRecords[String, String]
    ): IO[ProducerRecords[String, String]] =
      onInjectedBatch.set(Some(records)).as(records)

    override def produceAwaited(
      records: ProducerRecords[String, String]
    ): IO[ProducerResult[String, String]] =
      onAwaitedBatch.set(Some(records)).as(Chunk.empty)

    override def produceOneAwaited(
      record: ProducerRecord[String, String]
    ): IO[ProducerResult[String, String]] =
      onAwaitedSingle.set(Some(record)).as(Chunk.empty)

    override def produceAndCommitTransactionally(
      records: TransactionalProducerRecords[IO, String, String]
    ): IO[ProducerResult[String, String]] =
      IO.raiseError(new AssertionError("unexpected produceAndCommitTransactionally"))

    override def produceTransactionally(
      records: ProducerRecords[String, String]
    ): IO[ProducerResult[String, String]] =
      IO.raiseError(new AssertionError("unexpected produceTransactionally"))

    override def withSerializers[K2, V2](
      keySerializer: KeySerializer[IO, K2],
      valueSerializer: ValueSerializer[IO, V2]
    ): TracedKafkaProducer[IO, K2, V2] =
      this.asInstanceOf[TracedKafkaProducer[IO, K2, V2]]

  }

  final private class ConsumerSyntaxProbe(
    onReceive: Ref[IO, Option[Chunk[ConsumerRecord[String, String]]]],
    onReceiveCommittable: Ref[IO, Option[Chunk[CommittableConsumerRecord[IO, String, String]]]],
    onProcess: Ref[IO, Option[ConsumerRecord[String, String]]],
    onProcessCommittable: Ref[IO, Option[CommittableConsumerRecord[IO, String, String]]]
  ) extends TracedKafkaConsumer[IO, String, String] {

    override val underlying: KafkaConsumer[IO, String, String] =
      StubKafkaConsumer.metadataOnly()

    override def records: Stream[IO, CommittableConsumerRecord[IO, String, String]] =
      Stream.empty

    override def receive[A](records: Chunk[ConsumerRecord[String, String]])(fa: IO[A]): IO[A] =
      onReceive.set(Some(records)) *> fa

    override def receiveCommittable[A](
      records: Chunk[CommittableConsumerRecord[IO, String, String]]
    )(fa: IO[A]): IO[A] =
      onReceiveCommittable.set(Some(records)) *> fa

    override def process[A](record: ConsumerRecord[String, String])(fa: IO[A]): IO[A] =
      onProcess.set(Some(record)) *> fa

    override def process[A](
      record: CommittableConsumerRecord[IO, String, String]
    )(fa: IO[A]): IO[A] =
      onProcessCommittable.set(Some(record)) *> fa

    override def recordsWithProcess[A](
      f: CommittableConsumerRecord[IO, String, String] => IO[A]
    ): Stream[IO, A] =
      Stream.raiseError[IO](new AssertionError("unexpected recordsWithProcess"))

    override def recordsWithProcessOnly[A](
      f: CommittableConsumerRecord[IO, String, String] => IO[A]
    ): Stream[IO, A] =
      Stream.raiseError[IO](new AssertionError("unexpected recordsWithProcessOnly"))

  }

  private def committableRecord(
    record: ConsumerRecord[String, String]
  ): CommittableConsumerRecord[IO, String, String] =
    CommittableConsumerRecord(
      record,
      CommittableOffset(
        new TopicPartition(record.topic, record.partition),
        new OffsetAndMetadata(record.offset),
        KafkaCommitter[IO](
          _ => IO.unit,
          IO.raiseError(new AssertionError("committer metadata should not be called"))
        )
      )
    )

}
