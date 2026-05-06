/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import scala.concurrent.duration._

import cats.effect.{Deferred, IO, Ref}
import fs2.kafka._
import fs2.Chunk

import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, TraceForestExpectation}
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes

final class KafkaTransactionalTracingSuite extends KafkaTracingTestSupport {

  test("the underlying producer remains available for the two-phase producer contract") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          prepared       <- tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          staged         <- tracedProducer
                      .underlying
                      .produce(
                        ProducerRecords.one(prepared)
                      )
          producedAfterOuter    <- producer.getCaptured
          completionsAfterOuter <- producer.getCompletions
          spansAfterOuter       <- testkit.finishedSpans
          _                     <- staged
          completionsAfterInner <- producer.getCompletions
          spansAfterInner       <- testkit.finishedSpans
          _                     <- IO {
                 assertEquals(producedAfterOuter.size, 1)
                 assertEquals(producedAfterOuter.head.get.topic, "topic")
                 assertEquals(completionsAfterOuter, 0)
                 assertEquals(completionsAfterInner, 1)
                 assertEquals(spansAfterOuter, Nil)
                 assertEquals(spansAfterInner, Nil)
               }
        } yield ()
      }
  }

  test("produceTransactionally emits a send span and preserves transaction semantics") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          _              <- tracedProducer.produceTransactionally(
                 ProducerRecords.one(ProducerRecord("topic", "key", "value"))
               )
          produced        <- producer.getCaptured
          completionCount <- producer.getCompletions
          txCount         <- producer.getTransactionUses
          offsetsSent     <- producer.getSentOffsets
          spans           <- testkit.finishedSpans
          _               <- IO {
                 assertEquals(produced.size, 1)
                 assert(produced.head.get.headers.toChain.nonEmpty)
                 assertEquals(completionCount, 1)
                 assertEquals(txCount, 1)
                 assertEquals(offsetsSent, Vector.empty)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingSystem(
                             MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                           ),
                           MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                           MessagingExperimentalAttributes.MessagingOperationName("send"),
                           MessagingExperimentalAttributes.MessagingOperationType("send"),
                           MessagingExperimentalAttributes.MessagingClientId("producer-client")
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("produceAndCommitTransactionally emits a send span and forwards offsets") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          groupMetadata   = consumerGroupMetadata("consumer-group")
          topicPartition  = new TopicPartition("input", 0)
          offsetMetadata  = new OffsetAndMetadata(43L)
          committer       = KafkaCommitter[IO](
                        _ => IO.unit,
                        IO.pure(groupMetadata)
                      )
          offset  = CommittableOffset[IO](topicPartition, offsetMetadata, committer)
          records =
            Chunk(
              CommittableProducerRecords.one(ProducerRecord("topic", "key", "value"), offset)
            )
          _                <- tracedProducer.produceAndCommitTransactionally(records)
          produced         <- producer.getCaptured
          completionCount  <- producer.getCompletions
          txCount          <- producer.getTransactionUses
          offsetsSentValue <- producer.getSentOffsets
          spans            <- testkit.finishedSpans
          _                <- IO {
                 assertEquals(produced.size, 1)
                 assert(produced.head.get.headers.toChain.nonEmpty)
                 assertEquals(completionCount, 1)
                 assertEquals(txCount, 1)
                 assertEquals(offsetsSentValue.size, 1)
                 assertEquals(offsetsSentValue.head._1, Map(topicPartition -> offsetMetadata))
                 assertEquals(offsetsSentValue.head._2, groupMetadata)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingSystem(
                             MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                           ),
                           MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                           MessagingExperimentalAttributes.MessagingOperationName("send"),
                           MessagingExperimentalAttributes.MessagingOperationType("send"),
                           MessagingExperimentalAttributes.MessagingClientId("producer-client")
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("produceAndCommitTransactionally preserves multi-committer concurrency parity") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer        <- StubKafkaProducer.recorder[String, String]()
          tracedProducer  <- testkit.tracedProducer(producer)
          enteredMetadata <- Ref[IO].of(0)
          bothEntered     <- Deferred[IO, Unit]
          groupMetadata1   = consumerGroupMetadata("consumer-group-1")
          groupMetadata2   = consumerGroupMetadata("consumer-group-2")
          committer1       = KafkaCommitter[IO](
                         _ => IO.unit,
                         enteredMetadata
                           .updateAndGet(_ + 1)
                           .flatMap { n =>
                             (if (n == 2) bothEntered.complete(()).void else IO.unit) *> bothEntered
                               .get
                               .as(groupMetadata1)
                           }
                       )
          committer2 = KafkaCommitter[IO](
                         _ => IO.unit,
                         enteredMetadata
                           .updateAndGet(_ + 1)
                           .flatMap { n =>
                             (if (n == 2) bothEntered.complete(()).void else IO.unit) *> bothEntered
                               .get
                               .as(groupMetadata2)
                           }
                       )
          offset1 = CommittableOffset[IO](
                      new TopicPartition("input", 0),
                      new OffsetAndMetadata(10L),
                      committer1
                    )
          offset2 = CommittableOffset[IO](
                      new TopicPartition("input", 1),
                      new OffsetAndMetadata(20L),
                      committer2
                    )
          records = Chunk(
                      CommittableProducerRecords.one(
                        ProducerRecord("topic-a", "key-a", "value-a"),
                        offset1
                      ),
                      CommittableProducerRecords.one(
                        ProducerRecord("topic-b", "key-b", "value-b"),
                        offset2
                      )
                    )
          _                <- tracedProducer.produceAndCommitTransactionally(records).timeout(2.seconds)
          entered          <- enteredMetadata.get
          completionsCount <- producer.getCompletions
          offsetsSentCount <- producer.getSentOffsets.map(_.size)
          spans            <- testkit.finishedSpans
          _                <- IO {
                 assertEquals(entered, 2)
                 assertEquals(completionsCount, 2)
                 assertEquals(offsetsSentCount, 2)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.producer("send topic-a").scopeName("fs2.kafka")
                     ),
                     root(
                       SpanExpectation.producer("send topic-b").scopeName("fs2.kafka")
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  @annotation.nowarn("msg=deprecated")
  private def consumerGroupMetadata(groupId: String): ConsumerGroupMetadata =
    new ConsumerGroupMetadata(groupId)

}
