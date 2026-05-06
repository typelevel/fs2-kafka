/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.IO
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.Chunk

import org.typelevel.otel4s.context.propagation.TextMapGetter
import org.typelevel.otel4s.oteljava.testkit.trace.{
  LinkExpectation,
  LinkSetExpectation,
  SpanExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.oteljava.testkit.AttributesExpectation
import org.typelevel.otel4s.semconv.attributes.ServerAttributes
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes

final class KafkaProducerTracingSuite extends KafkaTracingTestSupport {

  final class UnrepresentableKey(val value: String)

  test("produce injects tracing headers and emits a send span") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          _              <- tracedProducer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic", "key", "value"))
               )
          produced <- producer.getCaptured
          spans    <- testkit.finishedSpans
          _        <- IO {
                 assertEquals(produced.size, 1)
                 assertEquals(produced.head.get.topic, "topic")
                 assert(produced.head.get.headers.toChain.nonEmpty)
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
                           MessagingExperimentalAttributes.MessagingClientId("producer-client"),
                           MessagingExperimentalAttributes.MessagingKafkaMessageKey("key")
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("awaited producer helpers complete traced sends in one effect") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          _              <- tracedProducer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic-a", "key-a", "value-a"))
               )
          _ <- tracedProducer.produceOneAwaited(
                 ProducerRecord("topic-b", "key-b", "value-b")
               )
          completionCount <- producer.getCompletions
          produced        <- producer.getCaptured
          spans           <- testkit.finishedSpans
          _               <- IO {
                 assertEquals(completionCount, 2)
                 assertEquals(produced.size, 1)
                 assertEquals(produced.head.get.topic, "topic-b")
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

  test("single-message send adds broker partition and offset after completion") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          tracedProducer <- testkit.tracedProducer[String, String](
                              StubKafkaProducer.metadataOnly("producer-client")
                            )
          _     <- tracedProducer.produceOneAwaited(ProducerRecord("topic", "key", "value"))
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                           MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("batch produce creates per-message creation contexts and links the batch send span") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          records         = Chunk(
                      ProducerRecord("topic-a", "key-a", "value-a"),
                      ProducerRecord("topic-b", "key-b", null.asInstanceOf[String]).withPartition(3)
                    )
          _        <- tracedProducer.produceAwaited(records)
          produced <- producer.getCaptured
          spans    <- testkit.finishedSpans
          _        <- IO {
                 assertEquals(produced.size, 2)

                 val traceparents = produced
                   .toList
                   .map { record =>
                     TextMapGetter[Headers]
                       .get(record.headers, "traceparent")
                       .getOrElse(fail("missing produced traceparent"))
                   }
                 assert(traceparents.distinct.size == 2)

                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.producer("create topic-a").scopeName("fs2.kafka")
                     ),
                     root(
                       SpanExpectation.producer("create topic-b").scopeName("fs2.kafka")
                     ),
                     root(
                       SpanExpectation
                         .client("send")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where(
                             "must keep tombstone detail off the batch span"
                           )(
                             _.get(MessagingExperimentalAttributes.MessagingKafkaMessageTombstone)
                               .isEmpty
                           )
                         )
                         .links(
                           LinkSetExpectation.exactly(
                             LinkExpectation
                               .any
                               .attributesSubset(
                                 MessagingExperimentalAttributes.MessagingDestinationName("topic-a")
                               ),
                             LinkExpectation
                               .any
                               .attributesSubset(
                                 MessagingExperimentalAttributes.MessagingDestinationName(
                                   "topic-b"
                                 ),
                                 MessagingExperimentalAttributes.MessagingKafkaMessageTombstone(
                                   true
                                 )
                               )
                           )
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("batch send does not emit span-level partition id for mixed-topic same-number partitions") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          records         = Chunk(
                      ProducerRecord("topic-a", "key-a", "value-a").withPartition(0),
                      ProducerRecord("topic-b", "key-b", "value-b").withPartition(0)
                    )
          _     <- tracedProducer.produceAwaited(records)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.producer("create topic-a").scopeName("fs2.kafka")
                     ),
                     root(
                       SpanExpectation.producer("create topic-b").scopeName("fs2.kafka")
                     ),
                     root(
                       SpanExpectation
                         .client("send")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where(
                             "must omit span-level partition id for mixed-topic batches"
                           )(
                             _.get(MessagingExperimentalAttributes.MessagingDestinationPartitionId)
                               .isEmpty
                           )
                         )
                         .links(
                           LinkSetExpectation
                             .count(2)
                             .and(
                               LinkSetExpectation.forall(
                                 LinkExpectation
                                   .any
                                   .attributesSubset(
                                     MessagingExperimentalAttributes
                                       .MessagingDestinationPartitionId("0")
                                   )
                               )
                             )
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("configured server address is emitted on spans") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config = KafkaTracer
          .Config
          .default
          .withServerAddress(
            "kafka.internal",
            Some(9092)
          )

        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer, config)
          _              <- tracedProducer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic", "key", "value"))
               )
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           ServerAttributes.ServerAddress("kafka.internal"),
                           ServerAttributes.ServerPort(9092L)
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("producer send spans omit endpoint metadata unless configured explicitly") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producerTracer <- testkit.tracedProducer[String, String](
                              StubKafkaProducer.metadataOnly("producer-client")
                            )
          _ <- producerTracer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic", "key", "value"))
               )
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where(
                             "must omit endpoint metadata unless configured explicitly"
                           )(attrs =>
                             attrs.get(ServerAttributes.ServerAddress).isEmpty &&
                               attrs.get(ServerAttributes.ServerPort).isEmpty
                           )
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("single-message send omits messaging.kafka.message.key for null and unrepresentable keys") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          nullProducer         <- StubKafkaProducer.recorder[String, String]()
          badProducer          <- StubKafkaProducer.recorder[UnrepresentableKey, String]()
          stringProducerTracer <- testkit.tracedProducer[String, String](nullProducer)
          badProducerTracer    <- testkit.tracedProducer[UnrepresentableKey, String](badProducer)
          _                    <- stringProducerTracer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic", null.asInstanceOf[String], "value"))
               )
          _ <- badProducerTracer.produceAwaited(
                 ProducerRecords.one(
                   ProducerRecord("topic", new UnrepresentableKey("secret"), "value")
                 )
               )
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where("must omit message key for null keys")(
                             _.get(MessagingExperimentalAttributes.MessagingKafkaMessageKey).isEmpty
                           )
                         )
                     ),
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where(
                             "must omit message key for unrepresentable keys"
                           )(
                             _.get(MessagingExperimentalAttributes.MessagingKafkaMessageKey).isEmpty
                           )
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("single-message send emits messaging.kafka.message.tombstone for null values") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          producerTracer <- testkit.tracedProducer[String, String](producer)
          _              <- producerTracer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic", "key", null.asInstanceOf[String]))
               )
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingKafkaMessageTombstone(true)
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

}
