/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.IO
import fs2.kafka._
import fs2.Chunk

import org.typelevel.otel4s.oteljava.testkit.trace._
import org.typelevel.otel4s.oteljava.testkit.AttributesExpectation
import org.typelevel.otel4s.semconv.attributes.ServerAttributes
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.trace.Tracer

final class KafkaConsumerTracingSuite extends KafkaTracingTestSupport {

  final class UnrepresentableKey(val value: String)
  final class CustomKey(val value: String)

  test("process links propagated context and emits a process span") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producerTracer <- testkit.tracedProducer[String, String](
                              StubKafkaProducer.metadataOnly("producer-client")
                            )
          produced <- Tracer[IO]
                        .rootSpan("producer-root")
                        .surround {
                          for {
                            span       <- Tracer[IO].currentSpanOrThrow
                            propagated <-
                              producerTracer.injectHeaders(ProducerRecord("topic", "key", "value"))
                          } yield (span.context, propagated)
                        }
          (producerRootContext, propagated) = produced
          consumed                          = ConsumerRecord("topic", 0, 42L, "key", "value").withHeaders(propagated.headers)
          consumerTracer                   <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          _     <- consumerTracer.process(consumed)(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")
                     ),
                     TraceExpectation.leaf(
                       SpanExpectation
                         .consumer("process topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingSystem(
                             MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                           ),
                           MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                           MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                           MessagingExperimentalAttributes.MessagingOperationName("process"),
                           MessagingExperimentalAttributes.MessagingOperationType("process"),
                           MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                           MessagingExperimentalAttributes.MessagingConsumerGroupName(
                             "consumer-group"
                           ),
                           MessagingExperimentalAttributes.MessagingKafkaMessageKey("key"),
                           MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
                         )
                         .noParentSpanContext
                         .links(
                           LinkSetExpectation.exactly(
                             LinkExpectation
                               .any
                               .spanContext(
                                 SpanContextExpectation
                                   .any
                                   .traceIdHex(producerRootContext.traceIdHex)
                                   .spanIdHex(producerRootContext.spanIdHex)
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

  test("receive emits a poll span and links to message creation contexts") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producerTracer <- testkit.tracedProducer[String, String](
                              StubKafkaProducer.metadataOnly("producer-client")
                            )
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          produced <- Tracer[IO]
                        .rootSpan("producer-root")
                        .surround {
                          for {
                            span    <- Tracer[IO].currentSpanOrThrow
                            record1 <-
                              producerTracer.injectHeaders(ProducerRecord("topic", "k1", "v1"))
                            record2 <-
                              producerTracer.injectHeaders(ProducerRecord("topic", "k2", "v2"))
                          } yield (span.context, record1, record2)
                        }
          (producerRootContext, record1, record2) = produced
          records                                 = Chunk(
                      ConsumerRecord("topic", 0, 1L, "k1", "v1").withHeaders(record1.headers),
                      ConsumerRecord("topic", 0, 2L, "k2", "v2").withHeaders(record2.headers)
                    )
          _     <- consumerTracer.receive(records)(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")
                     ),
                     root(
                       SpanExpectation
                         .client("poll topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingSystem(
                             MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                           ),
                           MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                           MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                           MessagingExperimentalAttributes.MessagingOperationName("poll"),
                           MessagingExperimentalAttributes.MessagingOperationType("receive"),
                           MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                           MessagingExperimentalAttributes.MessagingConsumerGroupName(
                             "consumer-group"
                           ),
                           MessagingExperimentalAttributes.MessagingBatchMessageCount(2L)
                         )
                         .noParentSpanContext
                         .links(
                           LinkSetExpectation.exactly(
                             LinkExpectation
                               .any
                               .spanContext(
                                 SpanContextExpectation
                                   .any
                                   .traceIdHex(producerRootContext.traceIdHex)
                                   .spanIdHex(producerRootContext.spanIdHex)
                               ),
                             LinkExpectation
                               .any
                               .spanContext(
                                 SpanContextExpectation
                                   .any
                                   .traceIdHex(producerRootContext.traceIdHex)
                                   .spanIdHex(producerRootContext.spanIdHex)
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

  test("receive keeps heterogeneous batch detail on links when span-level values vary") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producerTracer <- testkit.tracedProducer[String, String](
                              StubKafkaProducer.metadataOnly("producer-client")
                            )
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          produced <- testkit
                        .appTracer
                        .rootSpan("producer-root")
                        .surround {
                          for {
                            record1 <- producerTracer.injectHeaders(
                                         ProducerRecord("topic-a", "key-a", "value-a")
                                       )
                            record2 <-
                              producerTracer.injectHeaders(
                                ProducerRecord("topic-b", "key-b", null.asInstanceOf[String])
                                  .withPartition(3)
                              )
                          } yield (record1, record2)
                        }
          (record1, record2) = produced
          records            = Chunk(
                      ConsumerRecord("topic-a", 0, 1L, "key-a", "value-a").withHeaders(
                        record1.headers
                      ),
                      ConsumerRecord("topic-b", 3, 2L, "key-b", null.asInstanceOf[String])
                        .withHeaders(record2.headers)
                    )
          _     <- consumerTracer.receive(records)(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")
                     ),
                     root(
                       SpanExpectation
                         .client("poll")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where(
                             "must keep heterogeneous batch detail off the span attributes"
                           )(attrs =>
                             attrs
                               .get(MessagingExperimentalAttributes.MessagingDestinationName)
                               .isEmpty &&
                               attrs
                                 .get(
                                   MessagingExperimentalAttributes.MessagingDestinationPartitionId
                                 )
                                 .isEmpty &&
                               attrs
                                 .get(
                                   MessagingExperimentalAttributes.MessagingKafkaMessageTombstone
                                 )
                                 .isEmpty &&
                               attrs
                                 .get(MessagingExperimentalAttributes.MessagingBatchMessageCount)
                                 .contains(
                                   MessagingExperimentalAttributes.MessagingBatchMessageCount(2L)
                                 )
                           )
                         )
                         .links(
                           LinkSetExpectation.exactly(
                             LinkExpectation
                               .any
                               .attributesSubset(
                                 MessagingExperimentalAttributes.MessagingDestinationName(
                                   "topic-a"
                                 ),
                                 MessagingExperimentalAttributes.MessagingDestinationPartitionId(
                                   "0"
                                 ),
                                 MessagingExperimentalAttributes.MessagingKafkaMessageKey("key-a"),
                                 MessagingExperimentalAttributes.MessagingKafkaOffset(1L)
                               ),
                             LinkExpectation
                               .any
                               .attributesSubset(
                                 MessagingExperimentalAttributes.MessagingDestinationName(
                                   "topic-b"
                                 ),
                                 MessagingExperimentalAttributes.MessagingDestinationPartitionId(
                                   "3"
                                 ),
                                 MessagingExperimentalAttributes.MessagingKafkaMessageKey("key-b"),
                                 MessagingExperimentalAttributes.MessagingKafkaMessageTombstone(
                                   true
                                 ),
                                 MessagingExperimentalAttributes.MessagingKafkaOffset(2L)
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

  test("receive does not emit span-level partition id for mixed-topic same-number partitions") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producerTracer <- testkit.tracedProducer[String, String](
                              StubKafkaProducer.metadataOnly("producer-client")
                            )
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          produced <- appTracer
                        .rootSpan("producer-root")
                        .surround {
                          for {
                            record1 <- producerTracer.injectHeaders(
                                         ProducerRecord("topic-a", "key-a", "value-a")
                                           .withPartition(0)
                                       )
                            record2 <- producerTracer.injectHeaders(
                                         ProducerRecord("topic-b", "key-b", "value-b")
                                           .withPartition(0)
                                       )
                          } yield (record1, record2)
                        }
          (record1, record2) = produced
          records            = Chunk(
                      ConsumerRecord("topic-a", 0, 1L, "key-a", "value-a").withHeaders(
                        record1.headers
                      ),
                      ConsumerRecord("topic-b", 0, 2L, "key-b", "value-b").withHeaders(
                        record2.headers
                      )
                    )
          _     <- consumerTracer.receive(records)(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")
                     ),
                     root(
                       SpanExpectation
                         .client("poll")
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

  test("receive and process do not create extracted-context links when headers are absent") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val record = ConsumerRecord("topic", 0, 42L, "key", "value")

        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          _ <- testkit
                 .appTracer
                 .rootSpan("ambient-span")
                 .surround {
                   consumerTracer.receive(Chunk.singleton(record)) {
                     consumerTracer.process(record)(IO.unit)
                   }
                 }
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.ordered(
                     root(
                       SpanExpectation.internal("ambient-span").scopeName("fs2.kafka.otel4s.tests"),
                       TraceExpectation.ordered(
                         SpanExpectation
                           .client("poll topic")
                           .scopeName("fs2.kafka")
                           .attributesSubset(
                             MessagingExperimentalAttributes.MessagingSystem(
                               MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                             ),
                             MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                             MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                             MessagingExperimentalAttributes.MessagingOperationName("poll"),
                             MessagingExperimentalAttributes.MessagingOperationType("receive"),
                             MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                             MessagingExperimentalAttributes.MessagingConsumerGroupName(
                               "consumer-group"
                             ),
                             MessagingExperimentalAttributes.MessagingKafkaMessageKey("key"),
                             MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
                           )
                           .linkCount(0),
                         TraceExpectation.leaf(
                           SpanExpectation
                             .consumer("process topic")
                             .scopeName("fs2.kafka")
                             .attributesSubset(
                               MessagingExperimentalAttributes.MessagingSystem(
                                 MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                               ),
                               MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                               MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                               MessagingExperimentalAttributes.MessagingOperationName("process"),
                               MessagingExperimentalAttributes.MessagingOperationType("process"),
                               MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                               MessagingExperimentalAttributes.MessagingConsumerGroupName(
                                 "consumer-group"
                               ),
                               MessagingExperimentalAttributes.MessagingKafkaMessageKey("key"),
                               MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
                             )
                             .linkCount(0)
                         )
                       )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("single-message send and process use a custom KafkaMessageKey instance when provided") {
    implicit val customKafkaMessageKey: KafkaMessageKey[CustomKey] =
      KafkaMessageKey.instance(key => Some(s"custom:${key.value}"))

    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer       <- StubKafkaProducer.recorder[CustomKey, String]()
          producerTracer <- testkit.tracedProducer[CustomKey, String](producer)
          consumerTracer <- testkit.tracedConsumer[CustomKey, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          _ <- producerTracer.produceAwaited(
                 ProducerRecords.one(ProducerRecord("topic", new CustomKey("key"), "value"))
               )
          _ <- consumerTracer.process(
                 ConsumerRecord("topic", 0, 42L, new CustomKey("key"), "value")
               )(IO.unit)
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
                           MessagingExperimentalAttributes.MessagingKafkaMessageKey("custom:key")
                         )
                     ),
                     root(
                       SpanExpectation
                         .consumer("process topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingKafkaMessageKey("custom:key")
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test(
    "single-message process omits messaging.kafka.message.key for null and unrepresentable keys"
  ) {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          consumerTracer <- testkit.tracedConsumer[AnyRef, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          _ <- consumerTracer.process(
                 ConsumerRecord("topic", 0, 42L, null.asInstanceOf[AnyRef], "value")
               )(IO.unit)
          _ <- consumerTracer.process(
                 ConsumerRecord("topic", 0, 43L, new UnrepresentableKey("secret"), "value")
               )(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .consumer("process topic")
                         .scopeName("fs2.kafka")
                         .attributes(
                           AttributesExpectation.where("must omit message key for null keys")(
                             _.get(MessagingExperimentalAttributes.MessagingKafkaMessageKey).isEmpty
                           )
                         )
                     ),
                     root(
                       SpanExpectation
                         .consumer("process topic")
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

  test("single-message receive and process emit messaging.kafka.message.tombstone for null values") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          record = ConsumerRecord("topic", 0, 42L, "key", null.asInstanceOf[String])
          _     <- consumerTracer.receive(Chunk.singleton(record))(IO.unit)
          _     <- consumerTracer.process(record)(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .client("poll topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           MessagingExperimentalAttributes.MessagingKafkaOffset(42L),
                           MessagingExperimentalAttributes.MessagingKafkaMessageTombstone(true)
                         )
                     ),
                     root(
                       SpanExpectation
                         .consumer("process topic")
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

  test("configured server address is emitted on consumer receive and process spans") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group"),
                              KafkaTracer
                                .Config
                                .default
                                .withServerAddress("kafka.internal", Some(9092))
                            )
          record = ConsumerRecord("topic", 0, 42L, "key", "value")
          _     <- consumerTracer.receive(Chunk.singleton(record))(IO.unit)
          _     <- consumerTracer.process(record)(IO.unit)
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .client("poll topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           ServerAttributes.ServerAddress("kafka.internal"),
                           ServerAttributes.ServerPort(9092L)
                         )
                     ),
                     root(
                       SpanExpectation
                         .consumer("process topic")
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

}
