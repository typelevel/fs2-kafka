/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.IO
import fs2.kafka._
import fs2.Chunk

import org.typelevel.otel4s.oteljava.testkit.trace.{
  LinkSetExpectation,
  SpanExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.semconv.attributes.ServerAttributes
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.Attributes

class KafkaTracerConstAttributesSuite extends KafkaTracingTestSupport {

  private val tracerScopeName = "fs2.kafka.otel4s.tests"

  private val config =
    KafkaTracer
      .Config
      .default
      .withConstAttributes(
        Attributes(
          ServerAttributes.ServerAddress("kafka.internal"),
          ServerAttributes.ServerPort(9092L)
        )
      )

  private val endpointAttributes =
    Attributes(
      ServerAttributes.ServerAddress("kafka.internal"),
      ServerAttributes.ServerPort(9092L)
    )

  test("configured constant endpoint attributes are emitted on create and send spans") {
    KafkaTracerTestkit
      .create(tracerScopeName)
      .use { testkit =>
        for {
          producerTracer <-
            testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly(), config)
          _ <- producerTracer.produceAwaited(
                 ProducerRecords(
                   List(
                     ProducerRecord("topic", "key-1", "value-1"),
                     ProducerRecord("topic", "key-2", "value-2")
                   )
                 )
               )
          spans <- testkit.finishedSpans
          _     <- IO {
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("create topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           endpointAttributes + MessagingExperimentalAttributes
                             .MessagingKafkaMessageKey("key-1")
                         )
                     ),
                     root(
                       SpanExpectation
                         .producer("create topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(
                           endpointAttributes + MessagingExperimentalAttributes
                             .MessagingKafkaMessageKey("key-2")
                         )
                     ),
                     root(
                       SpanExpectation
                         .client("send topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(endpointAttributes)
                         .links(LinkSetExpectation.count(2))
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("configured constant endpoint attributes are emitted on receive and process spans") {
    KafkaTracerTestkit
      .create(tracerScopeName)
      .use { testkit =>
        val record = ConsumerRecord("topic", 0, 42L, "key", "value")

        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group"),
                              config
                            )
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
                         .attributesSubset(endpointAttributes)
                     ),
                     root(
                       SpanExpectation
                         .consumer("process topic")
                         .scopeName("fs2.kafka")
                         .attributesSubset(endpointAttributes)
                     )
                   )
                 )
               }
        } yield ()
      }
  }

}
