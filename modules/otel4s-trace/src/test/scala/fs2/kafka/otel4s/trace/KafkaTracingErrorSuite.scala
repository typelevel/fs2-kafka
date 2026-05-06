/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.IO
import fs2.kafka.{ConsumerRecord, ProducerRecord, ProducerRecords}
import fs2.Chunk

import org.typelevel.otel4s.oteljava.testkit.trace.{
  SpanExpectation,
  StatusExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.semconv.attributes.ErrorAttributes

final class KafkaTracingErrorSuite extends KafkaTracingTestSupport {

  test("failed send emits error.type and error status") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          tracedProducer <- testkit.tracedProducer(
                              StubKafkaProducer.failingAwait[String, String](
                                new RuntimeException("send failed")
                              )
                            )
          result <- tracedProducer
                      .produceAwaited(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
                      .attempt
          spans <- testkit.finishedSpans
          _     <- IO {
                 assert(result.isLeft)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .producer("send topic")
                         .scopeName("fs2.kafka")
                         .status(StatusExpectation.error)
                         .attributesSubset(
                           ErrorAttributes.ErrorType("java.lang.RuntimeException")
                         )
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("failed receive emits error.type and error status") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          boom    = new RuntimeException("receive failed")
          record  = ConsumerRecord("topic", 0, 1L, "key", "value")
          result <-
            consumerTracer.receive(Chunk.singleton(record))(IO.raiseError[Unit](boom)).attempt
          spans <- testkit.finishedSpans
          _     <- IO {
                 assert(result.isLeft)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .client("poll topic")
                         .scopeName("fs2.kafka")
                         .status(StatusExpectation.error)
                         .attributesSubset(ErrorAttributes.ErrorType("java.lang.RuntimeException"))
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("failed process emits error.type and error status") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
                              StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
                            )
          boom    = new RuntimeException("process failed")
          record  = ConsumerRecord("topic", 0, 42L, "key", "value")
          result <- consumerTracer.process(record)(IO.raiseError[Unit](boom)).attempt
          spans  <- testkit.finishedSpans
          _      <- IO {
                 assert(result.isLeft)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .consumer("process topic")
                         .scopeName("fs2.kafka")
                         .status(StatusExpectation.error)
                         .attributesSubset(ErrorAttributes.ErrorType("java.lang.RuntimeException"))
                     )
                   )
                 )
               }
        } yield ()
      }
  }

}
