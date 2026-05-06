/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.IO
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._

import org.typelevel.otel4s.context.propagation.{TextMapGetter, TextMapUpdater}
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, TraceForestExpectation}

final class KafkaPropagationSuite extends KafkaTracingTestSupport {

  test("headers text map updater replaces existing values for the same key") {
    val headers =
      TextMapUpdater[Headers].updated(
        TextMapUpdater[Headers].updated(
          Headers.empty,
          "traceparent",
          "old"
        ),
        "traceparent",
        "new"
      )

    assertEquals(
      TextMapGetter[Headers].get(headers, "traceparent"),
      Some("new")
    )
    assertEquals(headers.toChain.iterator.count(_.key == "traceparent"), 1)
  }

  test("produce preserves an existing message creation context and links the send span to it") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producer       <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          prepared       <- appTracer
                        .rootSpan("upstream-create-context")
                        .surround {
                          tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
                        }
          originalTraceparent = TextMapGetter[Headers]
                                  .get(prepared.headers, "traceparent")
                                  .getOrElse(fail("missing prepared traceparent"))
          _        <- tracedProducer.produceOneAwaited(prepared)
          produced <- producer.getCaptured
          spans    <- testkit.finishedSpans
          _        <- IO {
                 val producedRecord      = produced.head.getOrElse(fail("missing produced record"))
                 val producedTraceparent = TextMapGetter[Headers]
                   .get(producedRecord.headers, "traceparent")
                   .getOrElse(fail("missing produced traceparent"))

                 assertEquals(producedTraceparent, originalTraceparent)
                 assertExpected(
                   spans,
                   TraceForestExpectation.unordered(
                     root(
                       SpanExpectation
                         .internal("upstream-create-context")
                         .scopeName("fs2.kafka.otel4s.tests")
                     ),
                     root(
                       SpanExpectation.client("send topic").scopeName("fs2.kafka").linkCount(1)
                     )
                   )
                 )
               }
        } yield ()
      }
  }

  test("injectHeaders preserves an existing message creation context") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          producerTracer <- producerTracer
          prepared       <- testkit
                        .appTracer
                        .rootSpan("upstream-create-context")
                        .surround {
                          producerTracer.injectHeaders(ProducerRecord("topic", "key", "value"))
                        }
          originalTraceparent = TextMapGetter[Headers]
                                  .get(prepared.headers, "traceparent")
                                  .getOrElse(fail("missing prepared traceparent"))
          reinjected <- testkit
                          .appTracer
                          .rootSpan("different-current-context")
                          .surround {
                            producerTracer.injectHeaders(prepared)
                          }
          _ <- IO {
                 val reinjectedTraceparent = TextMapGetter[Headers]
                   .get(reinjected.headers, "traceparent")
                   .getOrElse(fail("missing reinjected traceparent"))

                 assertEquals(reinjectedTraceparent, originalTraceparent)
                 assertEquals(reinjected.headers.toChain.iterator.count(_.key == "traceparent"), 1)
               }
        } yield ()
      }
  }

  test("injectHeaders preserves an existing non-W3C creation context recognized by the configured propagator") {
    KafkaTracerTestkit
      .create(propagators = Seq(CustomTraceContextPropagator))
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          producerTracer <- producerTracer
          prepared       <- testkit
                        .appTracer
                        .rootSpan("upstream-custom-context")
                        .surround {
                          producerTracer.injectHeaders(ProducerRecord("topic", "key", "value"))
                        }
          originalCustomHeader =
            TextMapGetter[Headers]
              .get(prepared.headers, CustomTraceContextPropagator.customPropagationHeader)
              .getOrElse(fail("missing prepared custom propagation header"))
          reinjected <- testkit
                          .appTracer
                          .rootSpan("different-current-context")
                          .surround {
                            producerTracer.injectHeaders(prepared)
                          }
          _ <- IO {
                 val reinjectedCustomHeader = TextMapGetter[Headers]
                   .get(reinjected.headers, CustomTraceContextPropagator.customPropagationHeader)
                   .getOrElse(fail("missing reinjected custom propagation header"))

                 assertEquals(reinjectedCustomHeader, originalCustomHeader)
                 assertEquals(
                   reinjected
                     .headers
                     .toChain
                     .iterator
                     .count(_.key == CustomTraceContextPropagator.customPropagationHeader),
                   1
                 )
                 assertEquals(
                   TextMapGetter[Headers].get(reinjected.headers, "traceparent"),
                   None
                 )
               }
        } yield ()
      }
  }

  test("injectHeaders replaces malformed propagation input with the current context") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        val malformed = ProducerRecord("topic", "key", "value").withHeaders(
          Headers(Header("traceparent", "00-not-a-valid-traceparent"))
        )

        for {
          producerTracer <- producerTracer
          propagated     <- testkit
                          .appTracer
                          .rootSpan("replacement-context")
                          .surround {
                            producerTracer.injectHeaders(malformed)
                          }
          _ <- IO {
                 val traceparent = TextMapGetter[Headers]
                   .get(propagated.headers, "traceparent")
                   .getOrElse(fail("missing propagated traceparent"))

                 assertNotEquals(traceparent, "00-not-a-valid-traceparent")
                 assertEquals(propagated.headers.toChain.iterator.count(_.key == "traceparent"), 1)
               }
        } yield ()
      }
  }

}
