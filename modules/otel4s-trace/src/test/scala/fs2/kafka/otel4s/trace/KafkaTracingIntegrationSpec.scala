/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.Chunk

import io.opentelemetry.sdk.trace.data.SpanData
import org.typelevel.otel4s.context.propagation.TextMapGetter
import org.typelevel.otel4s.oteljava.testkit.trace.{
  SpanExpectation,
  TraceExpectation,
  TraceExpectations,
  TraceForestExpectation
}

final class KafkaTracingIntegrationSpec extends BaseKafkaSpec {

  describe("Kafka tracing integration") {
    it("should propagate context through Kafka and emit send, receive, and process spans") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 1).get

        KafkaTracerTestkit
          .create("fs2.kafka.otel4s.integration")
          .use { testkit =>
            import testkit.{appTracer, tracerProvider}

            for {
              kafkaTracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
              _           <- appTracer
                     .rootSpan("producer-root")
                     .surround {
                       KafkaProducer
                         .resource(producerSettings[IO])
                         .use { producer =>
                           kafkaTracer
                             .producer(producer)
                             .produceOneAwaited(
                               ProducerRecord(topic, "key", "value")
                             )
                             .void
                         }
                     }
              consumedRecord <- KafkaConsumer
                                  .stream(consumerSettings[IO].withGroupId(s"group-$topic"))
                                  .subscribeTo(topic)
                                  .flatMap { consumer =>
                                    val tracedConsumer = kafkaTracer.consumer(consumer)
                                    consumer
                                      .records
                                      .take(1)
                                      .evalMap { record =>
                                        tracedConsumer.receiveCommittable(Chunk.singleton(record)) {
                                          tracedConsumer.process(record)(IO.pure(record.record))
                                        }
                                      }
                                  }
                                  .interruptAfter(20.seconds)
                                  .compile
                                  .lastOrError
              spans <- testkit.finishedSpans
              _     <- IO {
                     assert(
                       TextMapGetter[Headers].get(consumedRecord.headers, "traceparent").nonEmpty
                     )
                     assertExpected(
                       spans,
                       TraceForestExpectation.unordered(
                         root(
                           SpanExpectation
                             .internal("producer-root")
                             .scopeName("fs2.kafka.otel4s.integration"),
                           TraceExpectation.leaf(
                             SpanExpectation.producer(s"send $topic").scopeName("fs2.kafka")
                           )
                         ),
                         root(
                           SpanExpectation
                             .client(s"poll $topic")
                             .scopeName("fs2.kafka")
                             .linkCount(1),
                           TraceExpectation.leaf(
                             SpanExpectation
                               .consumer(s"process $topic")
                               .scopeName("fs2.kafka")
                               .linkCount(1)
                           )
                         )
                       )
                     )
                   }
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should emit a send span for transactional traced produce against Kafka") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 1).get

        KafkaTracerTestkit
          .create("fs2.kafka.otel4s.integration")
          .use { testkit =>
            import testkit.{appTracer, tracerProvider}

            for {
              kafkaTracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
              _           <- appTracer
                     .rootSpan("producer-root")
                     .surround {
                       KafkaProducer
                         .transactional[IO, String, String](
                           producerSettingsTransactional[IO].withTransactionalId(s"id-$topic")
                         )
                         .use { producer =>
                           kafkaTracer
                             .producer(producer)
                             .produceTransactionally(
                               ProducerRecords.one(
                                 ProducerRecord(topic, "key", "value")
                               )
                             )
                             .void
                         }
                     }
              consumed <- IO(consumeFirstKeyedMessageFrom[String, String](topic))
              spans    <- testkit.finishedSpans
              _        <- IO {
                     assert(consumed == ("key" -> "value"))
                     assertExpected(
                       spans,
                       TraceForestExpectation.unordered(
                         root(
                           SpanExpectation
                             .internal("producer-root")
                             .scopeName("fs2.kafka.otel4s.integration"),
                           TraceExpectation.leaf(
                             SpanExpectation.producer(s"send $topic").scopeName("fs2.kafka")
                           )
                         )
                       )
                     )
                   }
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should let recordsWithProcess emit separate receive and process spans") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 1).get

        KafkaTracerTestkit
          .create("fs2.kafka.otel4s.integration")
          .use { testkit =>
            import testkit.tracerProvider

            for {
              kafkaTracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
              _           <- KafkaProducer
                     .resource(producerSettings[IO])
                     .use { producer =>
                       kafkaTracer
                         .producer(producer)
                         .produceOneAwaited(ProducerRecord(topic, "key", "value"))
                         .void
                     }
              consumedRecord <-
                KafkaConsumer
                  .stream(consumerSettings[IO].withGroupId(s"group-records-with-process-$topic"))
                  .subscribeTo(topic)
                  .flatMap { consumer =>
                    kafkaTracer
                      .consumer(consumer)
                      .recordsWithProcess(record => IO.pure(record.record))
                  }
                  .take(1)
                  .interruptAfter(20.seconds)
                  .compile
                  .lastOrError
              spans <- testkit.finishedSpans
              _     <- IO {
                     assert(consumedRecord.key == "key")
                     assertExpected(
                       spans,
                       TraceForestExpectation.unordered(
                         root(
                           SpanExpectation.producer(s"send $topic").scopeName("fs2.kafka")
                         ),
                         root(
                           SpanExpectation
                             .client(s"poll $topic")
                             .scopeName("fs2.kafka")
                             .linkCount(1)
                         ),
                         root(
                           SpanExpectation
                             .consumer(s"process $topic")
                             .scopeName("fs2.kafka")
                             .linkCount(1)
                         )
                       )
                     )
                   }
            } yield ()
          }
          .unsafeRunSync()
      }
    }

    it("should let recordsWithProcessOnly omit receive spans") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 1).get

        KafkaTracerTestkit
          .create("fs2.kafka.otel4s.integration")
          .use { testkit =>
            import testkit.tracerProvider

            for {
              kafkaTracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
              _           <- KafkaProducer
                     .resource(producerSettings[IO])
                     .use { producer =>
                       kafkaTracer
                         .producer(producer)
                         .produceOneAwaited(ProducerRecord(topic, "key", "value"))
                         .void
                     }
              consumedRecord <-
                KafkaConsumer
                  .stream(
                    consumerSettings[IO].withGroupId(s"group-records-with-process-only-$topic")
                  )
                  .subscribeTo(topic)
                  .flatMap { consumer =>
                    kafkaTracer
                      .consumer(consumer)
                      .recordsWithProcessOnly(record => IO.pure(record.record))
                  }
                  .take(1)
                  .interruptAfter(20.seconds)
                  .compile
                  .lastOrError
              spans <- testkit.finishedSpans
              _     <- IO {
                     assert(consumedRecord.key == "key")
                     assertExpected(
                       spans,
                       TraceForestExpectation.unordered(
                         root(
                           SpanExpectation.producer(s"send $topic").scopeName("fs2.kafka")
                         ),
                         root(
                           SpanExpectation
                             .consumer(s"process $topic")
                             .scopeName("fs2.kafka")
                             .linkCount(1)
                         )
                       )
                     )
                   }
            } yield ()
          }
          .unsafeRunSync()
      }
    }
  }

  private def root(span: SpanExpectation, children: TraceExpectation*): TraceExpectation =
    TraceExpectation.ordered(
      span.noParentSpanContext,
      children*
    )

  private def assertExpected(spans: List[SpanData], expected: TraceForestExpectation): Unit =
    TraceExpectations.check(spans, expected) match {
      case Right(_) =>
        ()
      case Left(mismatches) =>
        fail(TraceExpectations.format(mismatches))
    }

}
