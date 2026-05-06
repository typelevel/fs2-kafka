/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import cats.effect.{IO, Resource}
import fs2.kafka.{KafkaConsumer, KafkaProducer}

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.sdk.trace.data.SpanData
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

class KafkaTracerTestkit(testkit: OtelJavaTestkit[IO])(implicit
  val tracerProvider: TracerProvider[IO],
  val appTracer: Tracer[IO]
) {

  def tracedProducer[K: KafkaMessageKey, V](
    producer: KafkaProducer[IO, K, V],
    config: KafkaTracer.Config = KafkaTracer.Config.default
  ): IO[TracedKafkaProducer[IO, K, V]] =
    KafkaTracer.create[IO](config).map(_.producer(producer))

  def tracedConsumer[K: KafkaMessageKey, V](
    consumer: KafkaConsumer[IO, K, V],
    config: KafkaTracer.Config = KafkaTracer.Config.default
  ): IO[TracedKafkaConsumer[IO, K, V]] =
    KafkaTracer.create[IO](config).map(_.consumer(consumer))

  def finishedSpans: IO[List[SpanData]] =
    testkit.finishedSpans

}

object KafkaTracerTestkit {

  def create(
    appTracerName: String = "fs2.kafka.otel4s.tests",
    propagators: Seq[TextMapPropagator] = Seq(W3CTraceContextPropagator.getInstance())
  ): Resource[IO, KafkaTracerTestkit] = {
    OtelJavaTestkit
      .inMemory[IO](
        _.addTextMapPropagators(propagators*)
      )
      .evalMap { testkit =>
        for {
          tracer <- testkit.tracerProvider.get(appTracerName)
        } yield new KafkaTracerTestkit(testkit)(testkit.tracerProvider, tracer)
      }
  }

}
