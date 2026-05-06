/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import fs2.kafka.{Header, Headers}

import org.typelevel.otel4s.context.propagation.{TextMapGetter, TextMapUpdater}

/**
  * Typeclass instances for using [[fs2.kafka.Headers]] as an otel4s propagation carrier.
  *
  * Trace context is propagated via Kafka headers, following the OpenTelemetry messaging
  * specification.
  *
  * @see
  *   [[https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/]]
  */
trait Otel4sInstances {

  implicit val headersTextMapGetter: TextMapGetter[Headers] =
    new TextMapGetter[Headers] {
      override def get(carrier: Headers, key: String): Option[String] =
        carrier(key).map(_.as[String])

      override def keys(carrier: Headers): Iterable[String] =
        carrier.toChain.map(_.key).toVector
    }

  implicit val headersTextMapUpdater: TextMapUpdater[Headers] =
    new TextMapUpdater[Headers] {

      override def updated(carrier: Headers, key: String, value: String): Headers =
        Headers.fromSeq(
          carrier.toChain.iterator.filterNot(_.key == key).toVector :+ Header(key, value)
        )

    }

}

/**
  * Default otel4s propagation instances for [[fs2.kafka.Headers]].
  */
object instances extends Otel4sInstances
