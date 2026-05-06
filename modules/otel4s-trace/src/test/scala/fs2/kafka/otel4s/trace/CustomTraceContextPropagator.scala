/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.otel4s.trace

import io.opentelemetry.api.trace.{
  Span => OtelSpan,
  SpanContext => OtelSpanContext,
  TraceFlags => OtelTraceFlags,
  TraceState
}
import io.opentelemetry.context.propagation.{
  TextMapGetter => OtelTextMapGetter,
  TextMapPropagator,
  TextMapSetter
}
import io.opentelemetry.context.Context

object CustomTraceContextPropagator extends TextMapPropagator {

  val customPropagationHeader = "x-custom-trace"

  override def fields(): java.util.Collection[String] =
    java.util.Collections.singleton(customPropagationHeader)

  override def inject[C](context: Context, carrier: C, setter: TextMapSetter[C]): Unit = {
    val spanContext = OtelSpan.fromContext(context).getSpanContext

    if (spanContext.isValid)
      setter.set(carrier, customPropagationHeader, encodeCustomSpanContext(spanContext))
  }

  override def extract[C](
    context: Context,
    carrier: C,
    getter: OtelTextMapGetter[C]
  ): Context =
    Option(getter.get(carrier, customPropagationHeader))
      .flatMap(decodeCustomSpanContext)
      .fold(context)(spanContext => context.`with`(OtelSpan.wrap(spanContext)))

  private def encodeCustomSpanContext(spanContext: OtelSpanContext): String =
    s"${spanContext.getTraceId}->${spanContext.getSpanId}"

  private def decodeCustomSpanContext(value: String): Option[OtelSpanContext] =
    value.split("->", -1).toList match {
      case traceId :: spanId :: Nil if isHex(traceId, 32) && isHex(spanId, 16) =>
        Some(
          OtelSpanContext.createFromRemoteParent(
            traceId,
            spanId,
            OtelTraceFlags.getSampled,
            TraceState.getDefault
          )
        )

      case _ =>
        None
    }

  private def isHex(value: String, expectedLength: Int): Boolean =
    value.length == expectedLength && value.forall(ch => Character.digit(ch, 16) >= 0)

}
