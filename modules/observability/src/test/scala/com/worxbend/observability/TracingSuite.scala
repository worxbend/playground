/*
 * Copyright (c) 2020 Worxbend
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.worxbend.observability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.slf4j.MDC
import scala.jdk.CollectionConverters.*

/** Span recording and cross-process context propagation.
  *
  * Uses `InMemorySpanExporter` with a `SimpleSpanProcessor` rather than the autoconfigured SDK: the assertions are
  * about span *shape*, and a batching exporter aimed at a collector would turn every one of them into a timing
  * question. `Tracing.autoConfigured` is exercised where it belongs — against a real collector in the integration tier
  * — not here.
  */
final class TracingSuite extends munit.FunSuite:

  private val TraceParent = "traceparent"

  /** A trace context that did not originate in this process, used to stand in for a remote parent. */
  private val IncomingTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
  private val IncomingTraceId = "0af7651916cd43dd8448eb211c80319c"
  private val IncomingSpanId = "b7ad6b7169203331"

  private def withTracing[A](body: (Tracing, InMemorySpanExporter) => A): A =
    val exporter = InMemorySpanExporter.create()
    val provider = SdkTracerProvider
      .builder()
      .addSpanProcessor(SimpleSpanProcessor.create(exporter))
      .build()
    val sdk = OpenTelemetrySdk
      .builder()
      .setTracerProvider(provider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build()
    val tracing = Tracing(sdk, Tracing.ScopeName, () => provider.close())
    try body(tracing, exporter)
    finally tracing.close()

  // --- span recording ---------------------------------------------------------------------------------------------

  test("the span helper records one span and returns the body's value"):
    withTracing { (tracing, exporter) =>
      val result = tracing.span("decode-envelope")(_ => 42)
      assertEquals(result, 42)
      val spans = exporter.getFinishedSpanItems.asScala.toList
      assertEquals(spans.map(_.getName), List("decode-envelope"))
      assertEquals(spans.head.getInstrumentationScopeInfo.getName, Tracing.ScopeName)
    }

  test("kind and attributes reach the recorded span"):
    withTracing { (tracing, exporter) =>
      val topic = AttributeKey.stringKey(Meters.TagKeys.Topic)
      tracing.span("publish", SpanKind.PRODUCER, Attributes.of(topic, "events.cloudevents.v1"))(_ => ())
      val span = exporter.getFinishedSpanItems.asScala.head
      assertEquals(span.getKind, SpanKind.PRODUCER)
      assertEquals(span.getAttributes.get(topic), "events.cloudevents.v1")
    }

  test("the span is current inside the body, so nested work becomes a child"):
    withTracing { (tracing, exporter) =>
      tracing.span("outer") { outer =>
        assertEquals(Span.current().getSpanContext.getSpanId, outer.getSpanContext.getSpanId)
        tracing.span("inner")(_ => ())
      }
      val spans = exporter.getFinishedSpanItems.asScala.toList
      val inner = spans.find(_.getName == "inner").get
      val outer = spans.find(_.getName == "outer").get
      assertEquals(inner.getTraceId, outer.getTraceId)
      assertEquals(inner.getParentSpanId, outer.getSpanId)
    }

  test("a failing body ends the span with ERROR, records the exception, and rethrows"):
    withTracing { (tracing, exporter) =>
      val boom = new IllegalStateException("no broker")
      val thrown = intercept[IllegalStateException](tracing.span("publish")(_ => throw boom))
      assertEquals(thrown, boom)
      val span = exporter.getFinishedSpanItems.asScala.head
      assertEquals(span.getStatus.getStatusCode, StatusCode.ERROR)
      assertEquals(span.getStatus.getDescription, "no broker")
      assertEquals(span.getEvents.asScala.map(_.getName).toList, List("exception"))
    }

  test("the span is ended even when the body throws, so no trace is left open"):
    withTracing { (tracing, exporter) =>
      val _ = intercept[RuntimeException](tracing.span("work")(_ => throw new RuntimeException("x")))
      assertEquals(exporter.getFinishedSpanItems.size, 1)
      assertEquals(Span.current().getSpanContext.isValid, false, "the scope leaked out of the failing span")
    }

  test("the span's ids are in the MDC inside the body and gone after it"):
    withTracing { (tracing, _) =>
      tracing.span("work") { span =>
        assertEquals(MDC.get(LogContext.TraceIdKey), span.getSpanContext.getTraceId)
        assertEquals(MDC.get(LogContext.SpanIdKey), span.getSpanContext.getSpanId)
      }
      assert(MDC.get(LogContext.TraceIdKey) == null, "trace_id survived the span")
      assert(MDC.get(LogContext.SpanIdKey) == null, "span_id survived the span")
    }

  // --- carrier propagation ----------------------------------------------------------------------------------------

  test("inject then extract round-trips the trace context through a Map carrier"):
    withTracing { (tracing, _) =>
      tracing.span("produce") { span =>
        val carrier = tracing.inject(Map.empty[String, String])
        assert(carrier.contains(TraceParent), s"no traceparent in $carrier")

        val extracted = Span.fromContext(tracing.extract(carrier)).getSpanContext
        assertEquals(extracted.getTraceId, span.getSpanContext.getTraceId)
        assertEquals(extracted.getSpanId, span.getSpanContext.getSpanId)
        assert(extracted.isRemote, "a context read back off the wire must be marked remote")
      }
    }

  test("inject writes into a mutable carrier in place, as a Kafka Headers binding will"):
    withTracing { (tracing, _) =>
      // Stands in for org.apache.kafka.common.header.Headers, which modules/eventing binds to this same trait —
      // this module must never grow a Kafka dependency to prove that shape works (ADR §2).
      val headers = MutableHeaders()
      tracing.span("produce") { span =>
        val returned = tracing.inject(headers)
        assert(returned eq headers, "a mutable carrier must be updated in place, not copied")
        val extracted = Span.fromContext(tracing.extract(headers)).getSpanContext
        assertEquals(extracted.getTraceId, span.getSpanContext.getTraceId)
      }
    }

  test("a duplicated header resolves to the last value, matching Kafka's lastHeader semantics"):
    withTracing { (tracing, _) =>
      val headers = MutableHeaders()
      headers.add(TraceParent, "00-00000000000000000000000000000001-0000000000000001-01")
      headers.add(TraceParent, IncomingTraceParent)
      assertEquals(Span.fromContext(tracing.extract(headers)).getSpanContext.getTraceId, IncomingTraceId)
    }

  test("extracting from a carrier with no trace context yields a root, not a failure"):
    withTracing { (tracing, _) =>
      // An event published by a producer that is not yet instrumented must start a fresh trace, not break consumption.
      val context = tracing.extract(Map("content-type" -> "application/cloudevents+json"))
      assertEquals(Span.fromContext(context).getSpanContext.isValid, false)
    }

  test("extracting from a malformed traceparent yields a root, not a failure"):
    withTracing { (tracing, _) =>
      val context = tracing.extract(Map(TraceParent -> "not-a-traceparent"))
      assertEquals(Span.fromContext(context).getSpanContext.isValid, false)
    }

  test("spanFrom continues the remote trace instead of starting a new one"):
    withTracing { (tracing, exporter) =>
      val carrier = Map(TraceParent -> IncomingTraceParent)
      tracing.spanFrom(carrier, "consume", SpanKind.CONSUMER)(_ => ())
      val span = exporter.getFinishedSpanItems.asScala.head
      assertEquals(span.getTraceId, IncomingTraceId)
      assertEquals(span.getParentSpanId, IncomingSpanId)
      assertEquals(span.getKind, SpanKind.CONSUMER)
    }

  test("spanFrom ignores the ambient context, so a poll loop does not chain a batch into one trace"):
    withTracing { (tracing, exporter) =>
      tracing.span("poll-loop") { _ =>
        tracing.spanFrom(Map(TraceParent -> IncomingTraceParent), "record-1")(_ => ())
      }
      val consumed = exporter.getFinishedSpanItems.asScala.find(_.getName == "record-1").get
      assertEquals(consumed.getTraceId, IncomingTraceId, "the poll loop's trace was inherited")
    }

  test("spanFrom with an uncorrelated carrier starts a root span"):
    withTracing { (tracing, exporter) =>
      tracing.spanFrom(Map.empty[String, String], "consume")(_ => ())
      val span = exporter.getFinishedSpanItems.asScala.head
      assertEquals(span.getParentSpanId, "0000000000000000")
    }

  // --- the disabled configuration ---------------------------------------------------------------------------------

  test("a no-op tracer records nothing but still forwards context it received"):
    val tracing = Tracing.noop
    val context = tracing.extract(Map(TraceParent -> IncomingTraceParent))
    val scope = context.makeCurrent()
    try
      // Being un-instrumented must not mean being a black hole: stripping traceparent here would break tracing for
      // every downstream service, not just this one.
      assertEquals(tracing.inject(Map.empty[String, String]).get(TraceParent), Some(IncomingTraceParent))
    finally scope.close()

  test("a no-op tracer's spans do not throw and still run the body"):
    assertEquals(Tracing.noop.span("work")(_ => "done"), "done")

/** A minimal mutable, multi-valued header set — the shape `org.apache.kafka.common.header.Headers` has. */
final class MutableHeaders:
  private val entries = scala.collection.mutable.ListBuffer.empty[(String, String)]

  def add(key: String, value: String): Unit =
    val _ = entries += ((key, value))

  def keys: Iterable[String] = entries.map(_._1).distinct.toList
  def lastValue(key: String): Option[String] = entries.reverseIterator.find(_._1 == key).map(_._2)

object MutableHeaders:
  given TextCarrier[MutableHeaders] with
    def keys(carrier: MutableHeaders): Iterable[String] = carrier.keys
    def get(carrier: MutableHeaders, key: String): Option[String] = carrier.lastValue(key)
    def put(carrier: MutableHeaders, key: String, value: String): MutableHeaders =
      carrier.add(key, value)
      carrier
