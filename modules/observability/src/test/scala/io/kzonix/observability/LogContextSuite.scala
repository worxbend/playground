/*
 * Copyright (c) 2020 Kzonix Projects
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

package io.kzonix.observability

import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import org.slf4j.MDC

/** MDC scoping.
  *
  * The property under test throughout is *restoration*, not insertion. Inserting a trace id is the easy half; the half
  * that goes wrong is a scope that clears a key an outer scope owned, which silently strips correlation from every
  * later line on that thread — a failure that looks like "tracing is flaky" rather than like a bug.
  */
final class LogContextSuite extends munit.FunSuite:

  private val TraceId = "0af7651916cd43dd8448eb211c80319c"
  private val SpanId = "b7ad6b7169203331"

  private val validContext: SpanContext =
    SpanContext.create(TraceId, SpanId, TraceFlags.getSampled, TraceState.getDefault)

  test("withSpanContext puts both ids in scope and removes them on the way out"):
    LogContext.withSpanContext(validContext):
      assertEquals(MDC.get(LogContext.TraceIdKey), TraceId)
      assertEquals(MDC.get(LogContext.SpanIdKey), SpanId)
    assert(MDC.get(LogContext.TraceIdKey) == null)
    assert(MDC.get(LogContext.SpanIdKey) == null)

  test("an invalid span context writes nothing"):
    // The all-zero trace id would render as a populated field that matches no trace — worse than an absent one.
    LogContext.withSpanContext(SpanContext.getInvalid):
      assert(MDC.get(LogContext.TraceIdKey) == null)

  test("withSpanContext restores an outer scope's values rather than clearing them"):
    LogContext.withFields(LogContext.TraceIdKey -> "outer-trace"):
      LogContext.withSpanContext(validContext):
        assertEquals(MDC.get(LogContext.TraceIdKey), TraceId)
      assertEquals(MDC.get(LogContext.TraceIdKey), "outer-trace")

  test("withFields restores absence for keys that were not previously set"):
    LogContext.withFields("tenant" -> "acme"):
      assertEquals(MDC.get("tenant"), "acme")
    assert(MDC.get("tenant") == null)

  test("withFields restores on the exceptional path too"):
    val _ = intercept[RuntimeException]:
      LogContext.withFields("tenant" -> "acme")(throw new RuntimeException("boom"))
    assert(MDC.get("tenant") == null, "a thrown exception left the MDC dirty")

  test("withFields returns the body's value"):
    assertEquals(LogContext.withFields("k" -> "v")(7), 7)

  test("withCurrentSpan picks up the ambient span"):
    val span = Span.wrap(validContext)
    val scope = span.makeCurrent()
    try
      LogContext.withCurrentSpan:
        assertEquals(MDC.get(LogContext.TraceIdKey), TraceId)
    finally scope.close()

  test("currentTraceId is empty when no span is in scope"):
    assertEquals(LogContext.currentTraceId, None)

  test("currentTraceId reports the ambient trace, so a failure response can quote a correlation id"):
    val scope = Span.wrap(validContext).makeCurrent()
    try assertEquals(LogContext.currentTraceId, Some(TraceId))
    finally scope.close()

  test("kv carries the key alongside the value rather than concatenating them"):
    // Structured, so the field stays queryable; the message text stays a constant, which is what makes "how often does
    // this line fire" answerable at all.
    val rendered = LogContext.kv(Meters.TagKeys.Reason, Meters.Reasons.Malformed).toString
    assert(rendered.contains(Meters.TagKeys.Reason), rendered)
    assert(rendered.contains(Meters.Reasons.Malformed), rendered)

  test("entries renders several fields as one argument"):
    val rendered = LogContext.entries("topic" -> "events.cloudevents.v1", "partition" -> 3).toString
    assert(rendered.contains("events.cloudevents.v1"), rendered)
    assert(rendered.contains("partition"), rendered)

  test("the encoder's MDC include list and the keys we write are the same two strings"):
    assertEquals(LogContext.TraceKeys, Set("trace_id", "span_id"))
