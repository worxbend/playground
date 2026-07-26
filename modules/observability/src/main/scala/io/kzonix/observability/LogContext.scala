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

import scala.jdk.CollectionConverters.*

import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.argument.StructuredArguments
import org.slf4j.MDC

/** Correlation between the log stream and the trace stream (ADR §7.3).
  *
  * Logs and traces are exported by different pipelines to different backends. The only thing that joins them is the
  * trace id printed on the log line, so putting it in the MDC is not decoration — without it, "find the logs for this
  * slow request" is unanswerable and the trace tells you *where* time went but never *why*.
  *
  * The MDC is a `ThreadLocal`. That is the whole hazard: Play actions, Pekko stream stages and Vert.x event-loop hops
  * all move work to another thread, and the MDC does not follow. Every method here is therefore scoped — it restores
  * what it found — and the scope must be re-established inside the stage that does the work, never assumed to have
  * survived a `Future` boundary. A stale MDC is worse than an empty one: it attributes a log line to the wrong request.
  */
object LogContext:

  /** MDC key for the trace id. Snake case because it lands in JSON as a field name and the log backends this feeds
    * (and the OTel log data model) spell it this way.
    */
  val TraceIdKey: String = "trace_id"

  /** MDC key for the span id. */
  val SpanIdKey: String = "span_id"

  /** Both keys, for the logback encoder's include list. */
  val TraceKeys: Set[String] = Set(TraceIdKey, SpanIdKey)

  /** Runs `body` with the current span's ids in the MDC, restoring the previous values afterwards.
    *
    * A no-op when there is no recording span, and deliberately so: writing the all-zero invalid trace id onto log lines
    * produces a field that looks populated and matches nothing.
    */
  def withCurrentSpan[A](body: => A): A = withSpan(Span.current())(body)

  /** Runs `body` with `span`'s ids in the MDC.
    *
    * Takes an explicit `Span` because that is the form that survives an async boundary — the caller captures the span
    * on the thread that created it and re-enters here on the thread that continues the work.
    */
  def withSpan[A](span: Span)(body: => A): A = withSpanContext(span.getSpanContext)(body)

  /** Runs `body` with `spanContext`'s ids in the MDC. */
  def withSpanContext[A](spanContext: SpanContext)(body: => A): A =
    if !spanContext.isValid then body
    else withFields(TraceIdKey -> spanContext.getTraceId, SpanIdKey -> spanContext.getSpanId)(body)

  /** Runs `body` with `fields` added to the MDC, restoring prior values — including absence — on the way out.
    *
    * Restoring absence matters: `MDC.remove` on a key that was already set by an outer scope would silently strip
    * correlation from every subsequent line on this thread.
    */
  def withFields[A](fields: (String, String)*)(body: => A): A =
    val previous = fields.map((key, _) => key -> Option(MDC.get(key)))
    fields.foreach((key, value) => MDC.put(key, value))
    try body
    finally
      previous.foreach:
        case (key, Some(value)) => MDC.put(key, value)
        case (key, None)        => MDC.remove(key)

  /** The current trace id, if a valid one is in scope. Useful for echoing a correlation id back to a caller in an
    * error response so a user-reported failure can be found in the trace store.
    */
  def currentTraceId: Option[String] =
    Some(Span.current().getSpanContext).filter(_.isValid).map(_.getTraceId)

  /** A structured log argument: `logger.info("persisted {}", LogContext.kv("count", n))`.
    *
    * Structured rather than interpolated because an interpolated value is only greppable, while a key/value pair is
    * queryable — and because interpolation forces the string to be built even when the level is disabled. The message
    * text stays a constant, which is what makes "how often does *this* log line fire" answerable at all.
    */
  def kv(key: String, value: Any): StructuredArgument = StructuredArguments.kv(key, value)

  /** Several key/value pairs as one argument, rendered as a nested JSON object under `key`. */
  def entries(pairs: (String, Any)*): StructuredArgument =
    StructuredArguments.entries(pairs.toMap.asJava)
