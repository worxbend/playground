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

package io.kzonix.eventing

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.context.propagation.TextMapSetter
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers

/** W3C trace-context propagation across Kafka: inject on produce, extract on consume.
  *
  * This is the whole cross-service tracing story (ADR §7.2). Without it a trace stops at wolfram's HTTP handler and
  * restarts, unrelated, in cobalt — two disconnected traces where the interesting question ("why did this reading take
  * 40 seconds to appear?") lives precisely in the gap between them. With it, one trace runs from HTTP ingestion,
  * through the broker, into the consumer and its database write.
  *
  * **Why the propagator is a parameter with a W3C default rather than a hard-wired global.** `GlobalOpenTelemetry` is
  * exactly the sort of hidden singleton that makes a test's outcome depend on suite ordering. Services pass
  * `Telemetry.tracing.propagator`; a test passes its own; the default keeps a service that has not wired telemetry yet
  * from silently *stripping* `traceparent` and breaking tracing for everyone downstream, which is what a no-op
  * propagator would do.
  *
  * **Extraction always starts from `Context.root()`.** The consumer's poll loop may still be carrying the previous
  * record's context on that thread; inheriting it would chain every record in a batch into one ever-growing trace that
  * says nothing about any single record.
  *
  * **`-Werror` trap (ADR §7.4).** Kafka's `Headers.add`/`remove` and every OTel builder method return `this`, so
  * writing them as statements fails under `-Wnonunit-statement`. They are chained or bound with `val _ =` below.
  *
  * This module deliberately does not depend on `io.kzonix.observability` (ADR §3.3 lists eventing's dependencies as
  * kernel, cloudevents, kafka-clients and opentelemetry-api). [[KafkaHeaderCarrier]] therefore exposes the three
  * operations of that module's `TextCarrier` without naming it, so a module that sees both can bind
  * `given TextCarrier[Headers]` in one delegating line and get `Tracing.spanFrom` over Kafka headers for free.
  */
object KafkaTrace:

  /** The W3C header names, exposed because tests and tooling assert on them by name. */
  val TraceParent: String = "traceparent"

  val TraceState: String = "tracestate"

  /** The propagator used when a caller does not supply one. See the object Scaladoc for why this is not a no-op. */
  val defaultPropagator: TextMapPropagator = W3CTraceContextPropagator.getInstance()

  /** Adapts Kafka's mutable `Headers` to OTel's write side. */
  val setter: TextMapSetter[Headers] =
    (carrier, key, value) =>
      if carrier != null && key != null && value != null then
        val _ = KafkaHeaderCarrier.put(carrier, key, value)

  /** Adapts Kafka's `Headers` to OTel's read side. `null` is the API's spelling of "absent", not an oversight. */
  val getter: TextMapGetter[Headers] =
    new TextMapGetter[Headers]:
      def keys(carrier: Headers): java.lang.Iterable[String] =
        if carrier == null then List.empty[String].asJava else KafkaHeaderCarrier.keys(carrier).toList.asJava

      def get(carrier: Headers, key: String): String =
        if carrier == null then null else KafkaHeaderCarrier.get(carrier, key).orNull

  /** Writes the ambient trace context into `headers`, returning the same (mutable) headers.
    *
    * Ambient context is correct on wolfram's synchronous request path and wrong across every async boundary — a
    * `Future`, a stream stage, a Vert.x event-loop hop. OTel's `Context` is `ThreadLocal`-backed and returns *root*
    * rather than failing when it has been lost, so an orphaned span is silent. Use the explicit-context overload
    * whenever a thread hop is involved.
    */
  def inject(headers: Headers): Headers = inject(Context.current(), headers, defaultPropagator)

  /** Writes `context` into `headers`. */
  def inject(context: Context, headers: Headers, propagator: TextMapPropagator = defaultPropagator): Headers =
    propagator.inject(context, headers, setter)
    headers

  /** Injects into a record about to be sent. Convenience for the produce path, where the headers are always mutable. */
  def inject[K, V](record: ProducerRecord[K, V], context: Context): ProducerRecord[K, V] =
    val _ = inject(context, record.headers, defaultPropagator)
    record

  /** Reads trace context out of `headers`, starting from `Context.root()`.
    *
    * Returns root when there is no valid `traceparent`, which yields a fresh root trace rather than an error — the
    * right behaviour for an event published by a producer that is not instrumented yet.
    */
  def extract(headers: Headers, propagator: TextMapPropagator = defaultPropagator): Context =
    propagator.extract(Context.root(), headers, getter)

  /** Runs `body` inside a CONSUMER span that continues the trace the record carries.
    *
    * The span is made current for the duration, so `Span.current()` — and therefore
    * `io.kzonix.observability.LogContext.withCurrentSpan` in the consumer — sees it without the caller threading
    * anything through. The MDC is deliberately *not* written here: the keys belong to `modules/observability`, and
    * duplicating them in a module that cannot see them is how two spellings of `trace_id` end up in one log stream.
    *
    * A thrown exception is recorded and the status set to `ERROR` before it is rethrown. Swallowing it would make
    * instrumentation change program behaviour, which is the one thing instrumentation may never do; `NonFatal` only,
    * because an `OutOfMemoryError` is not a span attribute.
    */
  def withConsumerSpan[A, K, V](
      record: ConsumerRecord[K, V],
      tracer: Tracer,
      propagator: TextMapPropagator = defaultPropagator
  )(body: Span => A): A =
    val span = tracer
      .spanBuilder(s"${record.topic} process")
      .setSpanKind(SpanKind.CONSUMER)
      .setParent(extract(record.headers, propagator))
      .setAllAttributes(attributesOf(record))
      .startSpan()
    val scope = span.makeCurrent()
    try
      try body(span)
      catch
        case NonFatal(error) =>
          val _ = span.setStatus(StatusCode.ERROR, Option(error.getMessage).getOrElse(error.getClass.getName))
          val _ = span.recordException(error)
          throw error
    finally
      scope.close()
      span.end()

  /** The messaging attributes OTel's semantic conventions name for a Kafka consumer.
    *
    * Spelled as literal `AttributeKey`s rather than taken from `opentelemetry-semconv`, which is not on this module's
    * classpath (ADR §3.3) and whose constants have been renamed across releases often enough that pinning the strings
    * here is the more stable choice. The message *key* is included because it is this system's device identity and is
    * what makes a trace searchable by device; it is never high-cardinality per span, only per trace.
    */
  private def attributesOf[K, V](record: ConsumerRecord[K, V]): Attributes =
    val base = Attributes
      .builder()
      .put(AttributeKey.stringKey("messaging.system"), "kafka")
      .put(AttributeKey.stringKey("messaging.operation"), "process")
      .put(AttributeKey.stringKey("messaging.destination.name"), record.topic)
      .put(AttributeKey.longKey("messaging.kafka.destination.partition"), record.partition.toLong)
      .put(AttributeKey.longKey("messaging.kafka.message.offset"), record.offset)
    Option(record.key)
      .fold(base)(key => base.put(AttributeKey.stringKey("messaging.kafka.message.key"), key.toString))
      .build()

/** Kafka `Headers` seen as a string-keyed carrier: `keys`, `get`, `put`.
  *
  * Structurally identical to `io.kzonix.observability.TextCarrier[Headers]`, and named separately only because
  * `modules/eventing` must not depend on `modules/observability` (ADR §3.3 fixes this module's dependencies at kernel,
  * cloudevents, kafka-clients and opentelemetry-api). A module that sees both — wolfram, cobalt — binds the two
  * together in one line:
  *
  * {{{
  * given TextCarrier[Headers] with
  *   def keys(c: Headers) = KafkaHeaderCarrier.keys(c)
  *   def get(c: Headers, k: String) = KafkaHeaderCarrier.get(c, k)
  *   def put(c: Headers, k: String, v: String) = KafkaHeaderCarrier.put(c, k, v)
  * }}}
  *
  * and from then on `Tracing.inject` / `Tracing.spanFrom` work over Kafka headers with no further plumbing.
  */
object KafkaHeaderCarrier:

  /** Every distinct key present; duplicates are collapsed, which is correct for single-valued trace-context fields. */
  def keys(carrier: Headers): Iterable[String] = CloudEventHeaders.keys(carrier)

  /** The **last** value for `key`: a proxy or interceptor that appended a fresh `traceparent` meant its value to win. */
  def get(carrier: Headers, key: String): Option[String] = CloudEventHeaders.get(carrier, key)

  /** Sets `key`, replacing any existing occurrence, and returns the same mutable `carrier`. */
  def put(carrier: Headers, key: String, value: String): Headers = CloudEventHeaders.put(carrier, key, value)
