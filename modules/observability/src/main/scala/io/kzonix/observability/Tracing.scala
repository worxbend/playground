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

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.context.propagation.TextMapSetter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk

/** Distributed tracing: a traces-only OpenTelemetry SDK plus the two operations the services actually perform.
  *
  * **Traces only.** Metrics are Micrometer's, exclusively (ADR §7). If the OTel metrics SDK were also active, every
  * counter would exist twice under two naming conventions in two backends, and the two would disagree the first time
  * one pipeline dropped a batch — a discrepancy nobody can adjudicate. [[autoConfigured]] therefore *forces* the
  * metrics and logs exporters off rather than merely defaulting them off; see the note there.
  *
  * **`-Werror` trap (ADR §7.4).** Every OTel builder method returns `this`, so `builder.setAttribute(k, v)` written as
  * a statement is a compile error under `-Wnonunit-statement`. Chain into one expression, or bind with `val _ =`. The
  * same applies to `Span#setStatus` and `Span#recordException`.
  *
  * **Context is a `ThreadLocal`.** `Context.current()` returns root — silently, never an error — on any thread the
  * context was not entered on. Across a `Future`, a Pekko stream stage or a Vert.x event-loop hop, capture the
  * `Context` explicitly and pass it to [[span]] as `parent`; do not rely on ambient state. An orphaned span is not a
  * failure anything reports, it is simply a trace that is missing its middle.
  */
final class Tracing private (
    val openTelemetry: OpenTelemetry,
    val tracer: Tracer,
    private val onClose: () => Unit
) extends AutoCloseable:

  /** The configured propagator. W3C `traceparent`/`tracestate` in every deployment — see [[Tracing.autoConfigured]]. */
  def propagator: TextMapPropagator = openTelemetry.getPropagators.getTextMapPropagator

  /** Runs `body` inside a new span, ending it exactly once and on every path.
    *
    * The span is made current for the duration and its ids are put into the MDC, so any log line `body` writes on this
    * thread correlates with the trace without the caller doing anything (ADR §7.3).
    *
    * A thrown exception is recorded on the span and the status set to `ERROR` before it is rethrown: the span must
    * describe what happened, but swallowing the exception would make tracing change program behaviour, which is the
    * one thing instrumentation may never do. `NonFatal` only — an `OutOfMemoryError` is not a span attribute.
    *
    * @param parent
    *   defaults to the ambient context, which is correct on a synchronous call chain and wrong across every async
    *   boundary. Pass it explicitly whenever a thread hop is involved.
    */
  def span[A](
      name: String,
      kind: SpanKind = SpanKind.INTERNAL,
      attributes: Attributes = Attributes.empty(),
      parent: Context = Context.current()
  )(body: Span => A): A =
    val span = tracer
      .spanBuilder(name)
      .setSpanKind(kind)
      .setParent(parent)
      .setAllAttributes(attributes)
      .startSpan()
    val scope = span.makeCurrent()
    try
      LogContext.withSpanContext(span.getSpanContext):
        try body(span)
        catch
          case NonFatal(error) =>
            val _ = span.setStatus(StatusCode.ERROR, Option(error.getMessage).getOrElse(error.getClass.getName))
            val _ = span.recordException(error)
            throw error
    finally
      scope.close()
      span.end()

  /** Continues a trace that started in another process: extracts the context from `carrier`, then runs `body` in a
    * child span of it.
    *
    * This is the consume side of the cross-service story. Extraction starts from `Context.root()` and not from the
    * ambient context on purpose — the calling thread is a poll loop that may still be carrying the previous record's
    * context, and inheriting it would chain every record in a batch into one ever-growing trace.
    */
  def spanFrom[A, C](
      carrier: C,
      name: String,
      kind: SpanKind = SpanKind.CONSUMER,
      attributes: Attributes = Attributes.empty()
  )(body: Span => A)(using TextCarrier[C]): A =
    span(name, kind, attributes, extract(carrier))(body)

  /** Writes the ambient trace context into `carrier`, returning the result (which may be `carrier` itself for a
    * mutable transport).
    */
  def inject[C](carrier: C)(using TextCarrier[C]): C = inject(Context.current(), carrier)

  /** Writes `context` into `carrier`. */
  def inject[C](context: Context, carrier: C)(using textCarrier: TextCarrier[C]): C =
    // The propagator's setter API is `Unit`-returning, which a persistent carrier cannot satisfy directly; a single
    // mutable cell holds the accumulating value for the duration of the (synchronous) inject call and is then dropped.
    val cell = Tracing.Cell(carrier)
    val setter: TextMapSetter[Tracing.Cell[C]] =
      (target, key, value) =>
        if target != null && key != null && value != null then target.value = textCarrier.put(target.value, key, value)
    propagator.inject(context, cell, setter)
    cell.value

  /** Reads trace context out of `carrier`, starting from `Context.root()`.
    *
    * Returns root when the carrier holds no valid `traceparent`, which yields a new root trace rather than an error —
    * the correct behaviour for an event published by a producer that is not yet instrumented.
    */
  def extract[C](carrier: C)(using TextCarrier[C]): Context = extract(Context.root(), carrier)

  /** Reads trace context out of `carrier`, using `parent` as the base context. */
  def extract[C](parent: Context, carrier: C)(using textCarrier: TextCarrier[C]): Context =
    val getter: TextMapGetter[C] =
      new TextMapGetter[C]:
        def keys(target: C): java.lang.Iterable[String] =
          if target == null then List.empty[String].asJava else textCarrier.keys(target).toList.asJava
        def get(target: C, key: String): String =
          if target == null then null else textCarrier.get(target, key).orNull
    propagator.extract(parent, carrier, getter)

  /** Flushes and shuts the SDK down. Idempotent from the caller's perspective; normally reached through
    * [[Telemetry.close]] rather than directly.
    */
  def close(): Unit = onClose()

object Tracing:

  /** Instrumentation scope reported on every span this module creates.
    *
    * One name for all three services: the scope identifies the *instrumentation*, not the service — the service is
    * already a resource attribute, and splitting the scope per service would fragment trace-store queries for no gain.
    */
  val ScopeName: String = "io.kzonix.observability"

  /** How long [[close]] waits for the exporter to drain before giving up. Bounded because a shutdown that blocks on an
    * unreachable collector turns a rolling deploy into an outage.
    */
  val ShutdownTimeoutSeconds: Long = 10L

  /** Mutable accumulator for [[Tracing.inject]]. Package-private and never escapes the call that creates it. */
  private final class Cell[C](var value: C)

  /** Wraps an existing `OpenTelemetry` instance.
    *
    * The seam that keeps this module testable: a test supplies an SDK wired to an `InMemorySpanExporter`, and a service
    * that has tracing switched off supplies [[noop]] — neither needs a collector.
    */
  def apply(openTelemetry: OpenTelemetry, scopeName: String = ScopeName, onClose: () => Unit = () => ()): Tracing =
    new Tracing(openTelemetry, openTelemetry.getTracer(scopeName), onClose)

  /** A tracer that records nothing but propagates correctly.
    *
    * Not `OpenTelemetry.noop()`: that also installs a no-op propagator, so a service with tracing disabled would strip
    * `traceparent` from every message it forwarded and break tracing for everyone downstream. Being un-instrumented
    * must not mean being a black hole.
    */
  def noop: Tracing =
    apply(OpenTelemetry.propagating(ContextPropagators.create(W3CTraceContextPropagator.getInstance())))

  /** Properties that callers and the environment may override. Ordinary defaults.
    *
    * `otel.exporter.otlp.protocol` is pinned to gRPC per ADR §7; the SDK's own default is `http/protobuf`, so leaving
    * it unset would point the three services at a port the collector is not listening on.
    */
  private def defaultProperties(config: TelemetryConfig): Map[String, String] =
    Map(
      "otel.service.name" -> config.serviceName,
      "otel.resource.attributes" -> config.resourceAttributes,
      "otel.traces.exporter" -> "otlp",
      "otel.exporter.otlp.protocol" -> "grpc"
    )

  /** Properties the environment may **not** override.
    *
    * Applied through `addPropertiesCustomizer`, which the autoconfigure extension evaluates *after* system properties
    * and `OTEL_*` env vars, so these win. `addPropertiesSupplier` would not: its values are defaults, and a stray
    * `OTEL_METRICS_EXPORTER=otlp` in a deployment manifest would quietly re-enable the second metrics pipeline that
    * ADR §7 exists to prevent. Propagators are forced for the same reason — a service that negotiated B3 instead of
    * W3C would produce traces that break in half at its boundary.
    */
  private val forcedProperties: Map[String, String] =
    Map(
      "otel.metrics.exporter" -> "none",
      "otel.logs.exporter" -> "none",
      "otel.propagators" -> "tracecontext"
    )

  /** Builds the traces-only SDK from the `OTEL_*` environment (ADR §3.5, §7).
    *
    * Environment-driven rather than config-file-driven because the collector endpoint, sampling rate and headers are
    * properties of the *deployment*, not the build, and the autoconfigure extension is the one place where the OTel
    * spelling of those knobs is already implemented and documented.
    *
    * The SDK is **not** registered as the global instance and the JVM shutdown hook is disabled: this is a composition
    * root, and ownership of shutdown belongs to whoever owns [[Telemetry]]. A global would also make the object
    * un-replaceable in tests, which is exactly the property that makes global telemetry state painful.
    */
  def autoConfigured(config: TelemetryConfig): Tracing =
    val sdk = AutoConfiguredOpenTelemetrySdk
      .builder()
      .addPropertiesSupplier(() => defaultProperties(config).asJava)
      .addPropertiesCustomizer(_ => forcedProperties.asJava)
      .disableShutdownHook()
      .build()
      .getOpenTelemetrySdk
    apply(sdk, ScopeName, () => drain(sdk))

  /** Bounded shutdown: `join` returns whether the flush completed, and the result is deliberately discarded — there is
    * no useful recovery from "the collector did not acknowledge our last batch" during process exit.
    */
  private def drain(sdk: OpenTelemetrySdk): Unit =
    val _ = sdk.shutdown().join(ShutdownTimeoutSeconds, TimeUnit.SECONDS)
