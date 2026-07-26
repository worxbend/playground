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

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.java21.instrument.binder.jdk.VirtualThreadMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry

/** The observability composition root: one registry, one tracer, one identity, per process.
  *
  * **Why one registry is the whole point (ADR §7.1).** Frameworks each want to bring their own metrics pipeline. Two
  * pipelines mean two `/metrics` endpoints or two naming conventions, and then one Grafana dashboard stops working
  * across the three services — which is the entire reason `modules/observability` exists. So exactly one
  * `PrometheusRegistry` is constructed here and handed to `PrometheusMeterRegistry`; anything with a native Prometheus
  * collector registers into that same object and appears in the same exposition, with no bridging and no second port.
  *
  * **Why [[scrape]] and not a getter for the registry.** The three services mount metrics on three different HTTP
  * stacks — Play, Vert.x/Tapir, Cask — and none of them should have to know that the exposition comes from Micrometer,
  * let alone which Prometheus client version renders it. A `String` is the entire contract they need. (The registry is
  * still reachable for instrumenting code; it is `scrape` that keeps the *transport* ignorant.)
  *
  * **The version trap this class exists to surface.** `micrometer-registry-prometheus` is compiled against
  * `prometheus-metrics-core` 1.7.0 while the build forces 1.8.0 (ADR §3.5, §3.11). Nothing about that mismatch is
  * visible at compile time: it would appear as a `NoSuchMethodError` on the first scrape, in production, minutes after
  * a green deploy. The unit tests call [[scrape]] for real for exactly this reason — do not replace that with an
  * assertion on the meter list.
  *
  * **`-Werror` trap (ADR §7.4).** `MeterRegistry.Config#commonTags` and `#meterFilter` both return `Config`, so calling
  * them as statements fails under `-Wnonunit-statement`. They are bound with `val _ =` below.
  *
  * Not a singleton and not global: one instance is constructed by each service's `main` and passed down. Micrometer has
  * a static `Metrics.globalRegistry`, and this deliberately does not use it — a global makes the registry impossible to
  * replace per test and turns meter registration order into a hidden coupling between suites.
  */
final class Telemetry private (
  val config: TelemetryConfig,
  val prometheusRegistry: PrometheusRegistry,
  val registry: PrometheusMeterRegistry,
  val tracing: Tracing,
  private val binders: List[AutoCloseable]
) extends AutoCloseable:

  /** The Prometheus text exposition. Serve it verbatim with [[Telemetry.ContentType]].
    *
    * No content negotiation: `micrometer-registry-prometheus` 1.17 renders exactly one format, so accepting an `Accept`
    * header here would offer a choice the registry cannot honour. If OpenMetrics is ever needed it is a change in one
    * place — which is the point of the services not touching the registry.
    */
  def scrape(): String = registry.scrape()

  /** Releases the binders that hold JVM resources, then the registry, then the tracer.
    *
    * In that order: `JvmGcMetrics` and `JvmHeapPressureMetrics` register JMX notification listeners and
    * `VirtualThreadMetrics` opens a JFR recording stream, all of which outlive the registry and leak a thread per
    * instance if a test — or a Play application reload — constructs telemetry more than once. Failures are swallowed
    * per-component so one bad close does not strand the rest.
    */
  def close(): Unit =
    binders.foreach(closeQuietly)
    // MeterRegistry declares `close()` but does not implement AutoCloseable, so it cannot go through closeQuietly.
    closeQuietly(() => registry.close())
    closeQuietly(tracing)

  private def closeQuietly(resource: AutoCloseable): Unit =
    try resource.close()
    catch case _: Exception => ()

object Telemetry:

  /** `Content-Type` for [[Telemetry.scrape]]. Defined here so all three services set the identical header — Prometheus
    * falls back to a lenient parse when it is wrong, which hides the mistake until a scraper that is not Prometheus
    * tries to read the endpoint.
    */
  val ContentType: String = "text/plain; version=0.0.4; charset=utf-8"

  /** Cap on distinct values of the `uri` tag on [[Meters.HttpServerRequests]], after which further values are dropped.
    *
    * ADR §7.1 calls unbounded `uri` cardinality the single most likely way to take down Prometheus in this system, and
    * it is right: ferrite serves server-rendered search over millions of events, so one unfiltered permalink path per
    * query would mint a timeseries per query. Route templates keep the normal case bounded; this filter is the backstop
    * for the day someone tags a raw path by accident. Dropping the overflow is preferable to dropping the meter — the
    * first N routes keep working.
    */
  val MaxUriTagValues: Int = 100

  /** The tags stamped onto every meter.
    *
    * `service` and `version` only, per ADR §7.1. `instance` is deliberately **not** here: Prometheus attaches its own
    * `instance` label from the scrape target, and a same-named tag in the exposition is renamed to `exported_instance`
    * by default, producing two labels that mean the same thing and a dashboard that has to know which. The replica
    * identity is not lost — it is carried where it is authoritative, as the `service.instance.id` resource attribute on
    * traces (see [[TelemetryConfig.resourceAttributes]]) — and [[commonTagsWithInstance]] exists for a deployment that
    * genuinely scrapes through a proxy and has no per-replica target.
    */
  def commonTags(config: TelemetryConfig): Tags =
    Tags.of(
      Tag.of(Meters.TagKeys.Service, config.serviceName),
      Tag.of(Meters.TagKeys.Version, config.serviceVersion)
    )

  /** [[commonTags]] plus `instance`. See there for why this is opt-in. */
  def commonTagsWithInstance(config: TelemetryConfig): Tags =
    commonTags(config).and(Tag.of(Meters.TagKeys.Instance, config.instanceId))

  /** Every binder bound at startup, per ADR §7.1.
    *
    * These are the metrics that answer "is the process itself healthy", and they are bound here rather than by each
    * service so that a JVM-level dashboard panel works for all three. `VirtualThreadMetrics` comes from
    * `micrometer-java21` and is meaningful on JDK 25, where pinning and mount latency are real failure modes for
    * blocking JDBC work.
    */
  private def newBinders(): List[MeterBinder] =
    List(
      JvmMemoryMetrics(),
      JvmGcMetrics(),
      JvmThreadMetrics(),
      JvmHeapPressureMetrics(),
      JvmCompilationMetrics(),
      JvmInfoMetrics(),
      ClassLoaderMetrics(),
      ProcessorMetrics(),
      UptimeMetrics(),
      FileDescriptorMetrics(),
      VirtualThreadMetrics()
    )

  /** Starts telemetry with a traces-only OTel SDK configured from `OTEL_*`. The production entry point. */
  def start(config: TelemetryConfig): Telemetry = start(config, Tracing.autoConfigured(config))

  /** Starts telemetry with a caller-supplied [[Tracing]].
    *
    * Exists so tests can pass an in-memory exporter and so a service can pass [[Tracing.noop]] without this module
    * growing an "enabled" flag whose two branches then drift apart.
    */
  def start(config: TelemetryConfig, tracing: Tracing): Telemetry =
    start(config, tracing, commonTags(config))

  /** Full form, with the common tags chosen explicitly. */
  def start(config: TelemetryConfig, tracing: Tracing, tags: Tags): Telemetry =
    val prometheusRegistry = PrometheusRegistry()
    val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, prometheusRegistry, Clock.SYSTEM)

    // Filters are applied to meters as they are registered, so both of these must be installed before any binder runs;
    // a meter created earlier keeps the tags it was born with and no later filter can retrofit them.
    val _ = registry.config.commonTags(tags)
    val _ = registry.config.meterFilter(
      MeterFilter.maximumAllowableTags(
        Meters.HttpServerRequests,
        Meters.TagKeys.Uri,
        MaxUriTagValues,
        MeterFilter.deny()
      )
    )

    val binders = newBinders()
    binders.foreach(_.bindTo(registry))

    new Telemetry(config, prometheusRegistry, registry, tracing, binders.collect { case c: AutoCloseable => c })

  /** Convenience for a service `main`: identity from the environment, tracing from `OTEL_*`. */
  def start(serviceName: String): Telemetry = start(TelemetryConfig.fromEnv(serviceName))
