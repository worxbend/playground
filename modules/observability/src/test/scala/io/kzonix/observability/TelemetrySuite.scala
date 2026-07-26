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

import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** The registry, the binders and the exposition.
  *
  * **Every test here calls `scrape()` for real, and that is deliberate (ADR §3.5).**
  * `micrometer-registry-prometheus:1.17.0` is compiled against `prometheus-metrics-core:1.7.0`, and the build forces
  * 1.8.0. If those two ever drift into a genuine binary incompatibility, nothing fails at compile time and nothing
  * fails at startup — the first symptom is a `NoSuchMethodError` thrown by the *first Prometheus scrape*, i.e. in
  * production, seconds after a deploy that all the other tests declared green. Asserting on `registry.getMeters` would
  * pass in exactly that scenario. Rendering the text is the only assertion that exercises the boundary.
  */
final class TelemetrySuite extends munit.FunSuite:

  private val config = TelemetryConfig(serviceName = "ferrite", serviceVersion = "1.2.3", instanceId = "pod-7")

  /** Each test gets its own registry: meter filters are applied at registration time, so a registry shared across tests
    * would make the assertions depend on the order the tests happened to run in.
    */
  private def withTelemetry[A](tags: Tags = Telemetry.commonTags(config))(body: Telemetry => A): A =
    val telemetry = Telemetry.start(config, Tracing.noop, tags)
    try body(telemetry)
    finally telemetry.close()

  private def meterNames(telemetry: Telemetry): List[String] =
    telemetry.registry.getMeters.asScala.map(_.getId.getName).toList

  test("scrape renders a Prometheus exposition rather than throwing at the 1.7.0/1.8.0 boundary"):
    withTelemetry() { telemetry =>
      val exposition = telemetry.scrape()
      assert(exposition.nonEmpty, "the exposition is empty")
      assert(exposition.contains("# HELP"), "no HELP lines — this is not Prometheus text format")
      assert(exposition.contains("# TYPE"), "no TYPE lines — this is not Prometheus text format")
    }

  test("the JVM and system binders are bound and appear in the exposition"):
    withTelemetry() { telemetry =>
      val exposition = telemetry.scrape()
      // One representative meter per binder family, in the Prometheus spelling — that is what a dashboard queries.
      val expected = List(
        "jvm_memory_used_bytes", // JvmMemoryMetrics
        "jvm_threads_live_threads", // JvmThreadMetrics
        "jvm_classes_loaded_classes", // ClassLoaderMetrics
        "jvm_info", // JvmInfoMetrics
        "process_uptime_seconds", // UptimeMetrics
        "system_cpu_count" // ProcessorMetrics
      )
      expected.foreach(meter => assert(exposition.contains(meter), s"missing $meter in the exposition"))
    }

  test("the GC and heap-pressure binders are bound"):
    withTelemetry() { telemetry =>
      val names = meterNames(telemetry)
      assert(names.exists(_.startsWith("jvm.gc")), "JvmGcMetrics did not bind")
      assert(names.contains("jvm.memory.usage.after.gc"), "JvmHeapPressureMetrics did not bind")
    }

  test("the virtual-thread binder is bound (micrometer-java21 on JDK 25)"):
    withTelemetry() { telemetry =>
      // VirtualThreadMetrics publishes from a JFR stream, so its meters may carry no samples during a short test; the
      // registration itself is what proves the micrometer-java21 artifact resolved and bound without error.
      val names = meterNames(telemetry)
      assert(names.exists(_.startsWith("jvm.threads.virtual")), s"no virtual-thread meters among ${names.size} meters")
    }

  test("common tags are applied to every meter"):
    withTelemetry() { telemetry =>
      telemetry.registry.counter(Meters.IngestReceived).increment()
      val exposition = telemetry.scrape()
      assert(exposition.contains("""service="ferrite""""), "the service tag is missing")
      assert(exposition.contains("""version="1.2.3""""), "the version tag is missing")
    }

  test("instance is not a common tag by default — Prometheus supplies it from the scrape target"):
    withTelemetry() { telemetry =>
      assert(!telemetry.scrape().contains("""instance="pod-7""""), "instance leaked into the exposition")
    }

  test("instance can be opted into for deployments with no per-replica scrape target"):
    withTelemetry(Telemetry.commonTagsWithInstance(config)) { telemetry =>
      assert(telemetry.scrape().contains("""instance="pod-7""""))
    }

  test("a domain meter from the shared vocabulary survives the round trip to text"):
    withTelemetry() { telemetry =>
      telemetry.registry
        .counter(
          Meters.IngestReceived,
          Meters.TagKeys.EventType,
          "com.example.reading",
          Meters.TagKeys.Mode,
          Meters.Modes.Binary
        )
        .increment(3)
      val exposition = telemetry.scrape()
      // Micrometer's naming convention turns dots into underscores and appends _total to a counter.
      assert(exposition.contains("ingest_events_received_total"), "the counter did not reach the exposition")
      assert(exposition.contains("""type="com.example.reading""""))
      assert(exposition.contains("""mode="binary""""))
    }

  test("the uri tag is capped, so one timeseries per search permalink cannot happen"):
    withTelemetry() { telemetry =>
      val attempted = Telemetry.MaxUriTagValues + 50
      (1 to attempted).foreach: i =>
        telemetry.registry
          .timer(Meters.HttpServerRequests, Meters.TagKeys.Uri, s"/search/$i")
          .record(1L, TimeUnit.MILLISECONDS)
      val series = telemetry.scrape().linesIterator.count(_.startsWith("http_server_requests_seconds_count{"))
      assert(series > 0, "the http.server.requests family is absent entirely")
      assert(series <= Telemetry.MaxUriTagValues, s"$series uri series survived a cap of ${Telemetry.MaxUriTagValues}")
      assert(series < attempted, "the cardinality cap did not engage")
    }

  test("the Prometheus registry is shared, so a native collector lands in the same exposition"):
    withTelemetry() { telemetry =>
      // The anti-double-instrumentation property of ADR §7.1: one PrometheusRegistry, therefore one /metrics.
      assertEquals(telemetry.registry.getPrometheusRegistry, telemetry.prometheusRegistry)
    }

  test("two Telemetry instances do not share state — no global registry"):
    withTelemetry() { first =>
      withTelemetry() { second =>
        first.registry.counter("probe.isolation").increment()
        assert(first.scrape().contains("probe_isolation_total"))
        assert(!second.scrape().contains("probe_isolation_total"), "the registry is behaving like a global")
      }
    }

  test("close releases the binders without throwing"):
    val telemetry = Telemetry.start(config, Tracing.noop)
    assert(telemetry.scrape().nonEmpty)
    telemetry.close()

  test("the scrape content type is fixed and shared"):
    assertEquals(Telemetry.ContentType, "text/plain; version=0.0.4; charset=utf-8")
