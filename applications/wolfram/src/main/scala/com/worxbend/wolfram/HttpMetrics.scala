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

package com.worxbend.wolfram

import com.worxbend.observability.Meters
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import sttp.model.StatusCode
import sttp.tapir.AnyEndpoint
import sttp.tapir.server.interceptor.metrics.MetricsRequestInterceptor
import sttp.tapir.server.metrics.EndpointMetric
import sttp.tapir.server.metrics.Metric

/** `http.server.requests`, timed by Micrometer and by nothing else.
  *
  * ADR §7.1 makes this the enforceable rule of the whole observability design: exactly one component times each HTTP
  * request in each of the three services, and it is always Micrometer, so the meter has identical names and tags in
  * Play, in Cask and here — and one Grafana dashboard works for the fleet. ADR §3.6 rejects `tapir-prometheus-metrics`
  * for precisely this reason: it publishes `request_total` / `request_duration_seconds`, which is a second, parallel
  * family that no shared dashboard can join against.
  *
  * **`uri` is the matched route template, never the raw path.** ADR §7.1 calls unbounded `uri` cardinality the single
  * most likely way to take down Prometheus in this system. `Telemetry` installs a cap on this exact meter and tag as a
  * backstop; using the template keeps the normal case bounded so the cap never has to fire.
  *
  * `/metrics` and the health probes are absent from this family for free: they are plain Vert.x routes, not Tapir
  * endpoints, so the interceptor never sees them. That is the ADR's requirement met by construction rather than by an
  * exclusion list that someone has to remember to update.
  */
object HttpMetrics:

  /** Micrometer's conventional outcome bucket. Five values, ever, so it is safe as a tag and useful as a filter. */
  private[wolfram] def outcome(status: Int): String =
    status / 100 match
      case 1 => "INFORMATIONAL"
      case 2 => "SUCCESS"
      case 3 => "REDIRECTION"
      case 4 => "CLIENT_ERROR"
      case 5 => "SERVER_ERROR"
      case _ => "UNKNOWN"

  /** The route template a request matched, e.g. `/events/batch`. */
  private[wolfram] def route(endpoint: AnyEndpoint): String = OpenApi.pathOf(endpoint)

  /** Records one completed request. Split out so it is testable without an interpreter. */
  private[wolfram] def record(registry: MeterRegistry, method: String, uri: String, status: Int, nanos: Long): Unit =
    registry
      .timer(
        Meters.HttpServerRequests,
        Tags.of(
          "method",
          method,
          Meters.TagKeys.Uri,
          uri,
          "status",
          status.toString,
          Meters.TagKeys.Outcome,
          outcome(status)
        )
      )
      .record(nanos, TimeUnit.NANOSECONDS)

  /** The interceptor to hand to a Tapir server's options.
    *
    * The timer is started in `onRequest` and stopped in `onResponseBody` rather than `onResponseHeaders`, so a slow
    * response body is measured as slow — for this API the two are the same, but the choice is the one that stays
    * correct if a streaming endpoint is ever added.
    */
  def interceptor(registry: MeterRegistry): MetricsRequestInterceptor[Future] =
    MetricsRequestInterceptor[Future](List(timer(registry)), Seq.empty)

  private def timer(registry: MeterRegistry): Metric[Future, MeterRegistry] =
    Metric[Future, MeterRegistry](
      registry,
      (request, meters, monad) =>
        monad.eval:
          val started = System.nanoTime()
          val method = request.method.method
          EndpointMetric[Future]()
            .onResponseBody((endpoint, response) =>
              monad.eval(record(meters, method, route(endpoint), response.code.code, System.nanoTime() - started))
            )
            .onException((endpoint, _) =>
              monad.eval(
                record(
                  meters,
                  method,
                  route(endpoint),
                  StatusCode.InternalServerError.code,
                  System.nanoTime() - started
                )
              )
            )
    )
