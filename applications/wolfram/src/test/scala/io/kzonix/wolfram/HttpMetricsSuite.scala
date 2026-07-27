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

package io.kzonix.wolfram

import io.kzonix.observability.Meters
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import munit.FunSuite

/** `http.server.requests`, the one meter family that must be identical in all three services.
  *
  * The assertions are on the *tags*, because that is what a shared dashboard joins on: a wolfram panel grouping by
  * `uri` and a ferrite panel grouping by `path` is two dashboards, not one.
  */
final class HttpMetricsSuite extends FunSuite:

  test("the uri tag is the route template, never a raw path — the cardinality rule of ADR §7.1"):
    assertEquals(HttpMetrics.route(Endpoints.publishEvent), "/events")
    assertEquals(HttpMetrics.route(Endpoints.publishBatch), "/events/batch")

  test("outcome buckets follow Micrometer's convention"):
    assertEquals(HttpMetrics.outcome(202), "SUCCESS")
    assertEquals(HttpMetrics.outcome(400), "CLIENT_ERROR")
    assertEquals(HttpMetrics.outcome(503), "SERVER_ERROR")

  test("a recorded request lands on the shared meter name with the shared tag keys"):
    val registry = SimpleMeterRegistry()
    HttpMetrics.record(registry, "POST", "/events", 202, 1_000_000L)
    val timer = registry
      .find(Meters.HttpServerRequests)
      .tag(Meters.TagKeys.Uri, "/events")
      .tag(Meters.TagKeys.Outcome, "SUCCESS")
      .tag("status", "202")
      .timer()
    assert(timer != null, registry.getMeters.toString)
    assertEquals(timer.count(), 1L)
