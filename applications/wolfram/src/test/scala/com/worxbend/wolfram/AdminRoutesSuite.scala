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
import com.worxbend.observability.Telemetry
import com.worxbend.observability.TelemetryConfig
import com.worxbend.observability.Tracing
import io.circe.parser
import munit.FunSuite

/** The operational surface, and the split between liveness and readiness.
  *
  * The split is the point: a broker outage must fail readiness — so the load balancer stops sending requests that can
  * only end in 503 — and must *not* fail liveness, because restarting every replica does not repair someone else's
  * broker and turns a recoverable dependency failure into a crash loop.
  */
final class AdminRoutesSuite extends FunSuite:

  private val telemetryFixture = FunFixture[Telemetry](
    setup = _ => Telemetry.start(TelemetryConfig("wolfram", "test", "instance-0"), Tracing.noop),
    teardown = _.close()
  )

  test("liveness never consults the broker"):
    assertEquals(AdminRoutes.live.status, 200)
    assert(AdminRoutes.live.body.contains("UP"))

  telemetryFixture.test("readiness is 200 when the broker is reachable"): telemetry =>
    val routes = AdminRoutes(telemetry, Fixtures.StubPublisher())
    val reply = readiness(routes)
    assertEquals(reply.status, 200)
    assertEquals(field(reply, "broker"), Some("reachable"))

  telemetryFixture.test("readiness is 503 when it is not, and says why"): telemetry =>
    val routes = AdminRoutes(telemetry, Fixtures.unavailable)
    val reply = readiness(routes)
    assertEquals(reply.status, 503)
    assertEquals(field(reply, "status"), Some("OUT_OF_SERVICE"))

  telemetryFixture.test("the metrics route serves the shared registry's exposition verbatim"): telemetry =>
    val _ = telemetry.registry.counter(Meters.IngestRejected, Meters.TagKeys.Reason, Meters.Reasons.Malformed)
    // Through the route rather than through `telemetry.scrape()`: the thing that can regress is the route serving
    // some *other* registry, or the exposition's own content type being replaced with `application/json`.
    val reply = AdminRoutes(telemetry, Fixtures.StubPublisher()).metrics
    assertEquals(reply.status, 200)
    assertEquals(reply.contentType, "text/plain; version=0.0.4; charset=utf-8")
    assertEquals(reply.contentType, Telemetry.ContentType)
    assert(reply.body.contains("ingest_events_rejected"), reply.body.take(500))

  test("the probe paths are the ones the shared vocabulary defines, so one ingress config covers the fleet"):
    assertEquals(AdminRoutes.LivenessPath, s"${Meters.HealthPath}/live")
    assertEquals(AdminRoutes.ReadinessPath, s"${Meters.HealthPath}/ready")
    assert(Meters.UninstrumentedPaths.contains(Meters.MetricsPath))

  /** Reaches the readiness body without a Vert.x server: the reply is a pure function of the publisher's state, and
    * binding a port to observe it would test Vert.x rather than this.
    */
  private def readiness(routes: AdminRoutes): AdminRoutes.Reply = routes.readiness

  private def field(reply: AdminRoutes.Reply, name: String): Option[String] =
    parser.parse(reply.body).toOption.flatMap(_.hcursor.get[String](name).toOption)
