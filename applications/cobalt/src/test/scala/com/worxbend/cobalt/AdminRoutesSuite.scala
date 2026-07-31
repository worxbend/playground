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

package com.worxbend.cobalt

import com.worxbend.observability.Meters
import com.worxbend.observability.Telemetry
import io.circe.parser
import scala.concurrent.duration.DurationInt

/** The operational surface, asserted without binding a socket.
  *
  * Cask ships no test kit, so the alternative is an HTTP round trip in a unit test; splitting the answers out of the
  * annotated routes is what makes these three assertions cost nothing.
  */
final class AdminRoutesSuite extends munit.FunSuite:

  private def handlers(health: HealthChecks, telemetry: Telemetry): AdminHandlers =
    AdminHandlers(
      telemetry,
      health,
      DeadLetterAdmin(
        Fixtures.StubDeadLetterStore(),
        ReplayMetrics(telemetry.registry),
        ReplayConfig(enabled = true, maxRecords = 10, maxAttempts = 3, 1.second),
        Fixtures.Topic,
        "dlq"
      )
    )

  test("the paths match the shared vocabulary, so one scrape config covers all three services"):
    assertEquals(Meters.MetricsPath, "/metrics")
    assertEquals(AdminRoutes.LivenessPath, "/health/live")
    assertEquals(AdminRoutes.ReadinessPath, "/health/ready")

  test("the dead-letter routes live under one prefix, so an ingress can expose the probes without the replay"):
    // /metrics and /health are platform-owned and safe to expose; POST /admin/dlq/replay is the only route cobalt
    // serves that changes anything, and a prefix is what lets a network policy separate the two without a path list.
    assertEquals(AdminRoutes.DlqPath, "/admin/dlq")
    assertEquals(AdminRoutes.DlqRecordsPath, "/admin/dlq/records")
    assertEquals(AdminRoutes.DlqReplayPath, "/admin/dlq/replay")
    assert(!AdminRoutes.DlqPath.startsWith(Meters.HealthPath), "the replay surface must not sit under the probe path")

  test("metrics are served verbatim with the registry's own content type"):
    val telemetry = Fixtures.telemetry()
    try
      val reply = handlers(HealthChecks.create(), telemetry).metrics()
      assertEquals(reply.status, 200)
      assertEquals(reply.contentType, Telemetry.ContentType)
      assert(reply.body.contains("jvm_"), "the JVM binders must be in the same exposition")
    finally telemetry.close()

  test("liveness consults nothing, so a broker outage cannot crash-loop every replica"):
    val telemetry = Fixtures.telemetry()
    try
      val health = HealthChecks.create()
      health.broker.down("gone")
      health.database.down("gone")
      assertEquals(handlers(health, telemetry).live().status, 200)
    finally telemetry.close()

  test("readiness starts out false, before anything has been probed"):
    val telemetry = Fixtures.telemetry()
    try
      val reply = handlers(HealthChecks.create(), telemetry).ready()
      assertEquals(reply.status, 503)
      assert(reply.body.contains("not probed yet"))
    finally telemetry.close()

  test("readiness is true only when both dependencies are reachable"):
    val telemetry = Fixtures.telemetry()
    try
      val health = HealthChecks.create()
      health.broker.up()
      assertEquals(handlers(health, telemetry).ready().status, 503, "a consumer with no database is not ready")
      health.database.up()
      assertEquals(handlers(health, telemetry).ready().status, 200)
    finally telemetry.close()

  test("the readiness body names the failing dependency and its reason"):
    val telemetry = Fixtures.telemetry()
    try
      val health = HealthChecks.create()
      health.broker.up()
      health.database.down("connection refused")
      val json = parser.parse(handlers(health, telemetry).ready().body).toOption.get.hcursor
      assertEquals(json.get[String]("status").toOption, Some("OUT_OF_SERVICE"))
      assertEquals(json.downField("dependencies").downField("kafka").get[String]("status").toOption, Some("UP"))
      assertEquals(
        json.downField("dependencies").downField("postgresql").get[String]("detail").toOption,
        Some("connection refused")
      )
    finally telemetry.close()
