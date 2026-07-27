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

package io.kzonix.cobalt

import io.kzonix.observability.Telemetry
import io.kzonix.observability.TelemetryConfig
import io.kzonix.observability.Tracing

/** Cask over a real socket.
  *
  * Cask ships no test kit, so ADR §9.3 puts this tier on `com.lihaoyi::requests` against an ephemeral port. The unit
  * suite already asserts every *answer*; what is only observable here is that the annotation macros produced the
  * routes they claim to, that Undertow binds, and that the status code survives the round trip — three things that are
  * invisible to a test which calls the handler directly.
  *
  * Port `0` and not a fixed number: this is what [[AdminServer]] owns its own Undertow instance for, since Cask's own
  * `main` never hands the bound port back.
  */
final class AdminServerIT extends munit.FunSuite:

  private val fixture = FunFixture[(AdminServer, HealthChecks, Telemetry)](
    setup = _ =>
      val telemetry = Telemetry.start(TelemetryConfig("cobalt-it", "0.0.0-it", "it"), Tracing.noop)
      val health = HealthChecks.create()
      val server = AdminServer(CobaltRoutes(AdminHandlers(telemetry, health)), "127.0.0.1", 0)
      server.start()
      (server, health, telemetry),
    teardown = (server, _, telemetry) =>
      server.stop()
      telemetry.close()
  )

  fixture.test("liveness answers 200 without consulting anything"): (server, health, _) =>
    health.broker.down("gone")
    val response = requests.get(s"http://127.0.0.1:${server.boundPort}${AdminRoutes.LivenessPath}", check = false)
    assertEquals(response.statusCode, 200)

  fixture.test("readiness is 503 until both dependencies have been probed"): (server, health, _) =>
    val url = s"http://127.0.0.1:${server.boundPort}${AdminRoutes.ReadinessPath}"
    assertEquals(requests.get(url, check = false).statusCode, 503)
    health.broker.up()
    health.database.up()
    assertEquals(requests.get(url, check = false).statusCode, 200)

  fixture.test("the metrics endpoint serves the Prometheus exposition"): (server, _, _) =>
    val response = requests.get(s"http://127.0.0.1:${server.boundPort}/metrics", check = false)
    assertEquals(response.statusCode, 200)
    assert(response.text().contains("jvm_"), "the JVM binders must be in the same exposition")
