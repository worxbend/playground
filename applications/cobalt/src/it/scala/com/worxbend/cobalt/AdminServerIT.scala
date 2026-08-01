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

import com.worxbend.observability.Telemetry
import com.worxbend.observability.TelemetryConfig
import com.worxbend.observability.Tracing
import scala.concurrent.duration.DurationInt

/** Cask over a real socket.
  *
  * Cask ships no test kit, so ADR §9.3 puts this tier on `com.lihaoyi::requests` against an ephemeral port. The unit
  * suite already asserts every *answer*; what is only observable here is that the annotation macros produced the routes
  * they claim to, that Undertow binds, and that the status code survives the round trip — three things that are
  * invisible to a test which calls the handler directly.
  *
  * Port `0` and not a fixed number: this is what [[AdminServer]] owns its own Undertow instance for, since Cask's own
  * `main` never hands the bound port back.
  */
final class AdminServerIT extends munit.FunSuite:

  /** A write-scoped credential, since every /admin route now needs one. The refusals have their own suite. */
  private val credential: Map[String, String] = Map("Authorization" -> s"Bearer ${Tokens.signed()}")

  private val fixture = FunFixture[(AdminServer, HealthChecks, Telemetry)](
    setup = _ =>
      val telemetry = Telemetry.start(TelemetryConfig("cobalt-it", "0.0.0-it", "it"), Tracing.noop)
      val health = HealthChecks.create()
      val deadLetters = DeadLetterAdmin(
        Fixtures.StubDeadLetterStore(partitions = Vector(DlqPartitionDepth(0, 0L, 2L))),
        ReplayMetrics(telemetry.registry),
        ReplayConfig(enabled = true, maxRecords = 10, maxAttempts = 3, 1.second),
        Fixtures.Topic,
        "dlq"
      )
      given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic
      val consumer = SupervisorAdmin(Fixtures.idleSupervisor, 5.seconds, SupervisorMetrics(telemetry.registry))
      val server =
        AdminServer(
          CobaltRoutes(AdminHandlers(telemetry, health, deadLetters, consumer), Tokens.admin()),
          "127.0.0.1",
          0
        )
      server.start()
      (server, health, telemetry)
    ,
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

  fixture.test("the dead-letter routes exist, and the replay route is a POST"): (server, _, _) =>
    // The unit suite asserts every answer; what only a real socket can prove is that Cask's annotation macros produced
    // the routes they claim to — including that the one route which changes something is not reachable by GET.
    val base = s"http://127.0.0.1:${server.boundPort}"
    val summary = requests.get(base + AdminRoutes.DlqPath, headers = credential, check = false)
    assertEquals(summary.statusCode, 200)
    assert(summary.text().contains("\"outstanding\":2"), summary.text())

    assertEquals(
      requests.get(base + AdminRoutes.DlqRecordsPath + "?limit=5", headers = credential, check = false).statusCode,
      200
    )
    assertNotEquals(requests.get(base + AdminRoutes.DlqReplayPath, headers = credential, check = false).statusCode, 200)

  fixture.test("a replay dry run is the default, and query parameters reach the handler"): (server, _, _) =>
    val base = s"http://127.0.0.1:${server.boundPort}" + AdminRoutes.DlqReplayPath
    val defaulted = requests.post(base, headers = credential, check = false)
    assertEquals(defaulted.statusCode, 200)
    assert(defaulted.text().contains("\"dryRun\":true"), defaulted.text())
    // An over-limit request is refused rather than clamped, and the refusal survives the round trip as a 400.
    assertEquals(requests.post(base + "?limit=999", headers = credential, check = false).statusCode, 400)

  fixture.test("the consumer lifecycle routes are served, and the colon in a custom method survives routing"):
    (server, _, _) =>
      // The colon is the AIP-136 custom-method form and Cask has to match it as a literal path segment. A framework
      // that split on `:` would route `/admin/consumer:pause` to nothing, and the only way to find that out is to
      // ask a bound port — which is why this assertion lives here and not in a unit test.
      val status =
        requests.get(s"http://127.0.0.1:${server.boundPort}/admin/consumer", headers = credential, check = false)
      assertEquals(status.statusCode, 200)
      val body = io.circe.parser.parse(status.text()).getOrElse(fail("the status is not JSON"))
      assertEquals(body.hcursor.get[String]("state").toOption, Some("stopped"))
      assertEquals(body.hcursor.get[Boolean]("consuming").toOption, Some(false))

      // A lifecycle command over a real socket, reporting the state on both sides of itself.
      val paused =
        requests.post(s"http://127.0.0.1:${server.boundPort}/admin/consumer:pause", headers = credential, check = false)
      assertEquals(paused.statusCode, 200)
      val result = io.circe.parser.parse(paused.text()).getOrElse(fail("not JSON"))
      assertEquals(result.hcursor.get[String]("command").toOption, Some("pause"))
      assertEquals(result.hcursor.downField("status").get[String]("state").toOption, Some("paused"))

  fixture.test("restart plans by default and refuses an unknown target"): (server, _, _) =>
    val base = s"http://127.0.0.1:${server.boundPort}/admin/consumer:restart"
    // Default dryRun: the most destructive operation in this service must not fire because somebody omitted a flag.
    val planned = requests.post(s"$base?target=committed", headers = credential, check = false)
    assertEquals(planned.statusCode, 200)
    val plan = io.circe.parser.parse(planned.text()).getOrElse(fail("not JSON"))
    assertEquals(plan.hcursor.get[Boolean]("dryRun").toOption, Some(true))
    assertEquals(plan.hcursor.get[Boolean]("committed").toOption, Some(false))

    // A typo in the target is a 400 rather than a silent fallback to `committed`, which would be a different
    // operation performed under the name of the one that was asked for.
    val unknown = requests.post(s"$base?target=yesterday", headers = credential, check = false)
    assertEquals(unknown.statusCode, 400)
    assert(unknown.text().contains("yesterday"), unknown.text())

    // Coordinates supplied with the wrong target are refused, not ignored.
    val mismatched = requests.post(s"$base?target=latest&offsets=t/0/5", headers = credential, check = false)
    assertEquals(mismatched.statusCode, 400)

  fixture.test("a POST-only route refuses a GET"): (server, _, _) =>
    val response =
      requests.get(s"http://127.0.0.1:${server.boundPort}/admin/consumer:pause", headers = credential, check = false)
    assert(response.statusCode == 404 || response.statusCode == 405, response.statusCode.toString)
