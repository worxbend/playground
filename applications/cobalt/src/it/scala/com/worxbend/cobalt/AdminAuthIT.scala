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

/** The admin surface behind a real socket, with and without a credential.
  *
  * **This is the test that fails without the fix.** Before it,
  * `POST /admin/consumer:restart?target=latest&dryRun=false` answered 200 to an anonymous caller and permanently
  * skipped every unconsumed event; `GET /admin/dlq/records` returned event payloads to anybody who could reach the
  * port. Every assertion below is one of those routes.
  *
  * `AdminAccessSuite` proves the *declaration* in [[AdminRoutes.Access]] covers every route Cask serves. This drives
  * every entry in that declaration over HTTP and asserts the served behaviour matches it, so the two cannot be right
  * about each other and wrong about the service. The table is walked rather than enumerated by hand: a route added with
  * an access decision but no guard fails here without anyone remembering to add a case.
  */
final class AdminAuthIT extends munit.FunSuite:

  private val fixture = FunFixture[(AdminServer, Telemetry)](
    setup = _ =>
      val telemetry = Telemetry.start(TelemetryConfig("cobalt-it", "0.0.0-it", "it"), Tracing.noop)
      val deadLetters = DeadLetterAdmin(
        Fixtures.StubDeadLetterStore(partitions = Vector(DlqPartitionDepth(0, 0L, 2L))),
        ReplayMetrics(telemetry.registry),
        ReplayConfig(enabled = true, maxRecords = 10, maxAttempts = 3, 1.second),
        Fixtures.Topic,
        "dlq"
      )
      given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic
      val handlers = AdminHandlers(
        telemetry,
        HealthChecks.create(),
        deadLetters,
        SupervisorAdmin(Fixtures.idleSupervisor, 5.seconds, SupervisorMetrics(telemetry.registry))
      )
      val server = AdminServer(CobaltRoutes(handlers, Tokens.admin()), "127.0.0.1", 0)
      server.start()
      (server, telemetry)
    ,
    teardown = (server, telemetry) =>
      server.stop()
      telemetry.close()
  )

  /** The route's own method: a POST route answers 404/405 to a GET, which would mask a missing guard. */
  private def call(server: AdminServer, path: String, headers: Map[String, String]): requests.Response =
    val url = s"http://127.0.0.1:${server.boundPort}$path"
    if AdminRoutes.Access.get(path).contains(Some(AdminScope.Write)) then
      requests.post(url, headers = headers, check = false)
    else requests.get(url, headers = headers, check = false)

  private def guarded: Vector[(String, AdminScope)] =
    AdminRoutes.Access.toVector.collect { case (path, Some(scope)) => path -> scope }.sortBy((path, _) => path)

  private def open: Vector[String] =
    // `/docs/assets` is a static-resource prefix, not a route in its own right: a bare GET on it is a 404 whether or
    // not it is guarded, so it is asserted through one of the files it actually serves.
    AdminRoutes.Access.toVector.collect { case (path, None) => path }.filter(_ != AdminRoutes.SwaggerAssetsPath).sorted

  fixture.test("no credential is 401 on every guarded route, with the RFC 6750 challenge"): (server, _) =>
    assert(guarded.sizeIs >= 10, s"only ${guarded.size} guarded routes: $guarded")
    guarded.foreach: (path, _) =>
      val response = call(server, path, Map.empty)
      assertEquals(response.statusCode, 401, path)
      assert(response.headers.contains("www-authenticate"), s"$path: ${response.headers.keys}")
      // The whole surface answers JSON, refusals included: a script that reads `error` does not have to branch.
      assert(response.text().contains("\"error\""), s"$path: ${response.text()}")

  fixture.test("a forged token is 401 on every guarded route"): (server, _) =>
    // The one that matters most: "there is a check" and "the check verifies the signature" are different claims, and
    // only this distinguishes them over the wire.
    val forged = Map(
      "Authorization" -> s"Bearer ${Tokens.signed(secret = "an-entirely-different-secret-of-sufficient-length")}"
    )
    guarded.foreach((path, _) => assertEquals(call(server, path, forged).statusCode, 401, path))

  fixture.test("a read token opens the reads and is 403 on everything that changes anything"): (server, _) =>
    val read = Map("Authorization" -> s"Bearer ${Tokens.signed(scopes = Set(JwtVerifier.ReadScope))}")
    guarded.foreach: (path, scope) =>
      val response = call(server, path, read)
      scope match
        case AdminScope.Read => assertEquals(response.statusCode, 200, path)
        // 403 and not 401: the operator's token is genuine and retrying with it cannot help.
        case AdminScope.Write => assertEquals(response.statusCode, 403, path)

  fixture.test("a write token opens everything, including the reads"): (server, _) =>
    val write = Map("Authorization" -> s"Bearer ${Tokens.signed(scopes = Set(JwtVerifier.WriteScope))}")
    guarded.foreach: (path, _) =>
      // `:restart` and `:replay` default to dryRun=true, so this walk plans and publishes nothing; the lifecycle
      // routes act on the idle supervisor, which owns no broker.
      assertEquals(call(server, path, write).statusCode, 200, path)

  fixture.test("the platform routes stay open, or the stack goes down"): (server, _) =>
    // Prometheus scrapes /metrics and the orchestrator probes /health/*; neither can hold a bearer token, and a
    // credential on either would show up as a permanently DOWN target and a crash-looping container.
    open.foreach: path =>
      val response = requests.get(s"http://127.0.0.1:${server.boundPort}$path", check = false)
      assert(response.statusCode == 200 || response.statusCode == 503, s"$path answered ${response.statusCode}")
    // And one Swagger asset, for the prefix route the walk above skips.
    assertEquals(
      requests
        .get(s"http://127.0.0.1:${server.boundPort}${AdminRoutes.SwaggerAssetsPath}/swagger-ui.css", check = false)
        .statusCode,
      200
    )

  fixture.test("the DLQ payloads an anonymous caller used to get back are not in the 401 body"): (server, _) =>
    val response = call(server, AdminRoutes.DlqRecordsPath, Map.empty)
    assertEquals(response.statusCode, 401)
    assert(!response.text().contains("records"), response.text())

  fixture.test("with authentication disabled the surface is open, and only then"): (server, telemetry) =>
    // The escape hatch has to work, or a deployment that needs it reaches for something worse. It is a separate
    // server on its own port so the assertion cannot be confused with the fixture's.
    val _ = server
    val off = Tokens.config.copy(enabled = false, secret = None)
    given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic
    val handlers = AdminHandlers(
      telemetry,
      HealthChecks.create(),
      DeadLetterAdmin(
        Fixtures.StubDeadLetterStore(),
        ReplayMetrics(telemetry.registry),
        ReplayConfig(enabled = true, maxRecords = 10, maxAttempts = 3, 1.second),
        Fixtures.Topic,
        "dlq"
      ),
      SupervisorAdmin(Fixtures.idleSupervisor, 5.seconds, SupervisorMetrics(telemetry.registry))
    )
    val anonymous = AdminServer(CobaltRoutes(handlers, Tokens.admin(off)), "127.0.0.1", 0)
    anonymous.start()
    try
      val response =
        requests.get(s"http://127.0.0.1:${anonymous.boundPort}${AdminRoutes.ConsumerPath}", check = false)
      assertEquals(response.statusCode, 200)
    finally anonymous.stop()
