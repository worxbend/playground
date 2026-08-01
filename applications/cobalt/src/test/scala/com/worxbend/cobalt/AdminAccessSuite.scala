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
import munit.FunSuite
import scala.concurrent.duration.DurationInt

/** Which routes are guarded, checked against Cask's own dispatch table.
  *
  * **The failure this suite exists to catch is silent by construction.** A route added without a guard answers 200 to
  * anybody who can reach the port; nothing compiles differently, no test fails and no log line appears. The only way to
  * notice is to compare the routes the router actually serves against a declaration of what each one requires — which
  * is [[AdminRoutes.Access]], compared here in both directions.
  *
  * This suite proves the *declaration* is complete. `AdminAuthIT` proves the routes obey it, over a real socket.
  */
final class AdminAccessSuite extends FunSuite:

  private def servedRoutes: Vector[(String, Set[String])] =
    val telemetry = Fixtures.telemetry()
    try
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
        SupervisorAdmin(Fixtures.idleSupervisor, 5.seconds)
      )
      CobaltRoutes(handlers, Tokens.admin()).caskMetadata.value
        .map(entry => entry.endpoint.path -> entry.endpoint.methods.toSet)
        .toVector
    finally telemetry.close()

  test("every route the router serves has an access decision, and every decision names a real route"):
    val served = servedRoutes.map((path, _) => path).toSet
    assertEquals(served -- AdminRoutes.Access.keySet, Set.empty[String], "served with no access decision")
    assertEquals(AdminRoutes.Access.keySet -- served, Set.empty[String], "an access decision for no route")

  test("the reflection found the routes at all, so the comparison cannot pass vacuously"):
    // A guard on the guard, matching CobaltApiDocsSuite: if Cask's metadata ever stops being readable this way, the
    // test above becomes `Set.empty == Set.empty` and goes green over an unauthenticated admin API.
    assert(servedRoutes.sizeIs >= 16, s"only ${servedRoutes.size} routes discovered: $servedRoutes")

  test("everything under /admin requires a scope, and nothing else is reachable without one"):
    val open = AdminRoutes.Access.collect { case (path, None) => path }.toSet
    assert(
      !open.exists(_.startsWith("/admin")),
      s"an /admin route is open to anyone who can reach the port: ${open.filter(_.startsWith("/admin"))}"
    )
    // The open set is enumerated rather than derived, so widening it is a diff somebody reviews.
    assertEquals(
      open,
      Set(
        // Prometheus scrapes this and cannot hold a bearer token.
        Meters.MetricsPath,
        // The orchestrator probes these; a probe that needs a credential is a probe that fails on a rotation.
        AdminRoutes.LivenessPath,
        AdminRoutes.ReadinessPath,
        // A static description of this build's routes, already public in the repository. See AdminRoutes.Access.
        AdminRoutes.OpenApiJsonPath,
        AdminRoutes.OpenApiYamlPath,
        AdminRoutes.DocsPath,
        AdminRoutes.SwaggerAssetsPath
      )
    )

  test("every route that changes anything requires the write scope"):
    // Derived from the HTTP method rather than from a second list: this service's only mutating routes are its POSTs,
    // so a new POST that asked for `Read` fails here without anyone having to remember to add it.
    servedRoutes.foreach: (path, methods) =>
      if methods.contains("post") then
        assertEquals(AdminRoutes.Access.get(path), Some(Some(AdminScope.Write)), s"POST $path")

  test("the read routes are the GETs under /admin, and they are reads"):
    val reads = AdminRoutes.Access.collect { case (path, Some(AdminScope.Read)) => path }.toSet
    assertEquals(reads, Set(AdminRoutes.DlqPath, AdminRoutes.DlqRecordsPath, AdminRoutes.ConsumerPath))
    servedRoutes.foreach: (path, methods) =>
      if reads.contains(path) then assertEquals(methods, Set("get"), path)
