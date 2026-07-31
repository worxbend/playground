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

package com.worxbend.ferrite.controllers

import com.worxbend.ferrite.wiring.Readiness
import com.worxbend.observability.Telemetry
import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.libs.json.JsObject
import play.api.libs.json.Json
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

/** The operational surface: one metrics exposition and two probes with genuinely different jobs.
  *
  * **Liveness consults nothing.** A liveness probe that fails because PostgreSQL is unreachable gets the container
  * killed, and the replacement fails the same probe, so a recoverable database outage becomes a restart loop that also
  * discards every warmed connection pool in the fleet. Liveness answers one question — is this JVM still able to serve
  * an HTTP request — and it answers it without a dependency.
  *
  * **Readiness consults the database**, because a ferrite replica that cannot reach PostgreSQL has nothing to serve and
  * should be taken out of the load-balancer rotation rather than returning 500s. A failed readiness probe removes
  * traffic; it does not remove the process.
  *
  * `/metrics` and the `/health` subtree are excluded from `http.server.requests` by
  * [[com.worxbend.ferrite.web.MetricsFilter]] (ADR §7.1) — a scrape endpoint that times itself inflates the request
  * rate of every dashboard that reads it.
  */
@Singleton
final class OpsController @Inject() (cc: ControllerComponents, telemetry: Telemetry, readiness: Readiness)(using
  ec: ExecutionContext
) extends AbstractController(cc):

  /** The Prometheus text exposition, served verbatim with the registry's own content type.
    *
    * No content negotiation and no caching: `micrometer-registry-prometheus` renders exactly one format, and a cached
    * scrape is a lie about the moment it describes.
    */
  def metrics: Action[AnyContent] = Action:
    Ok(telemetry.scrape()).as(Telemetry.ContentType)

  /** Liveness. Constant by design — see the class comment. */
  def live: Action[AnyContent] = Action:
    Ok(status("UP")).withHeaders(CACHE_CONTROL -> NoStore)

  /** Readiness. 200 when the read pool can hand out a valid connection, 503 otherwise.
    *
    * A thrown exception is a "not ready", not a 500: the probe's contract is to report a state, and a probe endpoint
    * that can itself fail with a stack trace is one an orchestrator cannot interpret.
    */
  def ready: Action[AnyContent] = Action.async:
    readiness.check().transform {
      case Success(true)   => Success(Ok(status("UP")).withHeaders(CACHE_CONTROL -> NoStore))
      case Success(false)  => Success(ServiceUnavailable(status("DOWN", "the read pool has no usable connection")))
      case Failure(reason) =>
        Success(
          ServiceUnavailable(status("DOWN", Option(reason.getMessage).getOrElse(reason.getClass.getName)))
        )
    }

  private val NoStore: String = "no-store"

  private def status(state: String, detail: String = ""): JsObject =
    if detail.isEmpty then Json.obj("status" -> state)
    else Json.obj("status" -> state, "detail" -> detail)
