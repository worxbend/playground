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

import io.circe.Json
import io.kzonix.observability.Meters
import io.kzonix.observability.Telemetry
import io.vertx.core.Handler
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext

/** The operational surface: metrics, health, and the OpenAPI document.
  *
  * **These are plain Vert.x routes and not Tapir endpoints, on purpose.** ADR §7.1 requires `/metrics` and the health
  * probes to be excluded from `http.server.requests`, because they are the highest-frequency requests the process
  * serves, they are not user traffic, and leaving them in inflates request rate while dragging latency percentiles
  * down. Mounting them outside the Tapir interpreter makes the exclusion structural: [[HttpMetrics]] never sees them,
  * so no exclusion list exists to fall out of date. They are also absent from the OpenAPI document for the same reason
  * — they are not part of the API's contract with its clients.
  */
final class AdminRoutes(telemetry: Telemetry, publisher: EventPublisher):

  /** Mounts every operational route onto `router`. */
  def mount(router: Router): Unit =
    val _ = router.get(Meters.MetricsPath).handler(handler(metrics))
    val _ = router.get(AdminRoutes.LivenessPath).handler(handler(_ => AdminRoutes.live))
    val _ = router.get(AdminRoutes.ReadinessPath).handler(handler(_ => readiness))
    val _ = router.get(AdminRoutes.OpenApiPath).handler(handler(_ => AdminRoutes.openApi))

  /** The Prometheus exposition, served verbatim with the registry's own content type. */
  private[wolfram] def metrics(context: RoutingContext): AdminRoutes.Reply =
    val _ = context
    AdminRoutes.Reply(200, Telemetry.ContentType, telemetry.scrape())

  /** Readiness reflects broker reachability, because a wolfram that cannot publish cannot do the one thing it exists
    * for — every request it accepts would end in a 503, so taking it out of the load balancer is correct.
    *
    * Liveness deliberately does **not** consult the broker. A Kafka outage would otherwise fail liveness on every
    * replica at once and restart them all, turning a recoverable dependency failure into a crash loop that outlives it
    * — restarting a stateless process does not repair someone else's broker.
    */
  private[wolfram] def readiness: AdminRoutes.Reply =
    val reachable = publisher.brokerReachable
    AdminRoutes.reply(
      if reachable then 200 else 503,
      Json.obj(
        "status" -> Json.fromString(if reachable then "UP" else "OUT_OF_SERVICE"),
        "broker" -> Json.fromString(if reachable then "reachable" else "unreachable")
      )
    )

  private def handler(reply: RoutingContext => AdminRoutes.Reply): Handler[RoutingContext] =
    context =>
      val response = reply(context)
      val _ = context
        .response()
        .setStatusCode(response.status)
        .putHeader("content-type", response.contentType)
        .end(response.body)

object AdminRoutes:

  /** Split liveness and readiness, under [[Meters.HealthPath]] so one Prometheus/ingress config covers all three
    * services.
    */
  val LivenessPath: String = Meters.HealthPath + "/live"
  val ReadinessPath: String = Meters.HealthPath + "/ready"

  /** Served rather than published as a file so the document can never describe a build other than the running one. */
  val OpenApiPath: String = "/openapi.json"

  val JsonContentType: String = "application/json; charset=utf-8"

  /** A status code, a content type and a body — everything these routes ever need. */
  final case class Reply(status: Int, contentType: String, body: String)

  def reply(status: Int, body: Json): Reply = Reply(status, JsonContentType, body.noSpaces)

  /** Liveness: the process is running and its event loop is answering. Consults nothing else — see
    * [[AdminRoutes.readiness]].
    */
  val live: Reply = reply(200, Json.obj("status" -> Json.fromString("UP")))

  /** The generated OpenAPI document. */
  def openApi: Reply = reply(200, OpenApi.document())
