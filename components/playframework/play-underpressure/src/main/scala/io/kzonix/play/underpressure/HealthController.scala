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

package io.kzonix.play.underpressure

import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

@Singleton
final class HealthController @Inject() (
    cc: ControllerComponents,
    providers: Set[HealthProvider]
)(using ec: ExecutionContext)
    extends AbstractController(cc):

  /** Liveness — is the process running?
    *
    * Deliberately consults no dependencies. A liveness probe that fails during a downstream outage gets the container
    * killed and restarted, turning a recoverable outage into a restart loop. Dependency state belongs in [[ready]].
    */
  def live: Action[AnyContent] = Action:
    Ok(Json.obj("status" -> HealthStatus.Up.toString))

  /** Readiness — should this instance receive traffic?
    *
    * Aggregates every bound [[HealthProvider]]. Any `Down` check fails the probe with 503 so the instance is pulled
    * from the load balancer; `Degraded` still serves traffic.
    */
  def ready: Action[AnyContent] = Action.async:
    Future
      .traverse(providers.toList)(runCheck)
      .map: checks =>
        val overall = aggregate(checks)
        val body    = Json.obj(
          "status" -> overall.toString,
          "checks" -> Json.toJson(checks.map(render))
        )
        if overall == HealthStatus.Down then ServiceUnavailable(body) else Ok(body)

  private def runCheck(provider: HealthProvider): Future[HealthCheck] =
    provider
      .check()
      .recover:
        case NonFatal(error) => HealthCheck.down(provider.name, error.getMessage)

  private def aggregate(checks: List[HealthCheck]): HealthStatus =
    if checks.exists(_.status == HealthStatus.Down) then HealthStatus.Down
    else if checks.exists(_.status == HealthStatus.Degraded) then HealthStatus.Degraded
    else HealthStatus.Up

  private def render(check: HealthCheck): JsValue =
    Json
      .obj("name" -> check.name, "status" -> check.status.toString)
      .++(check.detail.fold(Json.obj())(detail => Json.obj("detail" -> detail)))
