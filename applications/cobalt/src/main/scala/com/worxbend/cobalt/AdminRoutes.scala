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
import io.circe.Json

/** A status code, a content type and a body — the whole of cobalt's HTTP contract. */
final case class AdminReply(status: Int, contentType: String, body: String)

/** The operational answers, computed without a socket.
  *
  * **Cask is cobalt's HTTP surface and nothing more** (ADR §1). There are no business endpoints here and there never
  * will be: events arrive over Kafka, and an HTTP write path would be a second, unordered, uncommitted way into the
  * same database.
  *
  * Splitting the *answers* from the annotated route class is what makes them testable at all — Cask has no test kit, so
  * the alternative is binding a port in a unit test. Here the routes are three one-line delegations and every decision
  * lives in a pure method.
  */
final class AdminHandlers(telemetry: Telemetry, health: HealthChecks, deadLetters: DeadLetterAdmin):

  /** The Prometheus exposition, served verbatim with the registry's own content type.
    *
    * One scrape endpoint for the whole process, because `modules/observability` hands out one `PrometheusRegistry` (ADR
    * §7.1) — Micrometer's meters, the JVM binders and anything a library registers natively all land in this one body,
    * with no bridging and no second port.
    */
  def metrics(): AdminReply = AdminReply(200, Telemetry.ContentType, telemetry.scrape())

  /** Liveness: the process is up and its HTTP handler answers. **Consults no dependency, deliberately.**
    *
    * A liveness probe that checked Kafka would fail on every replica the moment a broker went down, and the
    * orchestrator would restart them all — turning a recoverable dependency outage into a crash loop that outlives it.
    * Restarting a stateless consumer does not repair someone else's broker.
    */
  def live(): AdminReply = AdminRoutes.json(200, Json.obj("status" -> Json.fromString("UP")))

  /** Readiness: can this replica do its job right now?
    *
    * Both dependencies count. Without Kafka there is nothing to consume; without PostgreSQL there is nowhere to put it,
    * and a consumer that keeps polling while every insert fails burns through its restart budget and then dies. The
    * unreachable dependency's reason is included in the body so the probe itself carries the diagnosis.
    *
    * The values are read from [[DependencyHealth]], never probed inline — see that class for why.
    */
  def ready(): AdminReply =
    val ok = health.ready
    AdminRoutes.json(
      if ok then 200 else 503,
      Json.obj(
        "status" -> Json.fromString(if ok then "UP" else "OUT_OF_SERVICE"),
        "dependencies" -> Json.obj(
          health.dependencies.map { dependency =>
            dependency.name -> Json.obj(
              "status" -> Json.fromString(if dependency.reachable then "UP" else "DOWN"),
              "detail" -> dependency.reason.fold(Json.Null)(Json.fromString)
            )
          }*
        )
      )
    )

  /** How deep the DLQ is and what a replay is allowed to do. See [[DeadLetterAdmin.summary]]. */
  def dlq(): AdminReply = deadLetters.summary()

  /** A bounded, newest-first page of dead letters. See [[DeadLetterAdmin.records]]. */
  def dlqRecords(limit: Int, reason: String): AdminReply = deadLetters.records(limit, reason)

  /** Plans — and, with `dryRun=false`, commits — a replay. See [[DeadLetterAdmin.replay]]. */
  def dlqReplay(limit: Int, reason: String, refs: String, dryRun: Boolean): AdminReply =
    deadLetters.replay(limit, reason, refs, dryRun)

object AdminRoutes:

  /** Under [[Meters.HealthPath]] so one Prometheus scrape config and one ingress rule cover all three services. */
  val LivenessPath: String = Meters.HealthPath + "/live"
  val ReadinessPath: String = Meters.HealthPath + "/ready"

  /** The dead-letter surface.
    *
    * Under `/admin/` and not at the root because these are the first routes cobalt has ever served that are neither a
    * scrape nor a probe: the prefix is what lets an ingress or a network policy expose `/metrics` and `/health` to the
    * platform while keeping the replay endpoint on the inside, without enumerating paths one at a time.
    *
    * **This is not a contradiction of "cobalt is not an HTTP API".** ADR §1 forbids a business *write* path over HTTP —
    * a second, unordered, uncommitted way into the database. Replay is the opposite of that: it puts records back onto
    * Kafka, so every one of them still travels the single ordered committed path through the consumer, and the database
    * never hears from this endpoint at all.
    */
  val DlqPath: String = "/admin/dlq"
  val DlqRecordsPath: String = DlqPath + "/records"
  val DlqReplayPath: String = DlqPath + "/replay"

  val JsonContentType: String = "application/json; charset=utf-8"

  def json(status: Int, body: Json): AdminReply = AdminReply(status, JsonContentType, body.noSpaces)
