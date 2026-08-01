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

/** A status code, a content type, a body and any extra headers — the whole of cobalt's HTTP contract.
  *
  * `headers` is empty for every answer a handler produces and non-empty for exactly one thing: the `WWW-Authenticate`
  * challenge RFC 6750 requires on a 401. Carrying it on the reply rather than setting it at the route is what keeps
  * [[AdminAuth]] testable without Undertow.
  */
final case class AdminReply(status: Int, contentType: String, body: String, headers: Seq[(String, String)] = Nil)

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
final class AdminHandlers(
  telemetry: Telemetry,
  health: HealthChecks,
  deadLetters: DeadLetterAdmin,
  consumer: SupervisorAdmin
):

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

  /** The generated OpenAPI document, JSON and YAML. Served rather than published as a file so it can never describe a
    * build other than the running one.
    */
  def openApiJson(): AdminReply = AdminRoutes.json(200, CobaltApiDocs.json)

  def openApiYaml(): AdminReply = AdminReply(200, "application/yaml; charset=utf-8", CobaltApiDocs.yaml)

  /** Swagger UI, rendered from the `swagger-ui` webjar already on the classpath.
    *
    * Twelve lines of HTML rather than a second HTTP stack. Tapir's `SwaggerUI` bundle produces *server endpoints*,
    * which need a tapir server interpreter — and putting one beside Cask would give cobalt two routers, which is the
    * thing ADR §1 is most explicit about not doing. The assets themselves come from the webjar, served by Cask's own
    * static-resource route, so nothing is fetched from a CDN and the page works on a host with no internet.
    */
  def docs(): AdminReply = AdminReply(200, "text/html; charset=utf-8", AdminRoutes.SwaggerPage)

  /** The consumer's lifecycle state and offsets. See [[SupervisorAdmin]]. */
  def consumerStatus(): AdminReply = consumer.status()

  def consumerPause(): AdminReply = consumer.pause()
  def consumerResume(): AdminReply = consumer.resume()
  def consumerStop(): AdminReply = consumer.stop()
  def consumerStart(): AdminReply = consumer.start()

  def consumerRestart(target: String, offsets: String, dryRun: Boolean): AdminReply =
    consumer.restart(target, offsets, dryRun)

  def consumerClearCheckpoints(): AdminReply = consumer.clearCheckpoints()

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

  /** The colon form, not `/admin/dlq/replay`.
    *
    * Replay is a **custom method** on the DLQ resource, not a sub-resource of it, and AIP-136 spells that with a colon.
    * `/admin/dlq/records` keeps its slash because it genuinely is a sub-collection — the distinction is the one the
    * convention exists to make. Moved here after the document and the routes were compared and disagreed;
    * `CobaltApiDocsSuite` is what noticed.
    */
  val DlqReplayPath: String = DlqPath + ":replay"

  /** The consumer lifecycle surface.
    *
    * **Custom methods use a colon**, matching wolfram's `/v1/events:batchCreate` and for the same reason (AIP-136):
    * `/admin/consumer/pause` is indistinguishable from a sub-resource called `pause`, and the colon form cannot collide
    * with one. Two services in one system should not disagree about how a verb is spelled.
    */
  val ConsumerPath: String = "/admin/consumer"
  val ConsumerPausePath: String = ConsumerPath + ":pause"
  val ConsumerResumePath: String = ConsumerPath + ":resume"
  val ConsumerStopPath: String = ConsumerPath + ":stop"
  val ConsumerStartPath: String = ConsumerPath + ":start"
  val ConsumerRestartPath: String = ConsumerPath + ":restart"
  val ConsumerClearCheckpointsPath: String = ConsumerPath + ":clearCheckpoints"

  val JsonContentType: String = "application/json; charset=utf-8"

  /** Where the document and the UI are mounted, matching wolfram so an operator learns one convention. */
  val OpenApiJsonPath: String = "/openapi.json"
  val OpenApiYamlPath: String = "/openapi.yaml"
  val DocsPath: String = "/docs"

  /** Where Cask serves the webjar's assets from. The version is in the path because that is how webjars are laid out,
    * and hard-coding it here would break silently on a dependency bump — so it is read from the classpath.
    */
  val SwaggerAssetsPath: String = "/docs/assets"

  /** The Swagger UI page.
    *
    * Self-contained and CDN-free: every asset comes from the `swagger-ui` webjar on this service's own classpath, so
    * the page renders on a host with no internet — which a homelab behind a firewall is, and which is exactly when
    * somebody needs the docs.
    */
  val SwaggerPage: String =
    s"""<!doctype html>
       |<html lang="en">
       |<head>
       |  <meta charset="utf-8">
       |  <meta name="viewport" content="width=device-width, initial-scale=1">
       |  <title>${CobaltApiDocs.Title}</title>
       |  <link rel="stylesheet" href="$SwaggerAssetsPath/swagger-ui.css">
       |</head>
       |<body>
       |  <div id="swagger"></div>
       |  <script src="$SwaggerAssetsPath/swagger-ui-bundle.js"></script>
       |  <script>
       |    window.ui = SwaggerUIBundle({ url: "$OpenApiJsonPath", dom_id: "#swagger", deepLinking: true });
       |  </script>
       |</body>
       |</html>
       |""".stripMargin

  def json(status: Int, body: Json): AdminReply = AdminReply(status, JsonContentType, body.noSpaces)

  /** Every path this service serves, and what it takes to call it. `None` means deliberately unauthenticated.
    *
    * **This table exists because the failure it prevents is silent.** A route added without a guard answers 200 to
    * anybody, and nothing compiles differently, no test fails and no log line appears. `AdminAccessSuite` compares this
    * map against Cask's own dispatch table in both directions, so a new route with no entry here fails a unit test
    * before it is ever deployed, and `AdminAuthIT` drives every entry over a real socket and asserts the observed
    * status matches the declaration — so the table cannot be right while the routes are wrong.
    *
    * **What is open, and why.**
    *
    *   - `/metrics` and the two `/health` probes must stay open or the stack goes down: Prometheus scrapes one and the
    *     container orchestrator probes the others, and neither can hold a bearer token. They disclose meter values and
    *     a dependency-reachability boolean — no event data and no configuration.
    *   - `/openapi.json`, `/openapi.yaml`, `/docs` and its assets are open, and that is a decision rather than an
    *     oversight. The document is a static description of *this build's* routes, identical in every deployment and
    *     already readable in the repository; it contains no data, no configuration and no secret. Closing it would cost
    *     the Swagger page outright — the UI fetches `/openapi.json` from the browser with no `Authorization` header, so
    *     an authenticated document renders a blank page — which is to say it would remove the operator's tool during an
    *     incident in exchange for hiding paths an attacker can read on GitHub. What must not leak is the DLQ's contents
    *     and the ability to act on it, and those are what the scopes cover.
    */
  val Access: Map[String, Option[AdminScope]] =
    Map(
      Meters.MetricsPath -> None,
      LivenessPath -> None,
      ReadinessPath -> None,
      OpenApiJsonPath -> None,
      OpenApiYamlPath -> None,
      DocsPath -> None,
      SwaggerAssetsPath -> None,
      // Reads. `/admin/dlq/records` returns event payloads and `/admin/consumer` returns the group's offsets: both are
      // disclosure, neither changes anything.
      DlqPath -> Some(AdminScope.Read),
      DlqRecordsPath -> Some(AdminScope.Read),
      ConsumerPath -> Some(AdminScope.Read),
      // Writes. Every one of these changes the pipeline, and three of them irreversibly: `:replay` appends to the
      // production topic, `:restart` moves committed offsets, `:clearCheckpoints` destroys the externalised ones.
      // `dryRun=true` is not a substitute for a scope — it is the default of a caller who is already authorised.
      DlqReplayPath -> Some(AdminScope.Write),
      ConsumerPausePath -> Some(AdminScope.Write),
      ConsumerResumePath -> Some(AdminScope.Write),
      ConsumerStopPath -> Some(AdminScope.Write),
      ConsumerStartPath -> Some(AdminScope.Write),
      ConsumerRestartPath -> Some(AdminScope.Write),
      ConsumerClearCheckpointsPath -> Some(AdminScope.Write)
    )
