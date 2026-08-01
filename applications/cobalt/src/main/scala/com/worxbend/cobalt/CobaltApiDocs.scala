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

import io.circe.Json
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.openapi.circe.*
import sttp.apispec.openapi.circe.yaml.*
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsOptions
import sttp.tapir.json.circe.*

/** cobalt's admin API, described once and rendered as OpenAPI.
  *
  * ## Why tapir endpoint *values* for a Cask service
  *
  * cobalt's HTTP surface is Cask and stays Cask (ADR §1) — annotations on a routes class, no second web stack, no
  * server interpreter on this classpath. What tapir contributes here is only the *description*: a set of endpoint
  * values that a generator turns into a document with real schemas.
  *
  * The alternative was a hand-written `openapi.yaml`. That is a second statement of the contract, and the two disagree
  * the first time a parameter is added — silently, because nothing compiles YAML. This arrangement has the same hazard
  * in a smaller and *checkable* form: the description could drift from the Cask routes. So `CobaltApiDocsSuite`
  * asserts, in both directions, that every path here is served and every route served is described. That test is the
  * reason this design is acceptable rather than merely convenient.
  *
  * ## Why the admin API is documented at all
  *
  * Two of these routes move a production pipeline: one replays dead letters onto the main topic, the other moves a
  * consumer group's committed offsets. An operator meets them during an incident, from a terminal, having never used
  * them before. A generated document with every parameter's meaning and every failure's cause is the difference between
  * that and reading Scala.
  */
object CobaltApiDocs:

  val Title: String = "cobalt — consumer administration"

  /** The version of the *contract*, not the build. `SERVICE_VERSION` changes on every deploy and would make every
    * generated client look stale after a release that changed nothing a caller can see.
    */
  val ApiVersion: String = "1.0.0"

  /** Where the UI and the document live. `/docs` is the human entry point, matching wolfram. */
  val DocsPath: String = "/docs"
  val JsonPath: String = "/openapi.json"
  val YamlPath: String = "/openapi.yaml"

  private val Description: String =
    """cobalt consumes CloudEvents from Kafka and writes them to PostgreSQL. It has **no business HTTP API** and
      |never will: events arrive over Kafka, and an HTTP write path would be a second, unordered, uncommitted way
      |into the same database (ADR §1). Everything here is operational.
      |
      |### The two routes that change production
      |
      |`POST /admin/dlq:replay` puts dead letters back on the main topic. `POST /admin/consumer:restart` moves the
      |consumer group's committed offsets — with `target=latest` it **skips unconsumed events**. Both **plan by
      |default** and commit only with `dryRun=false`. Run them once to read the plan before you run them for real.
      |
      |### Custom methods use a colon
      |
      |`:pause`, `:restart`, `:replay` — AIP-136's form, the same one wolfram uses. `/admin/consumer/pause` would be
      |indistinguishable from a sub-resource named `pause`; the colon form cannot collide with one.
      |
      |### Offsets live in two places, and the difference is the diagnostic
      |
      |`GET /admin/consumer` reports `committed` (what Kafka has) beside `stored` (what
      |`events.consumer_checkpoint` has, written in the same transaction as the rows). When they disagree, one of the
      |two commits did not happen — and which one tells you whether events will be replayed or were lost.
      |
      |### Everything under `/admin` needs a bearer token
      |
      |Reads need the `admin:read` scope, the mutating operations need `admin:write`, and a token carrying
      |`admin:write` satisfies both. `/metrics` and `/health/*` are open because Prometheus and the container
      |orchestrator cannot hold a token; this document and the Swagger page are open because they describe the build
      |rather than the data.""".stripMargin

  // --- the shapes, described ----------------------------------------------------------------------------------

  private given Schema[PartitionPosition] = Schema
    .derived[PartitionPosition]
    .description(
      "One partition's position. `committed` is Kafka's answer, `stored` is the externalised checkpoint's, " +
        "`endOffset` is the log's end and `lag` is the gap. A null lag means *unknown*, never zero."
    )

  /** The status body, described structurally rather than derived from [[ConsumerStatus]].
    *
    * `ConsumerStatus` has a hand-written encoder — field order is reading order, and `state` renders as its wire name
    * rather than as an enum constant — so a derived schema would describe a different document from the one served.
    * Describing the wire shape directly keeps the document honest about what a caller receives.
    */
  final case class ConsumerStatusDoc(
    state: String,
    consuming: Boolean,
    since: String,
    generation: Int,
    restarts: Int,
    groupId: String,
    topic: String,
    totalLag: Option[Long],
    lastError: Option[String],
    positions: List[PartitionPosition]
  ) derives io.circe.Codec.AsObject

  private given Schema[ConsumerStatusDoc] = Schema
    .derived[ConsumerStatusDoc]
    .description(
      "The consumer's whole observable state. `state` is one of starting, running, paused, stopping, stopped, " +
        "failed — `stopped` is deliberate and `failed` is not, which is why they are different words."
    )

  final case class LifecycleResultDoc(command: String, changed: Boolean, from: String, status: ConsumerStatusDoc)
      derives io.circe.Codec.AsObject

  private given Schema[LifecycleResultDoc] = Schema
    .derived[LifecycleResultDoc]
    .description(
      "`changed` is false when the command was a no-op — pausing an already-paused consumer succeeds and says so, " +
        "which is what an operator who issued it twice needs to know."
    )

  final case class ErrorDoc(error: String) derives io.circe.Codec.AsObject
  private given Schema[ErrorDoc] = Schema.derived[ErrorDoc]

  private val jsonError = jsonBody[ErrorDoc]

  /** The `Authorization: Bearer` header every `/admin` operation requires.
    *
    * **Optional in the codec, and that is deliberate** — the same choice wolfram's endpoints make. A mandatory bearer
    * input rejects a missing header inside the codec and produces a bare 400 with no body; an `Option` moves the
    * decision to [[AdminAuth]], where "no credential" becomes a 401 carrying `WWW-Authenticate` like every other
    * refusal on this surface.
    */
  val bearer: EndpointInput.Auth[Option[String], EndpointInput.AuthType.Http] =
    auth
      .bearer[Option[String]]()
      .description(
        s"A JWT bearer token. Reads need the `${JwtVerifier.ReadScope}` scope and the mutating operations need " +
          s"`${JwtVerifier.WriteScope}`; a token carrying the write scope satisfies both. Verification covers the " +
          "signature, a **mandatory** `exp`, `nbf`, and — when the deployment configures them — `iss` and `aud`. " +
          "The algorithm is pinned by configuration and the token's own `alg` header is checked against it, never " +
          "trusted."
      )

  /** The base every operation shares. `/admin` is a prefix and not a tag: an ingress can expose `/metrics` and
    * `/health` to the platform while keeping everything under `/admin` on the inside, without enumerating paths.
    *
    * The bearer input lives here rather than on each operation, which is what makes "every admin route is
    * authenticated" a property of one value instead of ten call sites that each have to remember.
    */
  private val admin: Endpoint[Option[String], Unit, ErrorDoc, Unit, Any] =
    endpoint.securityIn(bearer).in("admin").errorOut(jsonError)

  // --- the consumer lifecycle ---------------------------------------------------------------------------------

  private val consumerTag = "consumer"

  val consumerStatus: Endpoint[Option[String], Unit, ErrorDoc, ConsumerStatusDoc, Any] =
    admin.get
      .in("consumer")
      .out(jsonBody[ConsumerStatusDoc])
      .name("getConsumer")
      .summary("The consumer's state and offsets")
      .description(
        """Never fails on a broker or database timeout. The state machine's own answer is always available and is the
          |more important half, so an unreachable dependency yields null positions and a state rather than a 500 —
          |this is the endpoint somebody hits *because* something is wrong.""".stripMargin
      )
      .tag(consumerTag)

  private def lifecycle(verb: String, summary: String, description: String) =
    admin.post
      .in(s"consumer:$verb")
      .out(jsonBody[LifecycleResultDoc])
      .name(s"${verb}Consumer")
      .summary(summary)
      .description(description)
      .tag(consumerTag)

  val consumerPause = lifecycle(
    "pause",
    "Pause consumption",
    """Drains the stream, commits what is in flight, and leaves the consumer group.
      |
      |**This tears the stream down; it does not idle it.** Holding the stream open and stopping demand would keep the
      |group's session alive and its partitions assigned, so no other replica could take over. Draining lets the
      |partitions move immediately. The cost is that pause/resume is a rebalance.
      |
      |`paused` rather than `stopped` is a statement of intent: alert on one, stay quiet for the other.""".stripMargin
  )

  val consumerResume = lifecycle(
    "resume",
    "Resume consumption",
    "Materialises a fresh stream, which rejoins the group and continues from the committed offsets. A drained " +
      "stream is not restartable, so this is a new one — hence a new `generation` in the status."
  )

  val consumerStop = lifecycle(
    "stop",
    "Stop consumption",
    "The same operation as `:pause`, reported as an outage rather than as intent."
  )

  val consumerStart = lifecycle("start", "Start consumption", "Idempotent; a running consumer is left alone.")

  val consumerRestart: Endpoint[Option[String], (String, String, Boolean), ErrorDoc, Json, Any] =
    admin.post
      .in("consumer:restart")
      .in(
        query[String]("target")
          .default(SeekTarget.Committed.name)
          .description(
            "Where to resume from. `committed` leaves the offsets alone. `stored` uses the externalised " +
              "checkpoints, which outlive Kafka's offsets.retention.minutes. `earliest` replays the retained log — " +
              "safe, because the insert deduplicates, and expensive. **`latest` skips unconsumed events.** " +
              "`explicit` uses the coordinates in `offsets`."
          )
      )
      .in(
        query[String]("offsets")
          .default("")
          .description(
            "Comma-separated `topic/partition/offset`, for `target=explicit`. Supplying these with any other " +
              "target is refused rather than ignored — a successful response that silently did something else is " +
              "worse than an error."
          )
      )
      .in(
        query[Boolean]("dryRun")
          .default(true)
          .description("Defaults to **true**. The plan is returned and nothing is stopped or moved.")
      )
      .out(jsonBody[Json].description("The plan, or what was committed, plus the resulting status."))
      .errorOut(statusCode(StatusCode.Conflict).description("The offsets could not be moved; the consumer is stopped."))
      .name("restartConsumer")
      .summary("Restart the consumer, optionally at new offsets")
      .description(
        """Stops the consumer, moves the group's committed offsets, and starts it again.
          |
          |**The stop is not optional and the order is not negotiable.** Kafka refuses `alterConsumerGroupOffsets`
          |while the group has live members, and it is right to: an offset moved under a running consumer is
          |overwritten by that consumer's next commit, so the call would appear to succeed and change nothing.
          |
          |On a 409 the consumer is left **stopped**, not started against offsets nobody chose.""".stripMargin
      )
      .tag(consumerTag)

  val consumerClearCheckpoints: Endpoint[Option[String], Unit, ErrorDoc, Json, Any] =
    admin.post
      .in("consumer:clearCheckpoints")
      .out(jsonBody[Json])
      .name("clearConsumerCheckpoints")
      .summary("Forget the externalised offsets")
      .description(
        "**Does not touch Kafka's own committed offsets.** This is the \"the stored position is wrong, use the " +
          "broker's answer instead\" escape hatch; conflating it with a Kafka offset reset would make one word mean " +
          "two irreversible things."
      )
      .tag(consumerTag)

  // --- the dead-letter queue ----------------------------------------------------------------------------------

  private val dlqTag = "dead-letters"

  val dlqSummary: Endpoint[Option[String], Unit, ErrorDoc, Json, Any] =
    admin.get
      .in("dlq")
      .out(jsonBody[Json])
      .name("getDeadLetterQueue")
      .summary("DLQ depth and the replay limits in force")
      .description(
        "`outstanding` is an **upper bound** computed from partition offsets, not an exact count: the DLQ topic is " +
          "compacted by key, so some of those offsets may be superseded. Treat it as \"at most this many\"."
      )
      .tag(dlqTag)

  val dlqRecords: Endpoint[Option[String], (Int, String), ErrorDoc, Json, Any] =
    admin.get
      .in("dlq" / "records")
      .in(query[Int](
        "limit"
      ).default(0).description("Bounded by `REPLAY_MAX_RECORDS`; an over-limit request is refused, never clamped."))
      .in(query[String]("reason").default("").description(
        "Filter by rejection reason — the same closed vocabulary as `consume_records_poison_total{reason}`."
      ))
      .out(jsonBody[Json])
      .name("listDeadLetters")
      .summary("A bounded, newest-first page of dead letters")
      .description(
        "Bounded is not a detail: an unbounded listing endpoint on a topic is a way to OOM the service that is " +
          "supposed to be telling you it is unhealthy."
      )
      .tag(dlqTag)

  val dlqReplay: Endpoint[Option[String], (Int, String, String, Boolean), ErrorDoc, Json, Any] =
    admin.post
      .in("dlq:replay")
      .in(query[Int]("limit").default(0).description("How many to replay. Refused above `REPLAY_MAX_RECORDS`."))
      .in(query[String]("reason").default("").description("Replay only dead letters with this reason."))
      .in(query[String](
        "refs"
      ).default("").description("Comma-separated `topic/partition/offset` of specific records."))
      .in(query[Boolean](
        "dryRun"
      ).default(true).description("Defaults to **true**. Returns the plan and publishes nothing."))
      .out(jsonBody[Json])
      .name("replayDeadLetters")
      .summary("Replay dead letters onto the main topic")
      .description(
        """Re-publishes with the original CloudEvents bytes and headers, so the idempotent insert makes a re-ingested
          |event that did land a no-op — which only holds because the event id survives replay unchanged.
          |
          |**Bounded by an attempt ceiling.** A record that fails again returns to the DLQ under new origin
          |coordinates, so the DLQ's own compaction key does nothing to stop the cycle; the replay counter in the
          |record's headers does. A defect that survived three deploys will not be fixed by a fourth replay.
          |
          |Watch `dlq_replay_records_total{outcome}` beside `consume_records_poison_total`: records coming straight
          |back is a poison loop, and the fix was not the fix.""".stripMargin
      )
      .tag(dlqTag)

  // --- the platform routes ------------------------------------------------------------------------------------

  private val platformTag = "platform"

  val metrics: PublicEndpoint[Unit, ErrorDoc, String, Any] =
    endpoint.get
      .errorOut(jsonError)
      .in("metrics")
      .out(stringBody.description("Prometheus text exposition, `text/plain; version=0.0.4`."))
      .name("getMetrics")
      .summary("Prometheus scrape endpoint")
      .tag(platformTag)

  val live: PublicEndpoint[Unit, ErrorDoc, Json, Any] =
    endpoint.get
      .errorOut(jsonError)
      .in("health" / "live")
      .out(jsonBody[Json])
      .name("getLiveness")
      .summary("Liveness")
      .description(
        "**Consults no dependency, deliberately.** A liveness probe that checked Kafka would fail on every replica " +
          "the moment a broker went down and the orchestrator would restart them all — turning a recoverable " +
          "dependency outage into a crash loop that outlives it."
      )
      .tag(platformTag)

  val ready: PublicEndpoint[Unit, ErrorDoc, Json, Any] =
    endpoint.get
      .errorOut(jsonError)
      .errorOut(statusCode(StatusCode.ServiceUnavailable))
      .in("health" / "ready")
      .out(jsonBody[Json])
      .name("getReadiness")
      .summary("Readiness")
      .description(
        "Both Kafka and PostgreSQL count: without one there is nothing to consume, without the other nowhere to put it."
      )
      .tag(platformTag)

  /** Every endpoint, in document order. The drift test walks this list against the Cask routes. */
  val all: List[AnyEndpoint] =
    List(
      consumerStatus,
      consumerPause,
      consumerResume,
      consumerStop,
      consumerStart,
      consumerRestart,
      consumerClearCheckpoints,
      dlqSummary,
      dlqRecords,
      dlqReplay,
      metrics,
      live,
      ready
    )

  private val options: OpenAPIDocsOptions =
    OpenAPIDocsOptions.default.copy(operationIdGenerator =
      (_, pathComponents, method) => pathComponents.lastOption.getOrElse(method.method.toLowerCase)
    )

  def openApi: OpenAPI =
    val generated = OpenAPIDocsInterpreter(options).toOpenAPI(all, Title, ApiVersion)
    generated.copy(info = generated.info.copy(description = Some(Description)))

  def json: Json = io.circe.syntax.EncoderOps(openApi).asJson.deepDropNullValues

  def yaml: String = openApi.toYaml

  /** The paths this document describes, in the spelling Cask routes use.
    *
    * Tapir renders a path with a leading slash and no trailing one, which is already Cask's spelling — so the drift
    * test can compare the two sets directly instead of normalising, and a normalisation step is one more place the
    * comparison could be made to pass by accident.
    */
  def documentedPaths: Set[String] = all.map(_.showPathTemplate(showQueryParam = None)).toSet
