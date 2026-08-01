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

package com.worxbend.wolfram

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import sttp.model.Header
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.PartialServerEndpoint
import sttp.tapir.server.ServerEndpoint

/** The HTTP contract, as values.
  *
  * Endpoint descriptions are separated from server logic so that the same values drive four things that must not be
  * allowed to disagree: the Vert.x routes, the OpenAPI document, the Swagger UI, and the stub-server tests. Writing the
  * contract once is the entire argument for Tapir in this service.
  *
  * ## Resource-oriented design
  *
  * The surface follows Google's API Improvement Proposals, because they answer the questions a hand-rolled HTTP API
  * answers differently every time — and answer them the way most tooling already expects.
  *
  *   - **AIP-133/135 — standard methods on a collection.** The resource is `events`; creating one is `POST /v1/events`,
  *     and the response is the created resource, not a bespoke receipt.
  *   - **AIP-122 — resource names.** Every response carries `name: "events/{event}"`. A client stores that.
  *   - **AIP-136 — custom methods** use `POST /v1/{collection}:verb`, with a **colon**, not a sub-path. The colon is
  *     the whole point: `POST /v1/events/batch` is indistinguishable from creating a resource named `batch`, and the
  *     day a `GET /v1/events/{event}` is added those two routes collide. `:batchCreate` can never collide with a
  *     resource id, because `:` is reserved from the identifier segment.
  *   - **AIP-185 — the `/v1` prefix** is on the path and not in a header, so a proxy can route on it and a browser can
  *     be pointed at it.
  *   - **AIP-193 — errors** are one `{"error": {code, message, status, details}}` envelope for every failure.
  *
  * **Both CloudEvents HTTP content modes arrive on one endpoint, and that is the spec's shape, not a shortcut.** The
  * CloudEvents HTTP binding defines binary and structured mode as two encodings of a request to the *same* resource; a
  * client picks one by setting headers, and a server must accept either. Two Tapir endpoints on one path would also be
  * fragile: routing between them would hinge on how the decode-failure handler classifies a missing header, which is
  * behaviour of the interpreter rather than of the contract. So the request body is taken raw and the mode is decided
  * by [[HttpBinding.modeOf]] — the same precedence rule `modules/eventing` uses on the Kafka side.
  *
  * **The body is `byteArrayBody`, not `stringBody` or `jsonBody`.** Binary mode's payload is arbitrary bytes and must
  * reach Kafka byte-identical (ADR §4.3); decoding it as a `String` would corrupt any non-UTF-8 payload, and decoding
  * it as JSON would reject the very events binary mode exists to carry. The size ceiling is enforced on those bytes,
  * before anything is decoded.
  *
  * The consequence is that the generated document declares one request media type, `application/octet-stream`, while
  * the service accepts any `Content-Type` — the header is an input to [[HttpBinding.modeOf]], not a routing key. A
  * `oneOfBody` per media type would document the three a client actually sends, but it would also make Tapir dispatch
  * on `Content-Type`, which is the precedence rule this endpoint exists to avoid.
  */
object Endpoints:

  /** Explicit schemas for every wire type, and explicit descriptions on the fields that need one.
    *
    * **Derived by name rather than by `sttp.tapir.generic.auto`.** Auto-derivation is a wildcard `given` that searches
    * for a schema for *any* type it meets, and when it fails it fails deep inside the search — the error for
    * `BatchCreateResponse` was a page of `Mirror.Sum` internals about `List`, naming neither the field nor the type
    * actually at fault. Naming each derivation makes a missing schema a one-line error at the type that is missing it.
    * It also makes the document's field descriptions a thing that exists in source rather than a thing that would be
    * nice to add later.
    */
  given Schema[Destination] = Schema
    .derived[Destination]
    .description("Where the event was written on the log. Diagnostic: enough to find the record again with kcat.")

  given Schema[Event] = Schema
    .derived[Event]
    .description("A published event. `name` is the resource name (AIP-122) and is the field to store.")

  given Schema[ErrorInfo] = Schema
    .derived[ErrorInfo]
    .description(
      "A machine-readable cause. `reason` is a closed vocabulary shared with the " +
        "`ingest_events_rejected_total{reason}` metric; `metadata` carries the numbers so clients need not parse prose."
    )

  given Schema[ErrorBody] = Schema
    .derived[ErrorBody]
    .description("The AIP-193 error body. Branch on `status`, not on the HTTP code — 400 covers three causes.")

  given Schema[BatchEntry] = Schema
    .derived[BatchEntry]
    .description("One element's outcome, correlated by `index`. Exactly one of `event` and `error` is present.")

  given Schema[BatchCreateResponse] = Schema.derived[BatchCreateResponse]
  given Schema[ValidateResponse] = Schema.derived[ValidateResponse]
  given Schema[InvalidArgument] = Schema.derived[InvalidArgument]
  given Schema[OutOfRange] = Schema.derived[OutOfRange]
  given Schema[PayloadTooLarge] = Schema.derived[PayloadTooLarge]
  given Schema[Unauthorized] = Schema.derived[Unauthorized]
  given Schema[Forbidden] = Schema.derived[Forbidden]
  given Schema[Unavailable] = Schema.derived[Unavailable]

  /** The tag every operation carries, so Swagger UI groups them under one heading. */
  val Tag: String = "events"

  /** The failure outputs, shared by every endpoint.
    *
    * `oneOf` and not a single body with a status field: Tapir matches variants on the runtime class, so the type of the
    * error value chooses the status code and the two cannot drift. Every variant renders the same AIP-193 envelope, so
    * a client parses one shape regardless of which arm it hits.
    *
    * `INVALID_ARGUMENT` and `OUT_OF_RANGE` share a status code and are separate variants anyway, because the canonical
    * `status` inside the body is what a generated client switches on and 400 is too coarse to carry it.
    */
  val failures: EndpointOutput.OneOf[ApiError, ApiError] =
    oneOf[ApiError](
      oneOfVariant(
        StatusCode.BadRequest,
        jsonBody[InvalidArgument].description(
          "`status: INVALID_ARGUMENT`. The request was not an acceptable CloudEvent — a missing or ill-typed " +
            "context attribute, or a body that is not a CloudEvent at all."
        )
      ),
      oneOfVariant(
        StatusCode.BadRequest,
        jsonBody[OutOfRange].description(
          "`status: OUT_OF_RANGE`. The event is well-formed, but its `time` falls outside the plausibility " +
            "window — too far in the future, or older than the retention horizon."
        )
      ),
      oneOfVariant(
        StatusCode.PayloadTooLarge,
        jsonBody[PayloadTooLarge].description(
          "`status: INVALID_ARGUMENT`. The body exceeded `wolfram.ingest.max-event-bytes`, or the batch exceeded " +
            "`max-batch-events`. `details[0].metadata` carries `limit`, `actual` and `unit`."
        )
      ),
      oneOfVariant(
        StatusCode.Unauthorized,
        jsonBody[Unauthorized].description(
          "`status: UNAUTHENTICATED`. No bearer token, or one that failed signature, `exp`, `nbf`, `iss` or `aud` " +
            "verification. Obtain a new token; do not retry this one."
        )
      ),
      oneOfVariant(
        StatusCode.Forbidden,
        jsonBody[Forbidden].description(
          s"`status: PERMISSION_DENIED`. The token verified but does not carry the " +
            s"`${JwtVerifier.PublishScope}` scope. Retrying will not help."
        )
      ),
      oneOfVariant(
        StatusCode.ServiceUnavailable,
        jsonBody[Unavailable].description(
          "`status: UNAVAILABLE`. The event was valid but could not be made durable: the broker did not " +
            "acknowledge it, or ingestion shed load rather than queue in front of it. **Retryable** — nothing " +
            "was published."
        )
      )
    )

  /** The security input every operation shares: an optional `Authorization: Bearer` header.
    *
    * **Optional, and that is deliberate.** Tapir's `auth.bearer[String]()` rejects a missing header inside the codec,
    * which produces a bare 400 with no body — not the AIP-193 envelope this API promises for every other failure. An
    * `Option` moves the decision into [[JwtVerifier]], where "no credential" becomes a documented 401 like every other
    * refusal.
    */
  val bearer: EndpointInput.Auth[Option[String], EndpointInput.AuthType.Http] =
    auth
      .bearer[Option[String]]()
      .description(
        s"A JWT bearer token carrying the `${JwtVerifier.PublishScope}` scope. Verification covers the signature, " +
          "`exp` and `nbf`, and — when the deployment configures them — `iss` and `aud`. The signing algorithm is " +
          "pinned by configuration and the token's own `alg` header is checked against it, never trusted."
      )

  /** The shape every operation starts from: bearer security, the AIP-193 error envelope, and the `/v1` prefix.
    *
    * Building the common part once is not only brevity — it is what makes "every endpoint is authenticated" a property
    * of the base rather than of three call sites that must each remember.
    */
  private val base: Endpoint[Option[String], Unit, ApiError, Unit, Any] =
    endpoint
      .securityIn(bearer)
      .in(ApiModel.Version)
      .errorOut(failures)
      .tag(Tag)

  /** `POST /v1/events` — AIP-133 Create. */
  val createEvent: Endpoint[Option[String], (List[Header], Array[Byte]), ApiError, Event, Any] =
    base.post
      .in(ApiModel.Collection)
      .in(headers)
      .in(byteArrayBody)
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Event].description("The created event, including where it landed on the log."))
      .name("createEvent")
      .summary("Create an event")
      .description(
        """Publishes a single CloudEvents 1.0 event in either HTTP content mode.
          |
          |**Binary mode** — context attributes travel as `ce-*` headers (`ce-specversion`, `ce-id`, `ce-source`,
          |`ce-type`, and optionally `ce-subject`, `ce-time`, `ce-dataschema` plus any extension attributes), the
          |body is the event's `data` and is forwarded to Kafka byte-for-byte. `Content-Type` describes the *payload*,
          |not the event. This is the mode the topic itself uses, so it is the cheapest path.
          |
          |**Structured mode** — `Content-Type: application/cloudevents+json` and the whole event, attributes and all,
          |in the body as one JSON object.
          |
          |Mode is chosen by the presence of `ce-specversion`, never by `Content-Type` alone: a binary-mode payload may
          |itself be a CloudEvents document, and deciding on the media type would misread those requests and discard
          |every attribute in the headers.
          |
          |`time` is **required** by this API even though the specification makes it optional: it becomes the
          |partitioning key of the stored event, it must fall within the configured plausibility window, and inventing
          |one would silently misfile the event.
          |
          |**200, not 201.** AIP-133 asks a Create to return the resource, and 201 would oblige a `Location` header
          |pointing at a `GET /v1/events/{event}` this service cannot serve — it owns no storage. Promising a URL
          |that answers 404 is worse than not promising one.""".stripMargin
      )

  /** `POST /v1/events:batchCreate` — AIP-233 Batch Create, with a documented deviation on atomicity. */
  val batchCreateEvents
    : Endpoint[Option[String], (List[Header], Array[Byte]), ApiError, (StatusCode, BatchCreateResponse), Any] =
    base.post
      .in(s"${ApiModel.Collection}:batchCreate")
      .in(headers)
      .in(byteArrayBody)
      .out(
        statusCode
          .description(StatusCode.Ok, "Every event in the batch was published.")
          .description(StatusCode.MultiStatus, "Some events were published and some were refused; see `entries`.")
      )
      .out(
        jsonBody[BatchCreateResponse]
          .description("The created events, plus one entry per element of the request, in request order.")
      )
      .name("batchCreateEvents")
      .summary("Create a batch of events")
      .description(
        """Accepts an `application/cloudevents-batch+json` document: a JSON array of structured-mode CloudEvents.
          |
          |**This batch is not atomic, which is a deviation from AIP-233 and is deliberate.** AIP-233 specifies that a
          |batch either wholly succeeds or wholly fails. That guarantee is not available here: the events go to Kafka
          |one at a time and an acknowledged event cannot be unpublished, so "roll back the successes" is not an
          |operation this service can perform. Pretending otherwise — answering 400 for a partial failure — would be
          |actively harmful, because a client retrying the whole document would duplicate every event that had
          |already landed.
          |
          |So a partial failure is **207 Multi-Status**, and `entries` reports each element's outcome by index. Retry
          |only the entries that carry an `error`.
          |
          |Elements are published sequentially, preserving per-key ordering: two events for the same device in one
          |document reach the same partition in the order they were sent.""".stripMargin
      )

  /** `POST /v1/events:validate` — AIP-136 custom method.
    *
    * The textbook case for a custom method: it acts on the collection, it is not one of the five standard methods, and
    * it has no side effect. A producer integrating against this API can check a payload against the *running* build's
    * rules — including the time window, which no schema can express — without putting anything on the log.
    */
  val validateEvent: Endpoint[Option[String], (List[Header], Array[Byte]), ApiError, ValidateResponse, Any] =
    base.post
      .in(s"${ApiModel.Collection}:validate")
      .in(headers)
      .in(byteArrayBody)
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[ValidateResponse].description("Whether the event would be accepted, and why not if it would not."))
      .name("validateEvent")
      .summary("Validate an event without publishing it")
      .description(
        """Runs every check `createEvent` runs — size, content mode, CloudEvents attributes, and the `time`
          |plausibility window — and publishes nothing.
          |
          |**Always 200 when the request itself is usable.** A rejected event is a successful validation with
          |`valid: false`, not a failed request: the question was "would you accept this", and "no, because `time` is
          |200 days old" is an answer. The 4xx arm is reserved for the request being unusable — an unverifiable
          |token, or a body over the size ceiling, which cannot be validated because it is refused before it is read.
          |
          |Useful in a producer's own test suite: the time window is configuration, so a payload that validated
          |during development can start failing in production, and this is the endpoint that says so first.""".stripMargin
      )

  /** Everything this API serves, for documentation and for routing. */
  val all: List[AnyEndpoint] = List(createEvent, batchCreateEvents, validateEvent)

/** The endpoints bound to [[IngestionService]] and to the token verifier.
  *
  * A class rather than an object because both are constructor dependencies: wolfram has no global state, and a
  * singleton holding a Kafka producer would make the whole HTTP layer untestable and un-restartable.
  */
final class IngestApi(service: IngestionService, verifier: JwtVerifier)(using ExecutionContext):

  /** Authentication, applied once.
    *
    * Every route below is derived from this value, so there is no route that *could* be added without it — a stronger
    * guarantee than a filter somebody has to remember to apply, and a much stronger one than a review checklist.
    */
  private def secured[I, O](
    description: Endpoint[Option[String], I, ApiError, O, Any]
  ): PartialServerEndpoint[Option[String], Principal, I, ApiError, O, Any, Future] =
    description.serverSecurityLogicPure(token =>
      verifier.verify(token, Some(JwtVerifier.PublishScope)).left.map(ApiModel.authFailure)
    )

  /** Server endpoints, ready for any Tapir interpreter. */
  val routes: List[ServerEndpoint[Any, Future]] =
    List(
      secured(Endpoints.createEvent).serverLogic(_ => create),
      secured(Endpoints.batchCreateEvents).serverLogic(_ => batchCreate),
      secured(Endpoints.validateEvent).serverLogic(_ => validate)
    )

  private def create(input: (List[Header], Array[Byte])): Future[Either[ApiError, Event]] =
    val (requestHeaders, body) = input
    service
      .ingest(IngestApi.headerMap(requestHeaders), body)
      .map(result => result.left.map(ApiModel.failure).map(ApiModel.event))

  private def batchCreate(
    input: (List[Header], Array[Byte])
  ): Future[Either[ApiError, (StatusCode, BatchCreateResponse)]] =
    val (requestHeaders, body) = input
    val headers = IngestApi.headerMap(requestHeaders)
    if !HttpBinding.isBatch(headers) then
      Future.successful(
        Left(
          ApiModel.failure(
            Rejection.Malformed(
              s"a batch request must be sent as '${HttpBinding.ContentTypeHeader}: ${HttpBinding.BatchMediaType}'"
            )
          )
        )
      )
    else service.ingestBatch(body).map(result => result.left.map(ApiModel.failure).map(ApiModel.batch))

  /** Validation reuses the service's own pure `validate`, so this endpoint cannot drift from what `createEvent` would
    * decide — the two call the same function, and a check added to one is a check added to both.
    *
    * A size rejection is the one case that stays a 4xx: an oversize body is refused before it is read, so there is
    * nothing to return a verdict about.
    */
  private def validate(input: (List[Header], Array[Byte])): Future[Either[ApiError, ValidateResponse]] =
    val (requestHeaders, body) = input
    val verdict = service.validate(IngestApi.headerMap(requestHeaders), body) match
      case Right(accepted) => Right(ValidateResponse(valid = true, Some(ApiModel.unpublished(accepted)), None))
      case Left(rejection: Rejection.TooLarge) => Left(ApiModel.failure(rejection))
      case Left(rejection)                     =>
        Right(ValidateResponse(valid = false, None, Some(ApiModel.failure(rejection).error)))
    Future.successful(verdict)

object IngestApi:

  /** Flattens Tapir's header list into the case-insensitive map the binding works with. */
  def headerMap(headers: List[Header]): Map[String, String] =
    HttpBinding.normalise(headers.map(header => header.name -> header.value))
