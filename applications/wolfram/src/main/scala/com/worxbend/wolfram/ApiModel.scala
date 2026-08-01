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

import com.worxbend.kernel.Rfc3339
import io.circe.Codec
import sttp.model.StatusCode

/** Where an accepted event landed on the log.
  *
  * A nested message rather than three fields flattened onto [[Event]], per AIP-144: these describe the *storage* of the
  * event, not the event, and a client that only wants the CloudEvent should not have to know which of seven sibling
  * fields belong to it. It is also the part a future transport would replace wholesale.
  */
final case class Destination(topic: String, partition: Int, offset: Long) derives Codec.AsObject

/** One published event, as this API represents it.
  *
  * **`name` is the resource name and it comes first, per AIP-122.** `events/{event}`, where `{event}` is the
  * percent-encoded CloudEvents `(source, id)` pair — that pair and not `id` alone is what the specification makes
  * unique.
  *
  * The identity is **echoed, never assigned**. CloudEvents makes `(source, id)` the producer's contract and the whole
  * downstream deduplication story — cobalt's `ON CONFLICT DO NOTHING` — depends on this service not touching it.
  */
final case class Event(
  name: String,
  id: String,
  source: String,
  eventType: String,
  time: String,
  partitionKey: String,
  destination: Destination
) derives Codec.AsObject

/** One machine-readable cause, in the shape of `google.rpc.ErrorInfo` (AIP-193).
  *
  * **`reason` is the closed-set value a client may branch on**, and it is deliberately the same vocabulary as the
  * `ingest.events.rejected{reason}` metric tag: an operator reading a dashboard and a client reading an error are then
  * looking at the same word for the same condition. `metadata` carries the numbers — the limit that was exceeded, the
  * window that was missed — because putting them only in the prose message forces clients to parse English.
  */
final case class ErrorInfo(reason: String, domain: String, metadata: Map[String, String]) derives Codec.AsObject

/** The body of an error response (AIP-193).
  *
  * @param code
  *   the HTTP status, repeated inside the body. Redundant over the wire and worth it: it survives proxies that rewrite
  *   status codes and logs that record only the payload.
  * @param status
  *   the canonical `google.rpc.Code` name — `INVALID_ARGUMENT`, `OUT_OF_RANGE`, `UNAUTHENTICATED`, `PERMISSION_DENIED`,
  *   `UNAVAILABLE`. This is the field a generated client switches on; the HTTP status is coarser (three distinct causes
  *   all arrive as 400) and the message is prose.
  */
final case class ErrorBody(code: Int, message: String, status: String, details: List[ErrorInfo])
    derives Codec.AsObject

/** Why a request was refused, in the shape a client sees.
  *
  * **One wire shape, several Scala types, and that is the point.** Every case renders to the identical `{"error": {…}}`
  * envelope AIP-193 specifies, so a client has one thing to parse. But Tapir's `oneOf` dispatches on the runtime class,
  * so the *type* is what selects the status code — which means a 401 body cannot be served with a 400 and no test has
  * to check that it wasn't. The alternative, a single type with a status field, makes that agreement a convention
  * somebody maintains by hand.
  */
sealed trait ApiError:
  def error: ErrorBody

/** 400. The request is not a CloudEvent this service can accept. */
final case class InvalidArgument(error: ErrorBody) extends ApiError derives Codec.AsObject

/** 400, distinct in the canonical vocabulary: well-formed, but `time` is outside the plausibility window. */
final case class OutOfRange(error: ErrorBody) extends ApiError derives Codec.AsObject

/** 413. The body, or the number of events in a batch, exceeded a configured ceiling. */
final case class PayloadTooLarge(error: ErrorBody) extends ApiError derives Codec.AsObject

/** 401. No credential, or one that did not verify. */
final case class Unauthorized(error: ErrorBody) extends ApiError derives Codec.AsObject

/** 403. A verified credential without the scope this operation needs. */
final case class Forbidden(error: ErrorBody) extends ApiError derives Codec.AsObject

/** 503. The event was fine; the broker was not. Retryable, and the only failure here that is. */
final case class Unavailable(error: ErrorBody) extends ApiError derives Codec.AsObject

/** One element's outcome inside a batch, correlated to the request by `index`.
  *
  * By index and not by `name` because a malformed element has no name — that is what makes it malformed — and because a
  * batch may legitimately carry two events with the same `id` from two different sources.
  */
final case class BatchEntry(index: Int, event: Option[Event], error: Option[ErrorBody]) derives Codec.AsObject

/** The response to `events:batchCreate`.
  *
  * `events` holds the successes in request order, which is the AIP-233 response shape; `entries` is this API's addition
  * for its non-atomic batch, and the two counts let a client branch without walking the list.
  */
final case class BatchCreateResponse(events: List[Event], entries: List[BatchEntry], created: Int, failed: Int)
    derives Codec.AsObject

/** The response to `events:validate` — a custom method in the sense of AIP-136. */
final case class ValidateResponse(valid: Boolean, event: Option[Event], error: Option[ErrorBody])
    derives Codec.AsObject

/** Translation between the service's domain values and the HTTP surface.
  *
  * It lives in one object so the mapping from a rejection to a status code and a canonical `status` exists exactly
  * once. The alternative — those fields on [[Rejection]] itself — would put `sttp.model` and Google's RPC vocabulary in
  * the ingestion layer and make the service untestable without a web stack, for no gain.
  */
object ApiModel:

  /** The `domain` on every [[ErrorInfo]]. AIP-193 wants a value identifying the service that produced the error, so a
    * client aggregating errors from several services can tell one `INVALID_ARGUMENT` from another.
    */
  val Domain: String = "wolfram.worxbend.com"

  /** The collection this API serves, and the API version. One constant each, used by the paths, the resource names and
    * the documentation, so `/v1` cannot appear in five places and be bumped in four.
    */
  val Collection: String = "events"
  val Version: String = "v1"

  /** The resource name for an event, per AIP-122: `events/{event}`.
    *
    * The id segment is `source` and `id` percent-encoded and joined, because CloudEvents guarantees uniqueness only for
    * the pair. Encoding rather than concatenating raw matters: a `source` is a URI-reference and routinely contains
    * `/`, which would otherwise fabricate extra path segments and make the name unparseable.
    */
  def resourceName(source: String, id: String): String =
    def encode(value: String): String =
      java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8)
    s"$Collection/${encode(source)}:${encode(id)}"

  /** Renders one published event.
    *
    * `time` goes back through `Rfc3339` rather than `OffsetDateTime#toString` because the JDK formatter omits the
    * seconds field when it is zero, which is not RFC 3339 — the same defect `modules/eventing` documents in the
    * CloudEvents SDK's Kafka serializer.
    */
  def event(receipt: Receipt): Event =
    val envelope = receipt.accepted.envelope
    Event(
      name = resourceName(envelope.source, envelope.id),
      id = envelope.id,
      source = envelope.source,
      eventType = envelope.eventType,
      time = Rfc3339.render(receipt.accepted.occurredAt),
      partitionKey = receipt.accepted.partitionKey,
      destination = Destination(receipt.ack.topic, receipt.ack.partition, receipt.ack.offset)
    )

  /** The event a validation would have produced. No destination, because nothing was published.
    *
    * The empty destination is deliberate rather than an `Option`: `events:validate` answers "would this be accepted",
    * and a client diffing a validate response against a later create response should see one field's contents change,
    * not the whole message change shape.
    */
  def unpublished(accepted: Accepted): Event =
    Event(
      name = resourceName(accepted.envelope.source, accepted.envelope.id),
      id = accepted.envelope.id,
      source = accepted.envelope.source,
      eventType = accepted.envelope.eventType,
      time = Rfc3339.render(accepted.occurredAt),
      partitionKey = accepted.partitionKey,
      destination = Destination("", -1, -1L)
    )

  /** The one place a rejection becomes an HTTP error. Exhaustive over the enum, so a new rejection kind is a compile
    * error here rather than a 500 in production.
    */
  def failure(rejection: Rejection): ApiError = rejection match
    case Rejection.Malformed(cause) =>
      InvalidArgument(body(400, cause, "INVALID_ARGUMENT", rejection.reason))
    case Rejection.InvalidAttributes(cause) =>
      InvalidArgument(body(400, cause, "INVALID_ARGUMENT", rejection.reason))
    case Rejection.ImplausibleTime(cause) =>
      // OUT_OF_RANGE and not INVALID_ARGUMENT: the value is the right type and the wrong magnitude, and AIP-193 draws
      // that line so a client can tell "your clock is wrong" from "your JSON is wrong" without reading the sentence.
      OutOfRange(body(400, cause, "OUT_OF_RANGE", rejection.reason))
    case Rejection.TooLarge(limit, actual, unit) =>
      PayloadTooLarge(
        body(
          413,
          rejection.detail,
          "INVALID_ARGUMENT",
          rejection.reason,
          Map("limit" -> limit.toString, "actual" -> actual.toString, "unit" -> unit)
        )
      )
    case Rejection.BrokerUnavailable(cause) =>
      Unavailable(body(503, cause, "UNAVAILABLE", rejection.reason))

  /** The one place an authentication decision becomes an HTTP error. */
  def authFailure(problem: AuthProblem): ApiError = problem match
    case AuthProblem.Unauthenticated(cause) =>
      Unauthorized(body(401, cause, "UNAUTHENTICATED", "credential-invalid"))
    case AuthProblem.Forbidden(cause) =>
      Forbidden(body(403, cause, "PERMISSION_DENIED", "scope-missing"))

  private def body(
    code: Int,
    message: String,
    status: String,
    reason: String,
    metadata: Map[String, String] = Map.empty
  ): ErrorBody =
    ErrorBody(code, message, status, List(ErrorInfo(reason, Domain, metadata)))

  /** The status a failure is served with.
    *
    * Stated here as well as on the endpoint's `oneOf` so the two can be asserted equal in a test — the endpoint
    * description is the authority the server obeys, and this is the authority a reader can check it against.
    */
  def status(failure: ApiError): StatusCode = failure match
    case _: InvalidArgument => StatusCode.BadRequest
    case _: OutOfRange      => StatusCode.BadRequest
    case _: PayloadTooLarge => StatusCode.PayloadTooLarge
    case _: Unauthorized    => StatusCode.Unauthorized
    case _: Forbidden       => StatusCode.Forbidden
    case _: Unavailable     => StatusCode.ServiceUnavailable

  /** Renders one batch element's outcome. */
  def entry(index: Int, result: Either[Rejection, Receipt]): BatchEntry = result match
    case Right(receipt)  => BatchEntry(index, Some(event(receipt)), None)
    case Left(rejection) => BatchEntry(index, None, Some(failure(rejection).error))

  /** Renders a whole batch outcome, together with the status it is served with.
    *
    * **207 for a partial failure, 200 for a clean one.** A batch in which some elements were refused is not a failed
    * request — the accepted events are durable and must not be resent — so a 4xx would be actively harmful: a client
    * retrying the whole document on 400 would duplicate every event that succeeded. 207 Multi-Status says exactly "look
    * inside", which is the only truthful answer. AIP-233 assumes an atomic batch and so does not cover this case; the
    * deviation is stated on the endpoint itself.
    */
  def batch(outcome: BatchOutcome): (StatusCode, BatchCreateResponse) =
    val entries = outcome.results.zipWithIndex.map((result, index) => entry(index, result)).toList
    val status = if outcome.rejected == 0 then StatusCode.Ok else StatusCode.MultiStatus
    (status, BatchCreateResponse(entries.flatMap(_.event), entries, outcome.accepted, outcome.rejected))
