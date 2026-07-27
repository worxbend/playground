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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import sttp.model.Header
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint

/** The HTTP contract, as values.
  *
  * Endpoint descriptions are separated from server logic so that the same values drive three things that must not be
  * allowed to disagree: the Vert.x routes, the OpenAPI document ([[OpenApi]]), and the stub-server tests. Writing the
  * contract once is the entire argument for Tapir in this service; restating it in a `routes` file and again in a
  * hand-maintained spec is precisely the drift this avoids.
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
  */
object Endpoints:

  /** Media types this API advertises for a request body. Kept as values because [[OpenApi]] documents them and the
    * tests assert against them; a literal repeated in three places is a literal that will disagree with itself.
    */
  val SingleRequestMediaTypes: List[String] =
    List(HttpBinding.StructuredMediaType, "application/json", "application/octet-stream")

  val BatchRequestMediaTypes: List[String] = List(HttpBinding.BatchMediaType)

  /** The failure outputs, shared by both endpoints.
    *
    * `oneOf` and not a single body with a status field: Tapir matches variants on the runtime class, so the type of the
    * error value chooses the status code and the two cannot drift. The 503 variant is last only for readability —
    * matching is by class, not by order.
    */
  val failures: EndpointOutput.OneOf[IngestFailure, IngestFailure] =
    oneOf[IngestFailure](
      oneOfVariant(
        StatusCode.BadRequest,
        jsonBody[InvalidEvent].description(
          "The request was not an acceptable CloudEvent. `reason` is the operator category; `detail` names what " +
            "was wrong — a missing attribute, an unparseable body, or a `time` outside the plausibility window."
        )
      ),
      oneOfVariant(
        StatusCode.PayloadTooLarge,
        jsonBody[OversizeEvent].description(
          "The body exceeded `wolfram.ingest.max-event-bytes`, or the batch exceeded `max-batch-events`. " +
            "`limit` and `actual` are in `unit`."
        )
      ),
      oneOfVariant(
        StatusCode.ServiceUnavailable,
        jsonBody[ServiceUnavailable].description(
          "The event was valid but could not be made durable: the broker did not acknowledge it, or ingestion shed " +
            "load rather than queue in front of it. Retryable — the event was not published."
        )
      )
    )

  /** `POST /events` — one CloudEvent, in either content mode. */
  val publishEvent: PublicEndpoint[(List[Header], Array[Byte]), IngestFailure, EventAccepted, Any] =
    endpoint.post
      .in("events")
      .in(headers)
      .in(byteArrayBody)
      .out(statusCode(StatusCode.Accepted))
      .out(jsonBody[EventAccepted].description("The broker's receipt: where the event landed."))
      .errorOut(failures)
      .name("publishEvent")
      .summary("Publish one CloudEvent")
      .description(
        """Accepts a single CloudEvents 1.0 event in either HTTP content mode.
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
          |one would silently misfile the event.""".stripMargin
      )
      .tag("ingestion")

  /** `POST /events/batch` — an `application/cloudevents-batch+json` document.
    *
    * A distinct path rather than a third content mode on `/events`. The CloudEvents HTTP binding does put batch on the
    * same resource, but a batch is a different operation with a different response shape (a per-element report, and a
    * 207), and OpenAPI keys operations by `(path, method)` — one path serving two response schemas chosen by request
    * media type is a document no generated client can express.
    */
  val publishBatch: PublicEndpoint[(List[Header], Array[Byte]), IngestFailure, (StatusCode, BatchReport), Any] =
    endpoint.post
      .in("events" / "batch")
      .in(headers)
      .in(byteArrayBody)
      .out(
        statusCode
          .description(StatusCode.Accepted, "Every event in the batch was published.")
          .description(StatusCode.MultiStatus, "Some events were published and some were refused; see `entries`.")
      )
      .out(jsonBody[BatchReport].description("One entry per element of the request, in request order."))
      .errorOut(failures)
      .name("publishEventBatch")
      .summary("Publish a batch of CloudEvents")
      .description(
        """Accepts an `application/cloudevents-batch+json` document: a JSON array of structured-mode CloudEvents.
          |
          |A batch is a convenience, not a transaction. A malformed element does not fail the request — the valid
          |events are published and the response reports each element's outcome by index. Retrying a 207 as a whole
          |would duplicate every event that succeeded; retry only the entries marked `accepted: false`.""".stripMargin
      )
      .tag("ingestion")

  /** Everything this API serves, for documentation and for routing. */
  val all: List[AnyEndpoint] = List(publishEvent, publishBatch)

  /** The request media types each endpoint advertises, keyed by endpoint name.
    *
    * Tapir's `byteArrayBody` reports one media type (`application/octet-stream`) because that is what its codec format
    * says, and there is no way to attach the others to the body without also changing how the body decodes. Stating
    * them here keeps [[OpenApi]] generated *from the endpoint values* while still documenting the media types a client
    * may actually send. A dedicated `tapir-openapi-docs` dependency would remove the need for this — see the module
    * notes.
    */
  val requestMediaTypes: Map[String, List[String]] =
    Map("publishEvent" -> SingleRequestMediaTypes, "publishEventBatch" -> BatchRequestMediaTypes)

/** The endpoints bound to [[IngestionService]].
  *
  * A class rather than an object because the service is a constructor dependency: wolfram has no global state, and a
  * singleton holding a Kafka producer would make the whole HTTP layer untestable and un-restartable.
  */
final class IngestApi(service: IngestionService)(using ExecutionContext):

  /** Server endpoints, ready for any Tapir interpreter. */
  val routes: List[ServerEndpoint[Any, Future]] =
    List(
      Endpoints.publishEvent.serverLogic(publish),
      Endpoints.publishBatch.serverLogic(publishBatch)
    )

  private def publish(input: (List[Header], Array[Byte])): Future[Either[IngestFailure, EventAccepted]] =
    val (requestHeaders, body) = input
    service
      .ingest(IngestApi.headerMap(requestHeaders), body)
      .map(result => result.left.map(ApiModel.failure).map(ApiModel.accepted))

  private def publishBatch(
    input: (List[Header], Array[Byte])
  ): Future[Either[IngestFailure, (StatusCode, BatchReport)]] =
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
    else
      service
        .ingestBatch(body)
        .map(result => result.left.map(ApiModel.failure).map(ApiModel.report))

object IngestApi:

  /** Flattens Tapir's header list into the case-insensitive map the binding works with. */
  def headerMap(headers: List[Header]): Map[String, String] =
    HttpBinding.normalise(headers.map(header => header.name -> header.value))
