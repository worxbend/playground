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

import io.circe.Codec
import io.kzonix.kernel.Rfc3339
import sttp.model.StatusCode

/** The receipt for one accepted event.
  *
  * It echoes the identity the producer sent (`id`, `source`) rather than assigning one, because CloudEvents makes
  * `(source, id)` the producer's contract and the whole deduplication story downstream depends on this service not
  * touching it. `partitionKey`, `partition` and `offset` are the diagnostic half: they are what an operator needs to
  * find the event again with `kcat`, and they let a producer see immediately when its events are not landing where it
  * expected.
  */
final case class EventAccepted(
  id: String,
  source: String,
  eventType: String,
  time: String,
  partitionKey: String,
  topic: String,
  partition: Int,
  offset: Long
) derives Codec.AsObject

/** Why a request was refused, in the shape a client sees.
  *
  * Split into three concrete types rather than one with a status field because Tapir's `oneOf` matches on the runtime
  * class: the type *is* the status code, so a 413 body cannot be returned with a 400 and no test has to check that it
  * wasn't. Every one of them carries `reason` — the closed-set operator category, identical to the Prometheus tag — and
  * `detail`, the sentence naming what was actually wrong.
  */
sealed trait IngestFailure:
  def reason: String
  def detail: String

/** 400. The event is not a CloudEvent this service can accept. */
final case class InvalidEvent(reason: String, detail: String) extends IngestFailure derives Codec.AsObject

/** 413. The body, or the number of events in a batch, exceeded a configured ceiling. */
final case class OversizeEvent(reason: String, detail: String, limit: Long, actual: Long, unit: String)
    extends IngestFailure derives Codec.AsObject

/** 503. The event was fine; the broker was not. Retryable, and the only failure here that is. */
final case class ServiceUnavailable(reason: String, detail: String) extends IngestFailure derives Codec.AsObject

/** One element's outcome inside a batch, correlated to the request by `index`.
  *
  * By index and not by `id` because a CloudEvents `id` is only unique within a `source`, so a batch may legitimately
  * carry two elements with the same id — and a malformed element may have no id at all.
  */
final case class BatchEntry(
  index: Int,
  accepted: Boolean,
  id: Option[String],
  partitionKey: Option[String],
  partition: Option[Int],
  offset: Option[Long],
  reason: Option[String],
  detail: Option[String]
) derives Codec.AsObject

/** The result of one batch document. */
final case class BatchReport(accepted: Int, rejected: Int, entries: List[BatchEntry]) derives Codec.AsObject

/** Translation between the service's [[Rejection]] values and the HTTP surface.
  *
  * It lives in one object so the mapping from a rejection to a status code exists exactly once. The alternative — a
  * status code on `Rejection` itself — would put `sttp.model` in the ingestion layer and make the service untestable
  * without a web stack for no gain.
  */
object ApiModel:

  /** Renders a receipt. `time` is re-rendered through `Rfc3339` rather than `OffsetDateTime#toString` because the JDK
    * formatter omits the seconds field when it is zero, which is not RFC 3339 — the same defect `modules/eventing`
    * documents in the CloudEvents SDK's Kafka serializer.
    */
  def accepted(receipt: Receipt): EventAccepted =
    EventAccepted(
      id = receipt.accepted.envelope.id,
      source = receipt.accepted.envelope.source,
      eventType = receipt.accepted.envelope.eventType,
      time = Rfc3339.render(receipt.accepted.occurredAt),
      partitionKey = receipt.accepted.partitionKey,
      topic = receipt.ack.topic,
      partition = receipt.ack.partition,
      offset = receipt.ack.offset
    )

  /** The one place a rejection becomes an HTTP status. Exhaustive over the enum, so a new rejection kind is a compile
    * error here rather than a 500 in production.
    */
  def failure(rejection: Rejection): IngestFailure = rejection match
    case Rejection.Malformed(cause)          => InvalidEvent(rejection.reason, cause)
    case Rejection.InvalidAttributes(cause)  => InvalidEvent(rejection.reason, cause)
    case Rejection.ImplausibleTime(cause)    => InvalidEvent(rejection.reason, cause)
    case Rejection.TooLarge(limit, actual, unit) =>
      OversizeEvent(rejection.reason, rejection.detail, limit, actual, unit)
    case Rejection.BrokerUnavailable(cause)  => ServiceUnavailable(rejection.reason, cause)

  /** The status a failure is served with. Stated here as well as on the endpoint's `oneOf` so the two can be asserted
    * equal in a test — the endpoint description is the authority the server obeys, this is the authority a reader can
    * check it against.
    */
  def status(failure: IngestFailure): StatusCode = failure match
    case _: InvalidEvent       => StatusCode.BadRequest
    case _: OversizeEvent      => StatusCode.PayloadTooLarge
    case _: ServiceUnavailable => StatusCode.ServiceUnavailable

  /** Renders one batch element's outcome. */
  def entry(index: Int, result: Either[Rejection, Receipt]): BatchEntry = result match
    case Right(receipt) =>
      BatchEntry(
        index = index,
        accepted = true,
        id = Some(receipt.accepted.envelope.id),
        partitionKey = Some(receipt.accepted.partitionKey),
        partition = Some(receipt.ack.partition),
        offset = Some(receipt.ack.offset),
        reason = None,
        detail = None
      )
    case Left(rejection) =>
      BatchEntry(
        index = index,
        accepted = false,
        id = None,
        partitionKey = None,
        partition = None,
        offset = None,
        reason = Some(rejection.reason),
        detail = Some(rejection.detail)
      )

  /** Renders a whole batch outcome, together with the status it is served with.
    *
    * **207 for a partial failure, 202 for a clean one.** A batch in which some elements were refused is not a failed
    * request — the accepted events are durable and must not be resent — so a 4xx would be actively harmful: a client
    * retrying the whole document on 400 would duplicate every event that succeeded. 207 Multi-Status says exactly
    * "look inside", which is the only truthful answer.
    */
  def report(outcome: BatchOutcome): (StatusCode, BatchReport) =
    val entries = outcome.results.zipWithIndex.map((result, index) => entry(index, result)).toList
    val status = if outcome.rejected == 0 then StatusCode.Accepted else StatusCode.MultiStatus
    (status, BatchReport(outcome.accepted, outcome.rejected, entries))
