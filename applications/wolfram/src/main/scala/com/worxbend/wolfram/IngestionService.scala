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

import com.worxbend.eventing.ContentMode
import com.worxbend.kernel.event.Envelope
import java.time.Instant
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** One event that passed validation, with everything the publisher and the response need already decided. */
final case class Accepted(envelope: Envelope, mode: ContentMode, occurredAt: OffsetDateTime, partitionKey: String)

/** A published event and the broker's receipt for it. */
final case class Receipt(accepted: Accepted, ack: PublishAck)

/** The outcome of one batch document: one result per element, in the order they were sent.
  *
  * Order is part of the contract — a client correlates by index, because CloudEvents `id` is only unique per `source`
  * and a batch may legitimately contain two events from two sources with the same id.
  */
final case class BatchOutcome(results: Vector[Either[Rejection, Receipt]]):
  def accepted: Int = results.count(_.isRight)
  def rejected: Int = results.count(_.isLeft)

/** The application service: decode, validate, publish. wolfram's whole product.
  *
  * **Layering.** This class knows nothing about HTTP — no status codes, no media-type constants beyond the CloudEvents
  * binding's own, no Tapir. That is what lets the endpoint tests drive the interesting cases (every rejection path,
  * batch partial failure) through a stub broker, and lets this class be tested without a server at all. The mapping
  * from [[Rejection]] to a status code lives once, in [[ApiModel]].
  *
  * **Order of checks is deliberate.** Size first, because a 2 GB body must be refused before it is turned into a
  * `String`; then decode, because everything after needs an envelope; then the time clamp, because it is the only check
  * whose failure is a *policy* rather than a spec violation and it reads better in a message when the event is
  * otherwise known-good.
  *
  * **The partition key is not computed here.** It is read off the envelope — `Envelope.partitionKey`, kernel's single
  * definition (ADR §4.1). This service holds it only so the response can report it and so a test can assert the
  * delegation; recomputing `source#subject` locally is the exact drift `modules/kernel` exists to prevent, and it would
  * be invisible until someone plotted a device timeline months later.
  */
final class IngestionService(
  publisher: EventPublisher,
  clamp: TimeClamp,
  limits: IngestConfig,
  metrics: IngestMetrics,
  now: () => Instant = () => Instant.now()
)(using ExecutionContext):

  /** Ingests one event in either content mode. */
  def ingest(headers: Map[String, String], body: Array[Byte]): Future[Either[Rejection, Receipt]] =
    validate(headers, body) match
      case Left(rejection)  => Future.successful(Left(record(rejection)))
      case Right(candidate) => publish(candidate)

  /** Ingests an `application/cloudevents-batch+json` document.
    *
    * The outer `Either` is the *document's* validity (not JSON, not an array, too many elements); the inner ones are
    * per-event. A batch never fails as a whole because one element is bad — see [[HttpBinding.decodeBatch]].
    *
    * Elements are published sequentially rather than with `Future.traverse`. Concurrency here would let two events for
    * the same device reach the accumulator out of order, discarding the per-key ordering guarantee that
    * `Envelope.partitionKey` exists to provide — and a batch is bounded by `max-batch-events`, so the serial cost is
    * bounded too.
    */
  def ingestBatch(body: Array[Byte]): Future[Either[Rejection, BatchOutcome]] =
    if body.length > limits.maxEventBytes then
      Future.successful(Left(record(Rejection.TooLarge(limits.maxEventBytes, body.length.toLong, "bytes"))))
    else
      HttpBinding.decodeBatch(body) match
        case Left(rejection) => Future.successful(Left(record(rejection)))
        case Right(elements) =>
          if elements.sizeIs > limits.maxBatchEvents then
            Future.successful(
              Left(record(Rejection.TooLarge(limits.maxBatchEvents.toLong, elements.size.toLong, "events")))
            )
          else
            val instant = now()
            elements
              .foldLeft(Future.successful(Vector.empty[Either[Rejection, Receipt]])): (acc, element) =>
                acc.flatMap: results =>
                  val one = element.flatMap(envelope => check(envelope, ContentMode.Structured, instant)) match
                    case Left(rejection)  => Future.successful(Left(record(rejection)))
                    case Right(candidate) => publish(candidate)
                  one.map(results :+ _)
              .map(results => Right(BatchOutcome(results)))

  /** Everything that can be decided without talking to the broker: size, content mode, attributes, and the time window.
    * Pure, so `events:validate` can answer with it, and so every rejection path is reachable from a test with neither a
    * server nor a broker.
    */
  def validate(headers: Map[String, String], body: Array[Byte]): Either[Rejection, Accepted] =
    if body.length > limits.maxEventBytes then
      Left(Rejection.TooLarge(limits.maxEventBytes, body.length.toLong, "bytes"))
    else
      val mode = HttpBinding.modeOf(headers).getOrElse(ContentMode.Binary)
      HttpBinding.decode(headers, body).flatMap(envelope => check(envelope, mode, now()))

  private def check(envelope: Envelope, mode: ContentMode, instant: Instant): Either[Rejection, Accepted] =
    clamp
      .check(envelope, instant)
      .map(occurredAt => Accepted(envelope, mode, occurredAt, envelope.partitionKey))

  private def publish(candidate: Accepted): Future[Either[Rejection, Receipt]] =
    publisher
      .publish(candidate.envelope)
      .map:
        case Right(ack) =>
          metrics.accepted(candidate.envelope, candidate.mode)
          // After the ack, not before: refinement is an observation about events that are now durable, and counting a
          // type the fleet does not understand for an event the broker then refused would report a producer problem
          // that did not happen.
          metrics.observed(candidate.envelope)
          Right(Receipt(candidate, ack))
        case Left(rejection) =>
          Left(record(rejection))

  /** Counts a rejection and returns it unchanged, so that "every rejection is metered" is enforced by there being no
    * other way to produce one from this class rather than by remembering to call the counter at eight sites.
    */
  private def record(rejection: Rejection): Rejection =
    metrics.rejected(rejection)
    rejection
