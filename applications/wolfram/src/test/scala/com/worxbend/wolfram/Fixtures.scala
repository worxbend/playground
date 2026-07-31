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
import com.worxbend.kernel.event.Envelope
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** Shared test data and doubles.
  *
  * Everything here builds *wire* representations — JSON text and header maps — rather than `Envelope` values, because
  * the code under test starts at the wire. A fixture that handed the service a pre-built `Envelope` would skip the
  * binding, which is where most of the interesting failures live.
  */
object Fixtures:

  /** A fixed ingest clock. Tests that involve the time clamp must not race a real one: at 90 days minus a millisecond,
    * a wall clock turns an assertion into a coin flip.
    */
  val now: Instant = Instant.parse("2026-07-01T12:00:00Z")

  def at(offset: java.time.Duration): OffsetDateTime = now.plus(offset).atOffset(ZoneOffset.UTC)

  val eventTime: String = Rfc3339.render(at(java.time.Duration.ofMinutes(-1)))

  val ingest: IngestConfig =
    IngestConfig(maxEventBytes = 4096L, maxBatchEvents = 8, maxFutureSkew = 24.hours, maxPastSkew = 90.days)

  /** A structured-mode CloudEvents document. */
  def structuredBody(
    id: String = "evt-1",
    source: String = "/gateway/kitchen",
    eventType: String = "com.worxbend.iot.telemetry",
    subject: Option[String] = Some("kitchen-thermostat"),
    time: Option[String] = Some(eventTime)
  ): String =
    val fields = Vector(
      Some(s""""specversion":"1.0""""),
      Some(s""""id":"$id""""),
      Some(s""""source":"$source""""),
      Some(s""""type":"$eventType""""),
      subject.map(value => s""""subject":"$value""""),
      time.map(value => s""""time":"$value""""),
      Some(s""""datacontenttype":"application/json""""),
      Some(s""""data":{"celsius":21.5}""")
    ).flatten
    fields.mkString("{", ",", "}")

  val structuredHeaders: Map[String, String] = Map("content-type" -> HttpBinding.StructuredMediaType)

  val batchHeaders: Map[String, String] = Map("content-type" -> HttpBinding.BatchMediaType)

  /** Binary-mode headers for the same event. Deliberately mixed-case, so the case-insensitivity of HTTP header names is
    * exercised on every test that uses them rather than only on the one that names it.
    */
  def binaryHeaders(
    id: String = "evt-1",
    source: String = "/gateway/kitchen",
    eventType: String = "com.worxbend.iot.telemetry",
    subject: Option[String] = Some("kitchen-thermostat"),
    time: Option[String] = Some(eventTime)
  ): Map[String, String] =
    Map(
      "Ce-SpecVersion" -> "1.0",
      "ce-id" -> id,
      "CE-Source" -> source,
      "ce-type" -> eventType,
      "Content-Type" -> "application/json"
    ) ++ subject.map("ce-subject" -> _) ++ time.map("ce-time" -> _)

  val binaryBody: String = """{"celsius":21.5}"""

  /** A batch document built from structured elements. */
  def batchBody(elements: String*): String = elements.mkString("[", ",", "]")

  /** An [[EventPublisher]] whose answer is scripted.
    *
    * Records what it was asked to publish so a test can assert on the *envelope* the service produced, which is the
    * only place the partition key and the decoded payload are both visible.
    */
  final class StubPublisher(
    outcome: Envelope => Either[Rejection, PublishAck] = _ => Right(Fixtures.ack),
    reachable: Boolean = true
  ) extends EventPublisher:
    private val seen = AtomicReference(Vector.empty[Envelope])

    def published: Vector[Envelope] = seen.get()

    def publish(envelope: Envelope): Future[Either[Rejection, PublishAck]] =
      val _ = seen.updateAndGet(_ :+ envelope)
      Future.successful(outcome(envelope))

    def brokerReachable: Boolean = reachable
    def flush(): Unit = ()
    def close(): Unit = ()

  val ack: PublishAck = PublishAck("events.cloudevents.v1", 3, 42L, "/gateway/kitchen#kitchen-thermostat")

  /** The publisher every "the broker is down" test uses. */
  def unavailable: StubPublisher =
    StubPublisher(_ => Left(Rejection.BrokerUnavailable("no broker available")), reachable = false)
