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

package com.worxbend.eventing

import com.worxbend.kernel.event.Binary
import com.worxbend.kernel.event.Topics
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Optional
import munit.ScalaCheckSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Prop.forAll

/** The dead-letter wrapper, tested the way it will actually be used: written to the DLQ as a structured CloudEvent and
  * read back by whoever has to diagnose or replay it.
  *
  * The round trip therefore goes through the *whole* path — dead letter to envelope to structured record bytes and all
  * the way back — rather than just the JSON codec. A DLQ that is only correct up to its own encoder is not a DLQ.
  */
class DeadLetterSuite extends ScalaCheckSuite:

  property("a dead letter survives the full DLQ round trip: wrapper, envelope, record bytes, and back"):
    forAll(WireGenerators.genDeadLetter): deadLetter =>
      val headers = RecordHeaders()
      val recovered = ContentMode
        .write(ContentMode.Structured, deadLetter.toEnvelope, headers)
        .left
        .map(DecodeFailure.MalformedStructured.apply)
        .flatMap(value => ContentMode.read(headers, value))
        .flatMap(envelope => DeadLetter.fromEnvelope(envelope).left.map(DecodeFailure.Unconvertible.apply))
      recovered == Right(deadLetter)

  property("the DLQ record is keyed on the origin coordinates so a replay overwrites"):
    forAll(WireGenerators.genDeadLetter): deadLetter =>
      KafkaCodecs.deadLetterRecord(deadLetter).map(_.key) ==
        Right(Topics.dlqKey(deadLetter.origin.topic, deadLetter.origin.partition, deadLetter.origin.offset))

  property("the DLQ record is in structured mode, whatever mode the original record was in"):
    forAll(WireGenerators.genDeadLetter): deadLetter =>
      KafkaCodecs.deadLetterRecord(deadLetter).map(record => ContentMode.of(record.headers)) ==
        Right(Some(ContentMode.Structured))

  test("a poison record becomes a dead letter carrying its bytes, headers and coordinates verbatim"):
    val payload = Array[Byte](0, 1, 2, 3)
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "ce_specversion", "1.0")
    val _ = CloudEventHeaders.put(headers, "ce_id", "  ")
    val record = ConsumerRecord[String, Array[Byte]](
      Topics.CloudEvents,
      7,
      4242L,
      1700000000000L,
      TimestampType.CREATE_TIME,
      1,
      payload.length,
      "device-1",
      payload,
      headers,
      Optional.empty[Integer]
    )

    val outcome = KafkaCodecs.decodeOrDeadLetter(record)
    assert(outcome.isLeft, outcome)
    val deadLetter = outcome.swap.getOrElse(fail("expected a dead letter"))

    assertEquals(deadLetter.origin.topic, Topics.CloudEvents)
    assertEquals(deadLetter.origin.partition, 7)
    assertEquals(deadLetter.origin.offset, 4242L)
    assertEquals(deadLetter.origin.key, Some("device-1"))
    assertEquals(deadLetter.origin.timestamp, Some(1700000000000L))
    assertEquals(deadLetter.payload, Some(Binary.copyOf(payload)))
    assertEquals(deadLetter.headers.get("ce_id"), Some("  "))
    assertEquals(deadLetter.reason, "malformed-binary")

  test("the dead-letter envelope is a plain CloudEvent, readable by the same decoder as everything else"):
    val deadLetter = DeadLetter(
      origin = RecordOrigin(Topics.CloudEvents, 1, 2L, None, None),
      reason = "unknown-encoding",
      detail = "no CloudEvents headers",
      failedAt = java.time.OffsetDateTime.of(2024, 5, 1, 8, 0, 0, 0, java.time.ZoneOffset.UTC),
      headers = Map.empty,
      payload = None,
      source = DeadLetter.DefaultSource
    )
    val record = KafkaCodecs.deadLetterRecord(deadLetter).getOrElse(fail("could not build the DLQ record"))
    val decoded = ContentMode.read(record.headers, Option(record.value))
    assertEquals(decoded.map(_.eventType: String), Right(DeadLetter.EventTypeName: String))
    assertEquals(decoded.map(_.id: String), Right("events.cloudevents.v1/1/2"))
    assertEquals(decoded.map(_.subject.map(s => s: String)), Right(Some(Topics.CloudEvents)))
    assert(String(record.value, UTF_8).contains("unknown-encoding"))

  test("an envelope that is not a dead letter is refused rather than half-decoded"):
    val envelope = WireGenerators.force(
      Envelopes.simple(Topics.CloudEvents)
    )
    assert(DeadLetter.fromEnvelope(envelope).isLeft)

/** A minimal well-formed envelope, kept out of the suite body so the intent of each test stays visible. */
private object Envelopes:

  def simple(subject: String): Either[String, com.worxbend.kernel.event.Envelope] =
    for
      id <- com.worxbend.kernel.event.EventId("plain-1")
      source <- com.worxbend.kernel.event.Source("/sensors/kitchen")
      eventType <- com.worxbend.kernel.event.EventType("com.worxbend.iot.telemetry")
      subj <- com.worxbend.kernel.event.Subject(subject)
    yield com.worxbend.kernel.event.Envelope(
      id,
      source,
      eventType,
      None,
      Some(subj),
      None,
      None,
      Map.empty,
      com.worxbend.kernel.event.Payload.Empty
    )
