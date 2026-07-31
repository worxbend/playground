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

package com.worxbend.kernel.event

import io.circe.Json
import org.scalacheck.Prop.forAll

/** The Kafka partition key and the topic constants.
  *
  * Determinism is not a nicety here: the key decides the partition, the partition decides ordering, and a key that
  * varies with anything but `(source, subject)` reorders a device's timeline in a way nothing downstream can detect.
  */
final class PartitionKeySuite extends munit.ScalaCheckSuite:

  private def envelopeOf(source: String, subject: Option[String]): Envelope =
    Envelope(
      id = Generators.force(EventId("e-1")),
      source = Generators.force(Source(source)),
      eventType = Generators.force(EventType("com.example.t")),
      time = None,
      subject = subject.map(s => Generators.force(Subject(s))),
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Empty
    )

  test("the key is source#subject when a subject is present"):
    assertEquals(envelopeOf("/gateways/1", Some("thermo-1")).partitionKey, "/gateways/1#thermo-1")

  test("the key is the bare source when no subject is present"):
    assertEquals(envelopeOf("/gateways/1", None).partitionKey, "/gateways/1")

  test("two gateways using the same local subject do not share a key"):
    assertNotEquals(
      envelopeOf("/gateways/1", Some("kitchen-1")).partitionKey,
      envelopeOf("/gateways/2", Some("kitchen-1")).partitionKey
    )

  property("the key depends on source and subject only"):
    forAll(Generators.genEnvelope): envelope =>
      val perturbed = envelope.copy(
        id = Generators.force(EventId("different")),
        eventType = Generators.force(EventType("com.example.other")),
        extensions = Map("extra" -> AttrValue.Flag(true)),
        payload = Payload.Structured(Json.fromString("different"))
      )
      envelope.partitionKey == perturbed.partitionKey

  property("the key is stable across repeated calls"):
    forAll(Generators.genEnvelope): envelope =>
      envelope.partitionKey == envelope.partitionKey && envelope.partitionKey == envelope.canonical.partitionKey

  test("topic names and partition counts are the ones the ADR fixes"):
    assertEquals(Topics.CloudEvents, "events.cloudevents.v1")
    assertEquals(Topics.CloudEventsDlq, "events.cloudevents.v1.dlq")
    assertEquals(Topics.CloudEventsPartitions, 12)
    assertEquals(Topics.CloudEventsDlqPartitions, 3)

  test("the DLQ key identifies the origin record so a replay overwrites"):
    assertEquals(Topics.dlqKey(Topics.CloudEvents, 7, 42L), "events.cloudevents.v1/7/42")
    assertEquals(
      Topics.dlqKey(Topics.CloudEvents, 7, 42L),
      Topics.dlqKey(Topics.CloudEvents, 7, 42L)
    )
