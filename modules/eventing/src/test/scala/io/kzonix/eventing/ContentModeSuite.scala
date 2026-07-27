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

package io.kzonix.eventing

import io.circe.Json
import io.kzonix.kernel.event.AttrValue
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.Topics
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Optional
import munit.ScalaCheckSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop.propBoolean

/** Round trips through both Kafka content modes, and the guarantee that a malformed record is a value.
  *
  * Binary is asserted against `CloudEventAdapter.binaryCanonical` rather than against the envelope itself, because a
  * header is bytes and extension *types* genuinely do not survive it. Naming that loss in a function keeps the property
  * an equation; asserting the envelope directly would only be achievable by weakening the comparison, which is how a
  * real encoding bug hides.
  */
class ContentModeSuite extends ScalaCheckSuite:

  property("binary mode round-trips every envelope up to the header-erasure canonical form"):
    forAll(WireGenerators.genEnvelope): envelope =>
      val expected = CloudEventAdapter.binaryCanonical(envelope)
      val actual = roundTrip(ContentMode.Binary, envelope)
      (actual == Right(expected)) :| s"expected Right($expected) but got $actual"

  property("structured mode round-trips every envelope exactly"):
    forAll(WireGenerators.genEnvelope): envelope =>
      val actual = roundTrip(ContentMode.Structured, envelope)
      (actual == Right(envelope)) :| s"expected Right($envelope) but got $actual"

  property("a binary record is always detected as binary and a structured one as structured"):
    forAll(WireGenerators.genEnvelope): envelope =>
      val binary = RecordHeaders()
      val structured = RecordHeaders()
      val _ = ContentMode.write(ContentMode.Binary, envelope, binary)
      val _ = ContentMode.write(ContentMode.Structured, envelope, structured)
      ContentMode.of(binary) == Some(ContentMode.Binary) &&
      ContentMode.of(structured) == Some(ContentMode.Structured)

  test("a binary record whose payload is itself a CloudEvents JSON document is not mistaken for structured mode"):
    // The ambiguity the SDK's own detection order gets wrong: `content-type` in binary mode describes the payload.
    val quoted = Json.obj("specversion" -> Json.fromString("1.0"), "id" -> Json.fromString("inner"))
    val envelope = base.copy(
      dataContentType = Some(ContentType.CloudEventsJson),
      payload = Payload.Structured(quoted)
    )
    assertEquals(roundTrip(ContentMode.Binary, envelope).map(_.payload), Right(Payload.Structured(quoted)))
    assertEquals(roundTrip(ContentMode.Binary, envelope).map(_.id: String), Right(base.id: String))

  test("an event with no data round-trips as an event with no data, not as zero bytes"):
    val envelope = base.copy(payload = Payload.Empty)
    assertEquals(roundTrip(ContentMode.Binary, envelope).map(_.payload), Right(Payload.Empty))
    assertEquals(roundTrip(ContentMode.Structured, envelope).map(_.payload), Right(Payload.Empty))

  test("binary mode puts the attributes where the binding says, and nowhere else"):
    val envelope = base.copy(
      subject = Some(WireGenerators.force(io.kzonix.kernel.event.Subject("kitchen-1"))),
      extensions = Map("tenantid" -> AttrValue.Text("acme"))
    )
    val headers = RecordHeaders()
    val value = ContentMode.write(ContentMode.Binary, envelope, headers)
    assertEquals(CloudEventHeaders.get(headers, "ce_specversion"), Some("1.0"))
    assertEquals(CloudEventHeaders.get(headers, "ce_id"), Some("event-1"))
    assertEquals(CloudEventHeaders.get(headers, "ce_source"), Some("/sensors/kitchen"))
    assertEquals(CloudEventHeaders.get(headers, "ce_type"), Some("io.kzonix.iot.telemetry"))
    assertEquals(CloudEventHeaders.get(headers, "ce_subject"), Some("kitchen-1"))
    assertEquals(CloudEventHeaders.get(headers, "ce_tenantid"), Some("acme"))
    assertEquals(value, Right(None))

  test("binary mode renders time as RFC 3339, keeping the seconds field the SDK's own writer would drop"):
    val headers = RecordHeaders()
    val _ = ContentMode.write(ContentMode.Binary, base, headers)
    assertEquals(CloudEventHeaders.get(headers, "ce_time"), Some("2024-01-01T17:31:00Z"))

  test("structured mode carries everything in the value and marks it with the CloudEvents media type"):
    val headers = RecordHeaders()
    val value = ContentMode.write(ContentMode.Structured, base, headers)
    assertEquals(CloudEventHeaders.get(headers, "content-type"), Some("application/cloudevents+json"))
    assertEquals(CloudEventHeaders.get(headers, "ce_id"), None)
    assertEquals(value.map(_.map(bytes => String(bytes, UTF_8))), Right(Some(base.render)))

  test("an extension name the spec forbids is refused before the record is built"):
    val envelope = base.copy(extensions = Map("Tenant-Id" -> AttrValue.Text("acme")))
    assert(ContentMode.write(ContentMode.Binary, envelope, RecordHeaders()).isLeft)
    assert(KafkaCodecs.producerRecord(Topics.CloudEvents, envelope).isLeft)

  test("a record with no CloudEvents headers at all decodes to UnknownEncoding, not an exception"):
    val decoded = ContentMode.read(RecordHeaders(), Some("hello".getBytes(UTF_8)))
    assertEquals(decoded.left.map(_.reason), Left("unknown-encoding"))

  test("a structured record whose value is not JSON decodes to MalformedStructured"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "content-type", "application/cloudevents+json")
    val decoded = ContentMode.read(headers, Some("{not json".getBytes(UTF_8)))
    assertEquals(decoded.left.map(_.reason), Left("malformed-structured"))

  test("a structured record whose JSON is not a CloudEvent decodes to MalformedStructured"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "content-type", "application/cloudevents+json")
    val decoded = ContentMode.read(headers, Some("""{"specversion":"1.0"}""".getBytes(UTF_8)))
    assertEquals(decoded.left.map(_.reason), Left("malformed-structured"))

  test("a binary record with a broken attribute decodes to MalformedBinary"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "ce_specversion", "1.0")
    val _ = CloudEventHeaders.put(headers, "ce_id", "a")
    val _ = CloudEventHeaders.put(headers, "ce_source", "/x")
    val _ = CloudEventHeaders.put(headers, "ce_type", "com.example.a")
    val _ = CloudEventHeaders.put(headers, "ce_time", "not-a-timestamp")
    assertEquals(ContentMode.read(headers, None).left.map(_.reason), Left("malformed-binary"))

  test("a binary record with an unrepresentable extension name decodes to MalformedBinary, never a throw"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "ce_specversion", "1.0")
    val _ = CloudEventHeaders.put(headers, "ce_id", "a")
    val _ = CloudEventHeaders.put(headers, "ce_source", "/x")
    val _ = CloudEventHeaders.put(headers, "ce_type", "com.example.a")
    val _ = CloudEventHeaders.put(headers, "ce_tenant-id", "acme")
    assertEquals(ContentMode.read(headers, None).left.map(_.reason), Left("malformed-binary"))

  test("a binary record with a blank id is Unconvertible — the bytes are fine, the domain rejects them"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "ce_specversion", "1.0")
    val _ = CloudEventHeaders.put(headers, "ce_id", "  ")
    val _ = CloudEventHeaders.put(headers, "ce_source", "/x")
    val _ = CloudEventHeaders.put(headers, "ce_type", "com.example.a")
    assertEquals(ContentMode.read(headers, None).left.map(_.reason), Left("unconvertible"))

  test("the producer record is keyed by the kernel's partition key and defaults to binary mode"):
    val envelope = base.copy(subject = Some(WireGenerators.force(io.kzonix.kernel.event.Subject("kitchen-1"))))
    val record = KafkaCodecs.producerRecord(Topics.CloudEvents, envelope)
    assertEquals(record.map(_.key), Right(envelope.partitionKey))
    assertEquals(record.map(_.topic), Right(Topics.CloudEvents))
    assertEquals(record.map(r => ContentMode.of(r.headers)), Right(Some(ContentMode.Binary)))

  test("the serializer/deserializer pair round-trips through the headers Kafka hands them"):
    val serializer = KafkaCodecs.envelopeSerializer()
    val deserializer = KafkaCodecs.envelopeDeserializer
    val headers: Headers = RecordHeaders()
    val bytes = serializer.serialize(Topics.CloudEvents, headers, base)
    assertEquals(deserializer.deserialize(Topics.CloudEvents, headers, bytes), Right(base))

  test("the deserializer turns rubbish into a Left instead of throwing into the poll loop"):
    val deserializer = KafkaCodecs.envelopeDeserializer
    val decoded = deserializer.deserialize(Topics.CloudEvents, RecordHeaders(), Array[Byte](0, 1, 2))
    assert(decoded.isLeft)

  test("decoding a consumed record never throws, whatever is in it"):
    val record = ConsumerRecord[String, Array[Byte]](
      Topics.CloudEvents,
      3,
      99L,
      1700000000000L,
      TimestampType.CREATE_TIME,
      1,
      3,
      "k",
      Array[Byte](7, 7, 7),
      RecordHeaders(),
      Optional.empty[Integer]
    )
    assertEquals(KafkaCodecs.decode(record).left.map(_.reason), Left("unknown-encoding"))

  private def roundTrip(mode: ContentMode, envelope: Envelope): Either[String, Envelope] =
    val headers: Headers = RecordHeaders()
    ContentMode
      .write(mode, envelope, headers)
      .flatMap(value => ContentMode.read(headers, value).left.map(_.message))

  private val base: Envelope =
    Envelope(
      id = WireGenerators.force(io.kzonix.kernel.event.EventId("event-1")),
      source = WireGenerators.force(io.kzonix.kernel.event.Source("/sensors/kitchen")),
      eventType = WireGenerators.force(io.kzonix.kernel.event.EventType("io.kzonix.iot.telemetry")),
      time = Some(java.time.OffsetDateTime.of(2024, 1, 1, 17, 31, 0, 0, java.time.ZoneOffset.UTC)),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Empty
    )
