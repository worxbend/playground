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
import io.kzonix.kernel.event.Binary
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.Payload
import java.net.URI
import java.time.OffsetDateTime
import java.time.ZoneOffset
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

/** Properties of the `Envelope` ↔ `CloudEvent` adapter.
  *
  * The central property is an equation, not a spot check: for *every* envelope,
  * `toEnvelope(toCloudEvent(e)) == Right(canonical(e))`. Paired with the idempotence property immediately after it,
  * that pins down exactly what the adapter may and may not change, and any future normalisation someone slips into
  * either direction breaks one of the two.
  */
class CloudEventAdapterSuite extends ScalaCheckSuite:

  property("envelope survives a round trip through the SDK, up to the documented canonical form"):
    forAll(WireGenerators.genRawEnvelope): envelope =>
      val roundTripped = CloudEventAdapter.toCloudEvent(envelope).flatMap(CloudEventAdapter.toEnvelope)
      roundTripped == Right(CloudEventAdapter.canonical(envelope))

  property("canonical is idempotent"):
    forAll(WireGenerators.genRawEnvelope): envelope =>
      val once = CloudEventAdapter.canonical(envelope)
      CloudEventAdapter.canonical(once) == once

  property("a spec-invalid extension name is a Left, never a thrown builder exception"):
    forAll(WireGenerators.genUnrepresentableEnvelope): envelope =>
      CloudEventAdapter.toCloudEvent(envelope).isLeft

  property("canonical never invents or drops an extension with a valid name"):
    forAll(WireGenerators.genRawEnvelope): envelope =>
      val expected = envelope.extensions.keySet.filterNot(Envelope.ReservedAttributes)
      CloudEventAdapter.canonical(envelope).extensions.keySet == expected

  test("the five typed extension shapes survive the adapter without collapsing to strings"):
    val when = OffsetDateTime.of(2024, 3, 1, 12, 0, 0, 0, ZoneOffset.ofHours(2))
    val envelope = base.copy(extensions =
      Map(
        "sequence" -> AttrValue.Num(42),
        "sampled" -> AttrValue.Flag(true),
        "observedat" -> AttrValue.Time(when),
        "origin" -> AttrValue.Ref(URI("https://gateway.example/1")),
        "signature" -> AttrValue.Bytes(Binary.copyOf(Array[Byte](1, 2, 3))),
        "tenantid" -> AttrValue.Text("acme")
      )
    )
    val roundTripped = CloudEventAdapter.toCloudEvent(envelope).flatMap(CloudEventAdapter.toEnvelope)
    assertEquals(roundTripped.map(_.extensions), Right(envelope.extensions))

  test("a JSON array extension degrades to its text rather than being dropped"):
    val json = Json.arr(Json.fromInt(1), Json.fromInt(2))
    val envelope = base.copy(extensions = Map("batch" -> AttrValue.Other(json)))
    val roundTripped = CloudEventAdapter.toCloudEvent(envelope).flatMap(CloudEventAdapter.toEnvelope)
    assertEquals(roundTripped.map(_.extensions), Right(Map("batch" -> AttrValue.Text("[1,2]"))))

  test("a reserved name is not an extension and is dropped, not written as an attribute"):
    val envelope = base.copy(extensions = Map("id" -> AttrValue.Text("shadow"), "tenantid" -> AttrValue.Text("acme")))
    val roundTripped = CloudEventAdapter.toCloudEvent(envelope).flatMap(CloudEventAdapter.toEnvelope)
    assertEquals(roundTripped.map(_.extensions), Right(Map("tenantid" -> AttrValue.Text("acme"))))
    assertEquals(roundTripped.map(_.id: String), Right(base.id: String))

  test("a JSON payload stays structured; a payload with a non-JSON media type becomes opaque"):
    val json = Json.obj("metric" -> Json.fromString("temperature"))
    val structured = base.copy(dataContentType = Some(jsonType), payload = Payload.Structured(json))
    assertEquals(
      CloudEventAdapter.toCloudEvent(structured).flatMap(CloudEventAdapter.toEnvelope).map(_.payload),
      Right(Payload.Structured(json))
    )
    val text = base.copy(dataContentType = Some(plainType), payload = Payload.Structured(json))
    assertEquals(
      CloudEventAdapter.toCloudEvent(text).flatMap(CloudEventAdapter.toEnvelope).map(_.payload),
      Right(Payload.Opaque(Binary.copyOf(json.noSpaces.getBytes("UTF-8")), plainType))
    )

  test("an event with no data is distinguishable from an event with zero bytes of data"):
    val empty = base.copy(payload = Payload.Empty)
    val zero = base.copy(dataContentType = Some(octetType), payload = Payload.Opaque(Binary.empty, octetType))
    assertEquals(
      CloudEventAdapter.toCloudEvent(empty).flatMap(CloudEventAdapter.toEnvelope).map(_.payload),
      Right(Payload.Empty)
    )
    assertEquals(
      CloudEventAdapter.toCloudEvent(zero).flatMap(CloudEventAdapter.toEnvelope).map(_.payload),
      Right(Payload.Opaque(Binary.empty, octetType))
    )

  test("a CloudEvent whose specversion is not 1.0 is rejected as a value"):
    val v03 = io.cloudevents.core.builder.CloudEventBuilder
      .v03()
      .withId("a")
      .withSource(URI("/x"))
      .withType("com.example.a")
      .build()
    assert(CloudEventAdapter.toEnvelope(v03).isLeft)

  test("media-type classification follows the essence, ignoring parameters"):
    assert(CloudEventAdapter.isJsonMediaType(force(ContentType("application/json; charset=utf-8"))))
    assert(CloudEventAdapter.isJsonMediaType(force(ContentType("application/cloudevents+json"))))
    assert(!CloudEventAdapter.isJsonMediaType(force(ContentType("text/plain"))))
    assert(!CloudEventAdapter.isJsonMediaType(force(ContentType("application/octet-stream"))))

  test("extension-name validity matches what the SDK builder enforces"):
    assert(CloudEventAdapter.isValidExtensionName("tenantid7"))
    assert(!CloudEventAdapter.isValidExtensionName(""))
    assert(!CloudEventAdapter.isValidExtensionName("TenantId"))
    assert(!CloudEventAdapter.isValidExtensionName("tenant-id"))
    // The claim above is only worth making if the SDK agrees; ask it directly.
    val envelope = base.copy(extensions = Map("tenant-id" -> AttrValue.Text("acme")))
    intercept[io.cloudevents.rw.CloudEventRWException]:
      io.cloudevents.core.builder.CloudEventBuilder.v1().withExtension("tenant-id", "acme")
    assert(CloudEventAdapter.toCloudEvent(envelope).isLeft)

  private def force[A](result: Either[String, A]): A = WireGenerators.force(result)

  private val jsonType: ContentType = force(ContentType("application/json"))
  private val plainType: ContentType = force(ContentType("text/plain"))
  private val octetType: ContentType = ContentType.OctetStream

  private val base: Envelope =
    Envelope(
      id = force(io.kzonix.kernel.event.EventId("event-1")),
      source = force(io.kzonix.kernel.event.Source("/sensors/kitchen")),
      eventType = force(io.kzonix.kernel.event.EventType("io.kzonix.iot.telemetry")),
      time = Some(OffsetDateTime.of(2024, 1, 1, 17, 31, 0, 0, ZoneOffset.UTC)),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Empty
    )
