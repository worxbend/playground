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

package io.kzonix.kernel.event

import io.circe.Json
import org.scalacheck.Prop.forAll
import scala.util.Try

/** Refinement into the known-type ADT.
  *
  * Totality is the property that matters. Everything downstream — the insert, the search projection, the detail view —
  * is written on the assumption that `from` always answers, so a single throwing path here would take out a whole
  * consumer batch on the day a new device ships.
  */
final class ObservationSuite extends munit.ScalaCheckSuite:

  override def scalaCheckTestParameters = super.scalaCheckTestParameters.withMinSuccessfulTests(300)

  property("derivation is total: no generated envelope throws"):
    forAll(Generators.genEnvelope): envelope =>
      Try(Observation.from(envelope)).isSuccess

  property("derivation is total for recognised types too"):
    forAll(Generators.genKnownEnvelope): envelope =>
      Try(Observation.from(envelope)).isSuccess

  property("derivation is deterministic"):
    forAll(Generators.genEnvelope): envelope =>
      Observation.from(envelope) == Observation.from(envelope)

  property("the original envelope is never lost"):
    forAll(Generators.genEnvelope): envelope =>
      val observed = Observed.of(envelope)
      observed.envelope == envelope && observed.observation == Observation.from(envelope)

  property("an unrecognised event keeps its type and payload verbatim"):
    forAll(Generators.genEnvelope): envelope =>
      Observation.from(envelope) match
        case Observation.Unrecognised(eventType, payload, _) =>
          (eventType: String) == (envelope.eventType: String) && payload == envelope.payload
        case _ => true

  private def envelopeOf(eventType: String, subject: Option[String], data: Json): Envelope =
    Envelope(
      id = Generators.force(EventId("e-1")),
      source = Generators.force(Source("/gateways/1")),
      eventType = Generators.force(EventType(eventType)),
      time = None,
      subject = subject.map(s => Generators.force(Subject(s))),
      dataContentType = Some(Generators.force(ContentType("application/json"))),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(data)
    )

  test("telemetry refines with the subject as the device"):
    val envelope = envelopeOf(
      EventTypes.Telemetry,
      Some("thermo-1"),
      Json.obj(
        "metric" -> Json.fromString("temperature"),
        "value" -> Json.fromDoubleOrNull(21.5),
        "unit" -> Json.fromString("celsius")
      )
    )
    assertEquals(
      Observation.from(envelope),
      Observation.Telemetry(Generators.force(Subject("thermo-1")), "temperature", 21.5, "celsius")
    )

  test("state changes refine"):
    val envelope = envelopeOf(
      EventTypes.StateChanged,
      Some("door-3"),
      Json.obj("from" -> Json.fromString("closed"), "to" -> Json.fromString("open"))
    )
    assertEquals(
      Observation.from(envelope),
      Observation.StateChanged(Generators.force(Subject("door-3")), "closed", "open")
    )

  test("alarm severity is ranked identically whether the producer sends text or a number"):
    val asText = envelopeOf(
      EventTypes.Alarm,
      Some("smoke-1"),
      Json.obj("severity" -> Json.fromString("Critical"), "message" -> Json.fromString("smoke"))
    )
    val asNumber = envelopeOf(
      EventTypes.Alarm,
      Some("smoke-1"),
      Json.obj("severity" -> Json.fromInt(60), "message" -> Json.fromString("smoke"))
    )
    assertEquals(Observation.from(asText), Observation.from(asNumber))
    assertEquals(
      Observation.from(asText),
      Observation.Alarm(Generators.force(Subject("smoke-1")), 60, "smoke")
    )

  test("the device falls back to data.deviceId when subject is absent"):
    val envelope = envelopeOf(
      EventTypes.StateChanged,
      None,
      Json.obj(
        "deviceId" -> Json.fromString("door-9"),
        "from" -> Json.fromString("locked"),
        "to" -> Json.fromString("unlocked")
      )
    )
    assertEquals(
      Observation.from(envelope),
      Observation.StateChanged(Generators.force(Subject("door-9")), "locked", "unlocked")
    )

  test("a known type with an unusable payload becomes Unrecognised with a reason, not an exception"):
    val envelope = envelopeOf(EventTypes.Telemetry, Some("thermo-1"), Json.obj("metric" -> Json.fromString("temp")))
    Observation.from(envelope) match
      case Observation.Unrecognised(eventType, payload, reason) =>
        assertEquals(eventType: String, EventTypes.Telemetry)
        assertEquals(payload, envelope.payload)
        assert(reason.isDefined, "a known type that failed to decode must explain itself")
      case other => fail(s"expected Unrecognised, got $other")

  test("a known type with no device identity at all is Unrecognised"):
    val envelope = envelopeOf(EventTypes.Alarm, None, Json.obj("message" -> Json.fromString("boom")))
    assert(Observation.from(envelope).isInstanceOf[Observation.Unrecognised])

  test("an unknown type is Unrecognised without a reason — a new device is not a failure"):
    val envelope = envelopeOf("com.example.brandnew", Some("x-1"), Json.obj())
    assertEquals(
      Observation.from(envelope),
      Observation.Unrecognised(Generators.force(EventType("com.example.brandnew")), envelope.payload, None)
    )

  test("a binary payload under a known type does not decode but does not throw"):
    val envelope = envelopeOf(EventTypes.Alarm, Some("x-1"), Json.obj())
      .copy(payload = Payload.Opaque(Binary.copyOf(Array[Byte](1, 2, 3)), ContentType.OctetStream))
    assert(Observation.from(envelope).isInstanceOf[Observation.Unrecognised])

  private val telemetryData = Json.obj(
    "metric" -> Json.fromString("temperature"),
    "value" -> Json.fromDoubleOrNull(21.5),
    "unit" -> Json.fromString("celsius")
  )

  test("a registered major still decodes when the minor and patch move"):
    val envelope = envelopeOf(EventTypes.Telemetry, Some("thermo-1"), telemetryData)
      .copy(schema = SchemaRef.parse("https://schemas.kzonix.io/iot/telemetry/1.7.3").toOption)
    assertEquals(
      Observation.from(envelope),
      Observation.Telemetry(Generators.force(Subject("thermo-1")), "temperature", 21.5, "celsius")
    )

  test("an unregistered major is Unrecognised rather than decoded by an incompatible older decoder"):
    val envelope = envelopeOf(EventTypes.Telemetry, Some("thermo-1"), telemetryData)
      .copy(schema = SchemaRef.parse("https://schemas.kzonix.io/iot/telemetry/9.0.0").toOption)
    assertEquals(
      Observation.from(envelope),
      Observation.Unrecognised(Generators.force(EventType(EventTypes.Telemetry)), envelope.payload, None)
    )

  test("the registry advertises exactly the types it can decode"):
    assertEquals(
      Observation.knownTypes,
      Set(
        (EventTypes.Telemetry, Observation.DefaultMajor),
        (EventTypes.StateChanged, Observation.DefaultMajor),
        (EventTypes.Alarm, Observation.DefaultMajor)
      )
    )
