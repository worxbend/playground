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

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

import io.circe.Json
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

/** ScalaCheck generators for the CloudEvents model.
  *
  * Two things are generated deliberately rather than incidentally, because they are what the losslessness requirement
  * is about: extension names this build has never heard of (including ones no conformant producer would emit), and
  * payloads nested several levels deep with mixed scalar types. A generator that only produced well-known shapes would
  * pass the round-trip properties while proving nothing.
  *
  * Envelopes come out already canonical (see `Envelope.canonical`), so `decode(encode(e)) == e` is the property under
  * test rather than a statement about normalisation.
  */
object Generators:

  /** Test-only escape hatch for the `Either`-returning smart constructors. Throwing here is correct: a generator that
    * produced an invalid value would be a bug in the generator, not a case under test.
    */
  def force[A](result: Either[String, A]): A =
    result.fold(message => throw IllegalArgumentException(message), identity)

  val genText: Gen[String] =
    Gen
      .listOf(Gen.oneOf(
        Gen.alphaNumChar, Gen.const(' '), Gen.const('-'), Gen.const('/'), Gen.const('ä'),
        Gen.const('中')
      ))
      .map(_.mkString)

  private val genKey: Gen[String] =
    Gen.oneOf(Gen.identifier.map(_.take(12)), Gen.oneOf("deviceId", "roomId", "value", "unit", "nested"))

  /** Depth-bounded so the recursion terminates and so the generated payloads stay comparable to what a device actually
    * emits; the bound is still deep enough that a naive flattening codec would be caught.
    */
  def genJson(depth: Int): Gen[Json] =
    val scalar: Gen[Json] = Gen.oneOf(
      Gen.const(Json.Null),
      Arbitrary.arbitrary[Boolean].map(Json.fromBoolean),
      Arbitrary.arbitrary[Int].map(Json.fromInt),
      Gen.zip(Gen.choose(-1000000L, 1000000L), Gen.choose(0, 4)).map((u, s) => Json.fromBigDecimal(BigDecimal(u, s))),
      genText.map(Json.fromString)
    )
    if depth <= 0 then scalar
    else
      Gen.frequency(
        6 -> scalar,
        2 -> Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, genJson(depth - 1))).map(Json.fromValues),
        2 -> Gen
          .choose(0, 3)
          .flatMap(n => Gen.listOfN(n, Gen.zip(genKey, genJson(depth - 1))))
          .map(Json.fromFields)
      )

  val genEventId: Gen[EventId] =
    Gen.oneOf(Gen.uuid.map(_.toString), Gen.identifier.map(_.take(20))).map(s => force(EventId(s)))

  val genSource: Gen[Source] =
    Gen
      .oneOf(
        Gen.const("/sensors/kitchen"),
        Gen.const("https://home.example/gateway/1"),
        Gen.const("urn:device:abc"),
        Gen.identifier.map(id => s"/gateways/${id.take(10)}")
      )
      .map(s => force(Source(s)))

  val genEventType: Gen[EventType] =
    Gen
      .oneOf(
        Gen.const(EventTypes.Telemetry),
        Gen.const(EventTypes.StateChanged),
        Gen.const(EventTypes.Alarm),
        Gen.identifier.map(id => s"com.example.${id.take(10)}")
      )
      .map(s => force(EventType(s)))

  val genSubject: Gen[Subject] =
    Gen.identifier.map(id => force(Subject(s"device-${id.take(8)}")))

  /** Offsets are whole quarter-hours: RFC 3339 admits only `±HH:MM`, and `OffsetDateTime.toString` would happily render
    * a second-precision offset that no other tool would accept back.
    */
  val genTime: Gen[OffsetDateTime] =
    for
      epochSecond <- Gen.choose(0L, 4102444800L)
      nano        <- Gen.choose(0, 999999999)
      quarters    <- Gen.choose(-56, 56)
    yield OffsetDateTime.ofInstant(
      Instant.ofEpochSecond(epochSecond, nano.toLong),
      ZoneOffset.ofTotalSeconds(quarters * 900)
    )

  val genContentType: Gen[ContentType] =
    Gen
      .oneOf("application/json", "application/cloudevents+json", "text/plain", "image/png", "application/octet-stream")
      .map(s => force(ContentType(s)))

  /** Mixes schema URIs the registry can parse with ones it cannot — the unversioned and relative forms exist to prove
    * that a `dataschema` this build cannot interpret still survives verbatim.
    */
  val genSchemaRef: Gen[SchemaRef] =
    Gen.oneOf(
      for
        name  <- Gen.oneOf("telemetry", "state-changed", "alarm", "unknown")
        major <- Gen.choose(1, 3)
        minor <- Gen.choose(0, 9)
        patch <- Gen.choose(0, 9)
      yield force(SchemaRef.parse(s"https://schemas.kzonix.io/iot/$name/$major.$minor.$patch")),
      Gen.const(force(SchemaRef.parse("urn:example:schema:unversioned"))),
      Gen.const(force(SchemaRef.parse("/local/schema")))
    )

  /** Includes names that violate the spec's own extension-name rule on purpose: they must round-trip anyway. */
  val genExtensionName: Gen[String] =
    Gen.oneOf(
      Gen.oneOf("traceparent", "tracestate", "tenantid", "sequence", "partitionkey"),
      Gen.identifier.map(_.take(20)),
      Gen.oneOf("X-Weird-Name", "unknown_1", "ünïcödé", "a.b.c")
    )

  val genExtensions: Gen[Map[String, AttrValue]] =
    Gen
      .choose(0, 4)
      .flatMap(n => Gen.listOfN(n, Gen.zip(genExtensionName, genJson(2).map(AttrValue.fromJson))))
      .map(_.toMap.filterNot((name, _) => Envelope.ReservedAttributes(name)))

  val genPayload: Gen[Payload] =
    Gen.frequency(
      6 -> genJson(3).map(Payload.Structured.apply),
      2 -> Gen
        .listOf(Arbitrary.arbitrary[Byte])
        .map(bytes => Payload.Opaque(Binary.copyOf(bytes.toArray), ContentType.OctetStream)),
      1 -> Gen.const(Payload.Empty)
    )

  val genEnvelope: Gen[Envelope] =
    for
      id          <- genEventId
      source      <- genSource
      eventType   <- genEventType
      time        <- Gen.option(genTime)
      subject     <- Gen.option(genSubject)
      contentType <- Gen.option(genContentType)
      schema      <- Gen.option(genSchemaRef)
      extensions  <- genExtensions
      payload     <- genPayload
    yield Envelope(id, source, eventType, time, subject, contentType, schema, extensions, payload).canonical

  /** Envelopes the observation registry is expected to recognise, so the totality property is not the only thing
    * exercising `Observation.from`.
    */
  val genKnownEnvelope: Gen[Envelope] =
    for
      base    <- genEnvelope
      subject <- genSubject
      known   <- Gen.oneOf(knownPayloads)
    yield base.copy(
      eventType = force(EventType(known._1)),
      subject = Some(subject),
      schema = None,
      dataContentType = Some(force(ContentType("application/json"))),
      payload = Payload.Structured(known._2)
    )

  private val knownPayloads: Vector[(String, Json)] = Vector(
    EventTypes.Telemetry -> Json.obj(
      "metric" -> Json.fromString("temperature"),
      "value"  -> Json.fromDoubleOrNull(21.5),
      "unit"   -> Json.fromString("celsius")
    ),
    EventTypes.StateChanged -> Json.obj(
      "from" -> Json.fromString("closed"),
      "to"   -> Json.fromString("open")
    ),
    EventTypes.Alarm -> Json.obj(
      "severity" -> Json.fromString("critical"),
      "message"  -> Json.fromString("smoke detected")
    )
  )
