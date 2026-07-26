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

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

import io.circe.Json
import io.kzonix.kernel.event.AttrValue
import io.kzonix.kernel.event.Binary
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.EventId
import io.kzonix.kernel.event.EventType
import io.kzonix.kernel.event.EventTypes
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.SchemaRef
import io.kzonix.kernel.event.Source
import io.kzonix.kernel.event.Subject
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

/** ScalaCheck generators for the wire round trips.
  *
  * These deliberately mirror `io.kzonix.kernel.event.Generators` rather than importing it: kernel's test artifact is
  * not published to this module's classpath (`eventing` depends on `kernel`'s main jar only), so reusing them would
  * need a build change this module is not allowed to make. The shapes are kept aligned on purpose — where they differ,
  * the difference is a wire constraint the kernel does not have, and each one is commented.
  *
  * The one systematic difference: **extension names here are spec-valid by default**. CloudEvents restricts them to
  * lowercase letters and digits, and the SDK enforces it. Names the spec forbids are generated separately by
  * [[genInvalidExtensionName]], because "an unrepresentable name is rejected as a value, not thrown" is itself a
  * property worth proving rather than an accident that would make every other property vacuous.
  */
object WireGenerators:

  /** Test-only escape hatch for the `Either`-returning smart constructors. Throwing is correct here: a generator that
    * produced an invalid value would be a bug in the generator, not a case under test.
    */
  def force[A](result: Either[String, A]): A =
    result.fold(message => throw IllegalArgumentException(message), identity)

  val genText: Gen[String] =
    Gen
      .listOf(Gen.oneOf(Gen.alphaNumChar, Gen.const(' '), Gen.const('-'), Gen.const('/'), Gen.const('ä'), Gen.const('中')))
      .map(_.mkString)

  private val genKey: Gen[String] =
    Gen.oneOf(Gen.identifier.map(_.take(12)), Gen.oneOf("deviceId", "roomId", "value", "unit", "nested"))

  /** Depth-bounded, and deep enough that a codec which flattened nested objects would be caught. */
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

  /** Whole quarter-hour offsets: RFC 3339 admits only `±HH:MM`, and a second-precision offset is a value no other
    * tool would accept back.
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

  /** Mixes JSON and non-JSON media types, because in a byte-oriented binding the media type is the *only* thing that
    * decides whether the payload comes back as `Structured` or `Opaque`.
    */
  val genContentType: Gen[ContentType] =
    Gen
      .oneOf("application/json", "application/cloudevents+json", "text/plain", "image/png", "application/octet-stream")
      .map(s => force(ContentType(s)))

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

  /** Lowercase alphanumerics only — everything the CloudEvents spec allows as an extension name, and nothing else. */
  val genExtensionName: Gen[String] =
    Gen.oneOf(
      Gen.oneOf("traceparent", "tracestate", "tenantid", "sequence", "partitionkey", "sampled"),
      Gen.nonEmptyListOf(Gen.oneOf(Gen.alphaLowerChar, Gen.numChar)).map(_.take(20).mkString)
    )

  /** Names the spec forbids, kept apart so the rejection path has its own property. */
  val genInvalidExtensionName: Gen[String] =
    Gen.oneOf("X-Weird-Name", "unknown_1", "ünïcödé", "a.b.c", "Sequence", "trace-parent")

  /** Every `AttrValue` shape, including the three the *structured* format keeps typed and binary mode cannot. */
  val genAttrValue: Gen[AttrValue] =
    Gen.oneOf(
      genText.map(AttrValue.Text.apply),
      Arbitrary.arbitrary[Int].map(AttrValue.Num.apply),
      Arbitrary.arbitrary[Boolean].map(AttrValue.Flag.apply),
      genTime.map(AttrValue.Time.apply),
      Gen.oneOf("urn:x:1", "https://example.test/a", "/relative").map(s => AttrValue.Ref(java.net.URI(s))),
      Gen.listOf(Arbitrary.arbitrary[Byte]).map(bytes => AttrValue.Bytes(Binary.copyOf(bytes.toArray))),
      genJson(1).map(AttrValue.Other.apply)
    )

  val genExtensions: Gen[Map[String, AttrValue]] =
    Gen
      .choose(0, 4)
      .flatMap(n => Gen.listOfN(n, Gen.zip(genExtensionName, genAttrValue)))
      .map(_.toMap.filterNot((name, _) => Envelope.ReservedAttributes(name)))

  val genPayload: Gen[Payload] =
    Gen.frequency(
      6 -> genJson(3).map(Payload.Structured.apply),
      2 -> Gen
        .listOf(Arbitrary.arbitrary[Byte])
        .map(bytes => Payload.Opaque(Binary.copyOf(bytes.toArray), ContentType.OctetStream)),
      1 -> Gen.const(Payload.Empty)
    )

  /** Envelopes exactly as constructed, with no normalisation applied.
    *
    * Used for the SDK adapter properties, and the distinction matters: `Envelope.canonical` collapses `Time`, `Ref` and
    * `Bytes` extensions into `Text`, because kernel's *JSON* format carries no per-extension type. The SDK's extension
    * model does carry it, so canonicalising first would quietly make "typed extensions survive the adapter"
    * untestable — the very property the `AttrValue` ADT exists for.
    */
  val genRawEnvelope: Gen[Envelope] =
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
    yield Envelope(id, source, eventType, time, subject, contentType, schema, extensions, payload)

  /** Envelopes already in `Envelope.canonical` form, so kernel's own normalisation is never what a structured-mode
    * property is measuring — only the wire's own is.
    */
  val genEnvelope: Gen[Envelope] = genRawEnvelope.map(_.canonical)

  /** An envelope carrying at least one extension name the spec forbids. */
  val genUnrepresentableEnvelope: Gen[Envelope] =
    for
      envelope <- genRawEnvelope
      name     <- genInvalidExtensionName
      value    <- genAttrValue
    yield envelope.copy(extensions = envelope.extensions + (name -> value))

  val genDecodeFailure: Gen[DecodeFailure] =
    for
      detail  <- genText.map(t => if t.isEmpty then "no detail" else t)
      failure <- Gen.oneOf(
                   DecodeFailure.UnknownEncoding(detail),
                   DecodeFailure.MalformedStructured(detail),
                   DecodeFailure.MalformedBinary(detail),
                   DecodeFailure.Unconvertible(detail)
                 )
    yield failure

  val genRecordOrigin: Gen[RecordOrigin] =
    for
      topic     <- Gen.oneOf("events.cloudevents.v1", "events.cloudevents.v1.dlq", "other.topic")
      partition <- Gen.choose(0, 11)
      offset    <- Gen.choose(0L, 9000000000L)
      timestamp <- Gen.option(Gen.choose(0L, 4102444800000L))
      key       <- Gen.option(genText)
    yield RecordOrigin(topic, partition, offset, timestamp, key)

  val genDeadLetter: Gen[DeadLetter] =
    for
      origin  <- genRecordOrigin
      failure <- genDecodeFailure
      failedAt <- genTime
      headers <- Gen
                   .choose(0, 4)
                   .flatMap(n => Gen.listOfN(n, Gen.zip(Gen.identifier.map(_.take(10)), genText)))
                   .map(_.toMap)
      payload <- Gen.option(Gen.listOf(Arbitrary.arbitrary[Byte]).map(bytes => Binary.copyOf(bytes.toArray)))
      source  <- genSource
    yield DeadLetter(origin, failure.reason, failure.detail, failedAt, headers, payload, source)
