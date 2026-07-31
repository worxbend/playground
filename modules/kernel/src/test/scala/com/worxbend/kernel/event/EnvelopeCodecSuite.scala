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
import io.circe.parser
import org.scalacheck.Prop.forAll

/** The CloudEvents JSON Format 1.0 codec.
  *
  * The properties here are the contract every other module leans on: if the codec is not an identity, cobalt's
  * `raw jsonb` is not the event wolfram received, and the "explainable five years later" promise of ADR §4.2 is void.
  */
final class EnvelopeCodecSuite extends munit.ScalaCheckSuite:

  override def scalaCheckTestParameters = super.scalaCheckTestParameters.withMinSuccessfulTests(300)

  property("decode after encode is the identity on envelopes"):
    forAll(Generators.genEnvelope): envelope =>
      Envelope.decoder.decodeJson(envelope.toJson) == Right(envelope)

  property("encode after decode is the identity on JSON, unknown fields included"):
    forAll(Generators.genEnvelope): envelope =>
      val json = envelope.toJson
      Envelope.decoder.decodeJson(json).map(_.toJson) == Right(json)

  property("the round-trip survives printing and re-parsing"):
    forAll(Generators.genEnvelope): envelope =>
      Envelope.parse(envelope.render) == Right(envelope)

  property("unknown extension attributes survive verbatim"):
    forAll(Generators.genEnvelope): envelope =>
      Envelope.parse(envelope.render).map(_.extensions) == Right(envelope.extensions)

  property("unknown payload shapes survive verbatim"):
    forAll(Generators.genEnvelope): envelope =>
      Envelope.parse(envelope.render).map(_.payload) == Right(envelope.payload)

  property("canonical is idempotent"):
    forAll(Generators.genEnvelope): envelope =>
      envelope.canonical.canonical == envelope.canonical

  /** Verbatim from the CloudEvents 1.0 JSON Format specification, extensions and all. */
  private val specExample =
    """{
      |  "specversion" : "1.0",
      |  "type" : "com.example.someevent",
      |  "source" : "/mycontext",
      |  "id" : "C234-1234-1234",
      |  "time" : "2018-04-05T17:31:00Z",
      |  "comexampleextension1" : "value",
      |  "comexampleothervalue" : 5,
      |  "datacontenttype" : "application/json",
      |  "data" : { "appinfoA" : "abc", "appinfoB" : 123, "appinfoC" : true }
      |}""".stripMargin

  test("the specification's structured-mode example round-trips exactly"):
    val json = parser.parse(specExample).toOption.get
    assertEquals(Envelope.decoder.decodeJson(json).map(_.toJson), Right(json))

  test("extensions keep their CloudEvents type through the codec"):
    val envelope = Envelope.parse(specExample).toOption.get
    assertEquals(envelope.extensions("comexampleextension1"), AttrValue.Text("value"))
    assertEquals(envelope.extensions("comexampleothervalue"), AttrValue.Num(5))

  test("context attributes are never mistaken for extensions"):
    val envelope = Envelope.parse(specExample).toOption.get
    assertEquals(envelope.extensions.keySet, Set("comexampleextension1", "comexampleothervalue"))
    assertEquals(envelope.eventType: String, "com.example.someevent")
    assertEquals(envelope.dataContentType.map(ct => ct: String), Some("application/json"))

  test("a reserved attribute placed in extensions is dropped rather than allowed to collide"):
    val envelope = Envelope.parse(specExample).toOption.get
    val tampered = envelope.copy(extensions = envelope.extensions + ("id" -> AttrValue.Text("hijacked")))
    assertEquals(tampered.toJson.hcursor.get[String]("id"), Right("C234-1234-1234"))

  test("binary payloads use data_base64 and come back byte-identical"):
    val bytes = Binary.copyOf(Array[Byte](0, 1, 2, -1, -2, 127, -128))
    val envelope = Envelope(
      id = Generators.force(EventId("b-1")),
      source = Generators.force(Source("/sensors/cam")),
      eventType = Generators.force(EventType("com.example.frame")),
      time = None,
      subject = None,
      dataContentType = Some(Generators.force(ContentType("image/png"))),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Opaque(bytes, Generators.force(ContentType("image/png")))
    )
    assert(envelope.toJson.hcursor.get[String]("data_base64").isRight)
    assertEquals(Envelope.parse(envelope.render), Right(envelope))

  test("data_base64 without a datacontenttype defaults to octet-stream rather than null"):
    val text = """{"specversion":"1.0","id":"x","source":"/s","type":"t","data_base64":"AAEC"}"""
    assertEquals(Envelope.parse(text).map(_.payload.isJson), Right(false))
    assertEquals(Envelope.parse(text).map(_.dataContentType), Right(None))
    assertEquals(Envelope.parse(text).map(_.render), Right(text))

  test("an explicit JSON null payload is preserved, not collapsed to an absent one"):
    val text = """{"specversion":"1.0","id":"x","source":"/s","type":"t","data":null}"""
    assertEquals(Envelope.parse(text).map(_.payload), Right(Payload.Structured(Json.Null)))
    assertEquals(Envelope.parse(text).map(_.render), Right(text))

  test("a missing required attribute is rejected"):
    assert(Envelope.parse("""{"specversion":"1.0","source":"/s","type":"t"}""").isLeft)

  test("a foreign specversion is rejected rather than silently reinterpreted"):
    assert(Envelope.parse("""{"specversion":"0.3","id":"x","source":"/s","type":"t"}""").isLeft)

  test("data and data_base64 together is rejected"):
    val text = """{"specversion":"1.0","id":"x","source":"/s","type":"t","data":{},"data_base64":"AAEC"}"""
    assert(Envelope.parse(text).isLeft)

  test("a non-object is rejected"):
    assert(Envelope.parse("[]").isLeft)
    assert(Envelope.parse("not json at all").isLeft)

  test("an attribute of the wrong JSON type is rejected"):
    assert(Envelope.parse("""{"specversion":"1.0","id":7,"source":"/s","type":"t"}""").isLeft)

  test("an explicit null optional attribute is read as absent"):
    val text = """{"specversion":"1.0","id":"x","source":"/s","type":"t","subject":null}"""
    assertEquals(Envelope.parse(text).map(_.subject), Right(None))
