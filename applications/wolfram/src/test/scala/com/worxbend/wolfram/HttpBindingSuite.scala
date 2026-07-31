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
import com.worxbend.kernel.event.Payload
import io.circe.Json
import java.nio.charset.StandardCharsets.UTF_8
import munit.FunSuite

/** The CloudEvents HTTP binding.
  *
  * The property that matters most here is the last one: an event sent in binary mode and the same event sent in
  * structured mode must produce the *same* envelope. If they do not, then which content mode a gateway happens to use
  * changes what ends up in Postgres — a difference that would only ever be noticed by someone comparing two devices'
  * rows months later.
  */
final class HttpBindingSuite extends FunSuite:

  private def bytes(text: String): Array[Byte] = text.getBytes(UTF_8)

  private def json(text: String): Json =
    io.circe.parser.parse(text).fold(failure => fail(failure.message), identity)

  test("mode is decided by ce-specversion, not by the payload's content type"):
    // A binary-mode request whose *payload* is itself a CloudEvents document: deciding on Content-Type would misread
    // this as structured and discard every attribute in the headers.
    val headers = HttpBinding.normalise(Fixtures.binaryHeaders()) + ("content-type" -> HttpBinding.StructuredMediaType)
    assertEquals(HttpBinding.modeOf(headers), Some(ContentMode.Binary))

  test("structured mode is recognised from the media type, parameters and all"):
    val headers = HttpBinding.normalise(Map("Content-Type" -> "application/cloudevents+json; charset=utf-8"))
    assertEquals(HttpBinding.modeOf(headers), Some(ContentMode.Structured))

  test("a request declaring neither mode is malformed, and says so"):
    val result = HttpBinding.decode(Map("content-type" -> "application/json"), bytes("{}"))
    assert(result.left.exists(_.reason == "malformed"), result.toString)
    assert(result.left.exists(_.detail.contains("ce-specversion")), result.toString)

  test("structured mode decodes through kernel's codec"):
    val envelope = HttpBinding
      .decodeStructured(bytes(Fixtures.structuredBody()))
      .fold(rejection => fail(rejection.message), identity)
    assertEquals(envelope.id: String, "evt-1")
    assertEquals(envelope.subject.map(s => s: String), Some("kitchen-thermostat"))
    assertEquals(envelope.payload, Payload.Structured(json("""{"celsius":21.5}""")))

  test("binary mode reads attributes out of ce-* headers, case-insensitively"):
    val envelope = HttpBinding
      .decodeBinary(HttpBinding.normalise(Fixtures.binaryHeaders()), bytes(Fixtures.binaryBody))
      .fold(rejection => fail(rejection.message), identity)
    assertEquals(envelope.id: String, "evt-1")
    assertEquals(envelope.source: String, "/gateway/kitchen")
    assertEquals(envelope.eventType: String, "com.worxbend.iot.telemetry")

  test("a JSON payload lands in `data`; anything else lands in `data_base64`"):
    val json = HttpBinding
      .decodeBinary(HttpBinding.normalise(Fixtures.binaryHeaders()), bytes(Fixtures.binaryBody))
      .fold(rejection => fail(rejection.message), identity)
    assert(json.payload.isJson, json.payload.toString)

    val opaque = HttpBinding
      .decodeBinary(
        HttpBinding.normalise(Fixtures.binaryHeaders()) + ("content-type" -> "application/octet-stream"),
        Array[Byte](3, 1, 4, 1, 5)
      )
      .fold(rejection => fail(rejection.message), identity)
    opaque.payload match
      case Payload.Opaque(value, mediaType) =>
        assertEquals(value.toArray.toVector, Vector[Byte](3, 1, 4, 1, 5))
        assertEquals(mediaType: String, "application/octet-stream")
      case other => fail(s"expected an opaque payload, got $other")

  test("a body declared JSON that is not JSON is malformed, not silently base64'd"):
    val result =
      HttpBinding.decodeBinary(HttpBinding.normalise(Fixtures.binaryHeaders()), bytes("not json at all"))
    assert(result.left.exists(_.reason == "malformed"), result.toString)

  test("unknown ce-* headers become extensions"):
    val headers = HttpBinding.normalise(Fixtures.binaryHeaders()) + ("ce-firmware" -> "3.2.1")
    val envelope = HttpBinding
      .decodeBinary(headers, bytes(Fixtures.binaryBody))
      .fold(rejection => fail(rejection.message), identity)
    assert(envelope.extensions.contains("firmware"), envelope.extensions.toString)

  test("a missing required attribute is invalid-attributes, not malformed — the two are fixed in different places"):
    val headers = HttpBinding.normalise(Fixtures.binaryHeaders()) - "ce-id"
    val result = HttpBinding.decodeBinary(headers, bytes(Fixtures.binaryBody))
    assert(result.left.exists(_.reason == "invalid-attributes"), result.toString)
    assert(result.left.exists(_.detail.contains("id")), result.toString)

  test("a specversion this build does not speak is rejected by name"):
    val headers = HttpBinding.normalise(Fixtures.binaryHeaders()) + ("ce-specversion" -> "0.3")
    val result = HttpBinding.decodeBinary(headers, bytes(Fixtures.binaryBody))
    assert(result.left.exists(_.detail.contains("0.3")), result.toString)

  test("a batch document yields one result per element, good and bad alike"):
    val body = Fixtures.batchBody(Fixtures.structuredBody(id = "a"), """{"specversion":"1.0"}""")
    val results = HttpBinding.decodeBatch(bytes(body)).fold(rejection => fail(rejection.message), identity)
    assertEquals(results.size, 2)
    assert(results(0).isRight, results(0).toString)
    assert(results(1).isLeft, results(1).toString)

  test("a batch that is not a JSON array fails as a whole"):
    assert(HttpBinding.decodeBatch(bytes(Fixtures.structuredBody())).isLeft)
    assert(HttpBinding.decodeBatch(bytes("not json")).isLeft)

  test("the same event in either content mode decodes to the same envelope"):
    val structured = HttpBinding
      .decodeStructured(bytes(Fixtures.structuredBody()))
      .fold(rejection => fail(rejection.message), identity)
    val binary = HttpBinding
      .decodeBinary(HttpBinding.normalise(Fixtures.binaryHeaders()), bytes(Fixtures.binaryBody))
      .fold(rejection => fail(rejection.message), identity)
    assertEquals(binary, structured)
    assertEquals(binary.partitionKey, structured.partitionKey)
