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
import io.circe.Json
import io.circe.parser
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import munit.FunSuite
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import sttp.capabilities.WebSockets
import sttp.client3.*
import sttp.client3.testing.SttpBackendStub
import sttp.model.StatusCode
import sttp.model.Uri
import sttp.tapir.server.stub.TapirStubInterpreter

/** The HTTP contract, driven through `tapir-sttp-stub-server`.
  *
  * The stub interpreter runs the *real* endpoint descriptions and the *real* server logic without binding a port, so
  * these assertions cover exactly what Vert.x would serve: input decoding, the `oneOf` error mapping, and the status
  * codes. What it deliberately does not cover is Vert.x itself — that is the integration test's job, and duplicating it
  * here would only slow the fast tier down.
  */
final class EndpointsSuite extends FunSuite:

  private given ExecutionContext = ExecutionContext.parasitic

  private def bytes(text: String): Array[Byte] = text.getBytes(UTF_8)

  private val single: Uri = uri"http://wolfram.test/events"
  private val batch: Uri = uri"http://wolfram.test/events/batch"

  private def backend(publisher: EventPublisher): SttpBackend[Future, WebSockets] =
    val service = IngestionService(
      publisher,
      TimeClamp.from(Fixtures.ingest),
      Fixtures.ingest,
      IngestMetrics(SimpleMeterRegistry()),
      () => Fixtures.now
    )
    TapirStubInterpreter(SttpBackendStub.asynchronousFuture)
      .whenServerEndpointsRunLogic(IngestApi(service).routes)
      .backend()

  private def post(
    path: Uri,
    headers: Map[String, String],
    body: String,
    publisher: EventPublisher = Fixtures.StubPublisher()
  ): Response[Either[String, String]] =
    val request = headers
      .foldLeft(basicRequest.post(path))((req, header) => req.header(header._1, header._2, true))
      .body(bytes(body))
    Await.result(request.send(backend(publisher)), 5.seconds)

  private def json(response: Response[Either[String, String]]): Json =
    val text = response.body.fold(error => error, identity)
    parser.parse(text).fold(failure => fail(s"response body is not JSON: $text (${failure.message})"), identity)

  private def field(response: Response[Either[String, String]], name: String): Option[String] =
    json(response).hcursor.get[String](name).toOption

  // --- structured mode ---------------------------------------------------------------------------------------------

  test("a structured-mode event is accepted with 202 and a receipt"):
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody())
    assertEquals(response.code, StatusCode.Accepted)
    assertEquals(field(response, "id"), Some("evt-1"))
    assertEquals(field(response, "partitionKey"), Some("/gateway/kitchen#kitchen-thermostat"))
    assertEquals(field(response, "topic"), Some("events.cloudevents.v1"))

  // --- binary mode -------------------------------------------------------------------------------------------------

  test("a binary-mode event is accepted with 202"):
    val response = post(single, Fixtures.binaryHeaders(), Fixtures.binaryBody)
    assertEquals(response.code, StatusCode.Accepted)
    assertEquals(field(response, "id"), Some("evt-1"))

  test("both content modes produce the same receipt for the same event"):
    val structured = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody())
    val binary = post(single, Fixtures.binaryHeaders(), Fixtures.binaryBody)
    assertEquals(json(binary), json(structured))

  // --- rejection paths ---------------------------------------------------------------------------------------------

  test("400 with a body naming the problem when the body is not JSON"):
    val response = post(single, Fixtures.structuredHeaders, "{ not json")
    assertEquals(response.code, StatusCode.BadRequest)
    assertEquals(field(response, "reason"), Some("malformed"))
    assert(field(response, "detail").exists(_.nonEmpty))

  test("400 when a required attribute is missing, naming the attribute"):
    val response = post(single, Fixtures.structuredHeaders, """{"specversion":"1.0","id":"x"}""")
    assertEquals(response.code, StatusCode.BadRequest)
    assertEquals(field(response, "reason"), Some("invalid-attributes"))
    assert(field(response, "detail").exists(_.contains("source")), json(response).noSpaces)

  test("400 when the request declares neither content mode"):
    val response = post(single, Map("content-type" -> "application/json"), "{}")
    assertEquals(response.code, StatusCode.BadRequest)
    assert(field(response, "detail").exists(_.contains("ce-specversion")), json(response).noSpaces)

  test("400 when `time` is outside the plausibility window"):
    val stale = Rfc3339.render(Fixtures.at(Duration.ofDays(-200)))
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody(time = Some(stale)))
    assertEquals(response.code, StatusCode.BadRequest)
    assert(field(response, "detail").exists(_.contains("in the past")), json(response).noSpaces)

  test("413 when the body exceeds the configured ceiling, reporting the limit and the actual size"):
    val response = post(single, Fixtures.structuredHeaders, "x" * (Fixtures.ingest.maxEventBytes.toInt + 1))
    assertEquals(response.code, StatusCode.PayloadTooLarge)
    assertEquals(field(response, "reason"), Some("too-large"))
    assertEquals(json(response).hcursor.get[Long]("limit").toOption, Some(Fixtures.ingest.maxEventBytes))
    assertEquals(json(response).hcursor.get[String]("unit").toOption, Some("bytes"))

  test("503 when the broker will not take the record"):
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody(), Fixtures.unavailable)
    assertEquals(response.code, StatusCode.ServiceUnavailable)
    assertEquals(field(response, "reason"), Some("unpersistable"))

  // --- batch -------------------------------------------------------------------------------------------------------

  test("a clean batch is 202 and reports every element as accepted"):
    val body = Fixtures.batchBody(Fixtures.structuredBody(id = "a"), Fixtures.structuredBody(id = "b"))
    val response = post(batch, Fixtures.batchHeaders, body)
    assertEquals(response.code, StatusCode.Accepted)
    assertEquals(json(response).hcursor.get[Int]("accepted").toOption, Some(2))
    assertEquals(json(response).hcursor.get[Int]("rejected").toOption, Some(0))

  test("a partially bad batch is 207, not 4xx — the accepted events are durable and must not be resent"):
    val body = Fixtures.batchBody(
      Fixtures.structuredBody(id = "a"),
      """{"specversion":"1.0","id":"b"}""",
      Fixtures.structuredBody(id = "c")
    )
    val response = post(batch, Fixtures.batchHeaders, body)
    assertEquals(response.code, StatusCode.MultiStatus)

    val report = json(response)
    assertEquals(report.hcursor.get[Int]("accepted").toOption, Some(2))
    assertEquals(report.hcursor.get[Int]("rejected").toOption, Some(1))

    val entries = report.hcursor.downField("entries").values.getOrElse(fail("entries should be an array")).toVector
    assertEquals(entries.size, 3)
    assertEquals(entries(1).hcursor.get[Int]("index").toOption, Some(1))
    assertEquals(entries(1).hcursor.get[Boolean]("accepted").toOption, Some(false))
    assertEquals(entries(1).hcursor.get[String]("reason").toOption, Some("invalid-attributes"))
    assertEquals(entries(2).hcursor.get[Boolean]("accepted").toOption, Some(true))

  test("a batch sent without the batch media type is refused rather than guessed at"):
    val body = Fixtures.batchBody(Fixtures.structuredBody())
    val response = post(batch, Fixtures.structuredHeaders, body)
    assertEquals(response.code, StatusCode.BadRequest)
    assert(field(response, "detail").exists(_.contains(HttpBinding.BatchMediaType)), json(response).noSpaces)

  test("a batch document that is not an array fails as a whole with 400"):
    val response = post(batch, Fixtures.batchHeaders, Fixtures.structuredBody())
    assertEquals(response.code, StatusCode.BadRequest)

  test("an oversize batch is 413"):
    val body = Fixtures.batchBody(Seq.fill(Fixtures.ingest.maxBatchEvents + 1)(Fixtures.structuredBody())*)
    val response = post(batch, Fixtures.batchHeaders, body)
    assertEquals(response.code, StatusCode.PayloadTooLarge)
    assertEquals(json(response).hcursor.get[String]("unit").toOption, Some("events"))

  // --- the description and the implementation agree ----------------------------------------------------------------

  test("every failure type is served with the status the endpoint description advertises"):
    val cases: List[(IngestFailure, Int)] = List(
      InvalidEvent("malformed", "x") -> 400,
      OversizeEvent("too-large", "x", 1L, 2L, "bytes") -> 413,
      ServiceUnavailable("unpersistable", "x") -> 503
    )
    cases.foreach: (failure, expected) =>
      assertEquals(ApiModel.status(failure).code, expected, failure.toString)
