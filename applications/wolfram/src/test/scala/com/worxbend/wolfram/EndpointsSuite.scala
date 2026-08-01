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

  private val single: Uri = uri"http://wolfram.test/v1/events"
  private val batch: Uri = uri"http://wolfram.test/v1/events:batchCreate"
  private val validate: Uri = uri"http://wolfram.test/v1/events:validate"

  private def backend(publisher: EventPublisher): SttpBackend[Future, WebSockets] =
    val service = IngestionService(
      publisher,
      TimeClamp.from(Fixtures.ingest),
      Fixtures.ingest,
      IngestMetrics(SimpleMeterRegistry()),
      () => Fixtures.now
    )
    TapirStubInterpreter(SttpBackendStub.asynchronousFuture)
      .whenServerEndpointsRunLogic(IngestApi(service, Tokens.verifier).routes)
      .backend()

  private def post(
    path: Uri,
    headers: Map[String, String],
    body: String,
    publisher: EventPublisher = Fixtures.StubPublisher()
  ): Response[Either[String, String]] =
    // Every request carries a valid publish token. The authenticated surface is the only surface, so a suite about
    // *content* has to get past the credential check to reach anything — the credential check itself is AuthSuite's.
    val request = (headers ++ Tokens.bearerHeader)
      .foldLeft(basicRequest.post(path))((req, header) => req.header(header._1, header._2, true))
      .body(bytes(body))
    Await.result(request.send(backend(publisher)), 5.seconds)

  /** Accessors for the AIP-193 envelope. Written once because every failure has the same shape, which is the whole
    * point of the envelope — a test that reached into a different place per status would be evidence it was not.
    */
  private def reason(response: Response[Either[String, String]]): Option[String] =
    json(response).hcursor.downField("error").downField("details").downArray.get[String]("reason").toOption

  private def message(response: Response[Either[String, String]]): Option[String] =
    json(response).hcursor.downField("error").get[String]("message").toOption

  private def status(response: Response[Either[String, String]]): Option[String] =
    json(response).hcursor.downField("error").get[String]("status").toOption

  private def metadata(response: Response[Either[String, String]], key: String): Option[String] =
    json(response).hcursor
      .downField("error")
      .downField("details")
      .downArray
      .downField("metadata")
      .get[String](key)
      .toOption

  private def json(response: Response[Either[String, String]]): Json =
    val text = response.body.fold(error => error, identity)
    parser.parse(text).fold(failure => fail(s"response body is not JSON: $text (${failure.message})"), identity)

  private def field(response: Response[Either[String, String]], name: String): Option[String] =
    json(response).hcursor.get[String](name).toOption

  // --- structured mode ---------------------------------------------------------------------------------------------

  test("a structured-mode event is created, and the response is the resource"):
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody())
    assertEquals(response.code, StatusCode.Ok)
    assertEquals(field(response, "id"), Some("evt-1"))
    assertEquals(field(response, "partitionKey"), Some("/gateway/kitchen#kitchen-thermostat"))
    // `topic` moved under `destination` (AIP-144): it describes where the event was stored, not the event.
    assertEquals(
      json(response).hcursor.downField("destination").get[String]("topic").toOption,
      Some("events.cloudevents.v1")
    )

  // --- binary mode -------------------------------------------------------------------------------------------------

  test("a binary-mode event is created"):
    val response = post(single, Fixtures.binaryHeaders(), Fixtures.binaryBody)
    assertEquals(response.code, StatusCode.Ok)
    assertEquals(field(response, "id"), Some("evt-1"))

  test("both content modes produce the same resource for the same event"):
    val structured = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody())
    val binary = post(single, Fixtures.binaryHeaders(), Fixtures.binaryBody)
    assertEquals(json(binary), json(structured))

  // --- rejection paths ---------------------------------------------------------------------------------------------

  test("400 with a body naming the problem when the body is not JSON"):
    val response = post(single, Fixtures.structuredHeaders, "{ not json")
    assertEquals(response.code, StatusCode.BadRequest)
    assertEquals(reason(response), Some("malformed"))
    assert(message(response).exists(_.nonEmpty))

  test("400 when a required attribute is missing, naming the attribute"):
    val response = post(single, Fixtures.structuredHeaders, """{"specversion":"1.0","id":"x"}""")
    assertEquals(response.code, StatusCode.BadRequest)
    assertEquals(reason(response), Some("invalid-attributes"))
    assert(message(response).exists(_.contains("source")), json(response).noSpaces)

  test("400 when the request declares neither content mode"):
    val response = post(single, Map("content-type" -> "application/json"), "{}")
    assertEquals(response.code, StatusCode.BadRequest)
    assert(message(response).exists(_.contains("ce-specversion")), json(response).noSpaces)

  test("400 when `time` is outside the plausibility window"):
    val stale = Rfc3339.render(Fixtures.at(Duration.ofDays(-200)))
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody(time = Some(stale)))
    assertEquals(response.code, StatusCode.BadRequest)
    assert(message(response).exists(_.contains("in the past")), json(response).noSpaces)
    // The canonical status is what a client branches on, and it is finer than the HTTP code.
    assertEquals(status(response), Some("OUT_OF_RANGE"))

  test("413 when the body exceeds the configured ceiling, reporting the limit and the actual size"):
    val response = post(single, Fixtures.structuredHeaders, "x" * (Fixtures.ingest.maxEventBytes.toInt + 1))
    assertEquals(response.code, StatusCode.PayloadTooLarge)
    assertEquals(reason(response), Some("too-large"))
    // The numbers live in `details[0].metadata` so a client need not parse the sentence to find them.
    assertEquals(metadata(response, "limit"), Some(Fixtures.ingest.maxEventBytes.toString))
    assertEquals(metadata(response, "unit"), Some("bytes"))

  test("503 when the broker will not take the record"):
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody(), Fixtures.unavailable)
    assertEquals(response.code, StatusCode.ServiceUnavailable)
    assertEquals(reason(response), Some("unpersistable"))
    assertEquals(status(response), Some("UNAVAILABLE"))

  // --- batch -------------------------------------------------------------------------------------------------------

  test("a clean batch is 200 and returns the created events"):
    val body = Fixtures.batchBody(Fixtures.structuredBody(id = "a"), Fixtures.structuredBody(id = "b"))
    val response = post(batch, Fixtures.batchHeaders, body)
    assertEquals(response.code, StatusCode.Ok)
    assertEquals(json(response).hcursor.get[Int]("created").toOption, Some(2))
    assertEquals(json(response).hcursor.get[Int]("failed").toOption, Some(0))
    // AIP-233's response shape: the created resources, in request order.
    assertEquals(json(response).hcursor.downField("events").values.map(_.size), Some(2))

  test("a partially bad batch is 207, not 4xx — the accepted events are durable and must not be resent"):
    val body = Fixtures.batchBody(
      Fixtures.structuredBody(id = "a"),
      """{"specversion":"1.0","id":"b"}""",
      Fixtures.structuredBody(id = "c")
    )
    val response = post(batch, Fixtures.batchHeaders, body)
    assertEquals(response.code, StatusCode.MultiStatus)

    val report = json(response)
    assertEquals(report.hcursor.get[Int]("created").toOption, Some(2))
    assertEquals(report.hcursor.get[Int]("failed").toOption, Some(1))
    // `events` holds only the successes; `entries` holds every element, so an index is still resolvable.
    assertEquals(report.hcursor.downField("events").values.map(_.size), Some(2))

    val entries = report.hcursor.downField("entries").values.getOrElse(fail("entries should be an array")).toVector
    assertEquals(entries.size, 3)
    assertEquals(entries(1).hcursor.get[Int]("index").toOption, Some(1))
    assertEquals(entries(1).hcursor.downField("event").focus.map(_.isNull), Some(true))
    assertEquals(
      entries(1).hcursor.downField("error").downField("details").downArray.get[String]("reason").toOption,
      Some("invalid-attributes")
    )
    assert(entries(2).hcursor.downField("event").focus.exists(!_.isNull))

  test("a batch sent without the batch media type is refused rather than guessed at"):
    val body = Fixtures.batchBody(Fixtures.structuredBody())
    val response = post(batch, Fixtures.structuredHeaders, body)
    assertEquals(response.code, StatusCode.BadRequest)
    assert(message(response).exists(_.contains(HttpBinding.BatchMediaType)), json(response).noSpaces)

  test("a batch document that is not an array fails as a whole with 400"):
    val response = post(batch, Fixtures.batchHeaders, Fixtures.structuredBody())
    assertEquals(response.code, StatusCode.BadRequest)

  test("an oversize batch is 413"):
    val body = Fixtures.batchBody(Seq.fill(Fixtures.ingest.maxBatchEvents + 1)(Fixtures.structuredBody())*)
    val response = post(batch, Fixtures.batchHeaders, body)
    assertEquals(response.code, StatusCode.PayloadTooLarge)
    assertEquals(metadata(response, "unit"), Some("events"))

  // --- the description and the implementation agree ----------------------------------------------------------------

  test("every failure type is served with the status the endpoint description advertises"):
    def body(code: Int) = ErrorBody(code, "x", "STATUS", Nil)
    val cases: List[(ApiError, Int)] = List(
      InvalidArgument(body(400)) -> 400,
      OutOfRange(body(400)) -> 400,
      PayloadTooLarge(body(413)) -> 413,
      Unauthorized(body(401)) -> 401,
      Forbidden(body(403)) -> 403,
      Unavailable(body(503)) -> 503
    )
    cases.foreach: (failure, expected) =>
      assertEquals(ApiModel.status(failure).code, expected, failure.toString)

  // --- the custom methods (AIP-136) --------------------------------------------------------------------------------

  test("events:validate answers 200 with valid=true and publishes nothing"):
    val publisher = Fixtures.StubPublisher()
    val response = post(validate, Fixtures.structuredHeaders, Fixtures.structuredBody(), publisher)
    assertEquals(response.code, StatusCode.Ok)
    assertEquals(json(response).hcursor.get[Boolean]("valid").toOption, Some(true))
    assertEquals(publisher.published.size, 0, "validate must not publish")

  test("a rejected event is a successful validation, not a failed request"):
    // The question was "would you accept this". "No, and here is why" is an answer, so it is a 200 — a 400 would
    // make a client's error handling indistinguishable from its own request being malformed.
    val stale = Rfc3339.render(Fixtures.at(Duration.ofDays(-200)))
    val response = post(validate, Fixtures.structuredHeaders, Fixtures.structuredBody(time = Some(stale)))
    assertEquals(response.code, StatusCode.Ok)
    val cursor = json(response).hcursor
    assertEquals(cursor.get[Boolean]("valid").toOption, Some(false))
    assertEquals(cursor.downField("error").get[String]("status").toOption, Some("OUT_OF_RANGE"))

  test("an oversize body is still a 4xx on validate — it is refused before it can be judged"):
    val padding = "x" * (Fixtures.ingest.maxEventBytes.toInt + 1)
    val response = post(validate, Fixtures.structuredHeaders, padding)
    assertEquals(response.code, StatusCode.PayloadTooLarge)

  test("validate agrees with create, because both call the same pure function"):
    val body = Fixtures.structuredBody()
    val validated = json(post(validate, Fixtures.structuredHeaders, body)).hcursor
    val created = json(post(single, Fixtures.structuredHeaders, body)).hcursor
    assertEquals(validated.downField("event").get[String]("name").toOption, created.get[String]("name").toOption)

  // --- AIP-193 error envelope --------------------------------------------------------------------------------------

  test("every failure renders the same envelope, whatever the status"):
    val cases = List(
      post(single, Fixtures.structuredHeaders, "{ not json"),
      post(single, Fixtures.structuredHeaders, """{"specversion":"1.0","id":"x"}"""),
      post(single, Fixtures.structuredHeaders, "x" * (Fixtures.ingest.maxEventBytes.toInt + 1))
    )
    cases.foreach: response =>
      val error = json(response).hcursor.downField("error")
      assert(error.get[Int]("code").toOption.contains(response.code.code), response.body.toString)
      assert(error.get[String]("status").toOption.exists(_.nonEmpty), response.body.toString)
      assert(error.get[String]("message").toOption.exists(_.nonEmpty), response.body.toString)
      // `reason` is the same closed vocabulary as the Prometheus tag, which is what lets a client's error handling
      // and an operator's dashboard use the same word for the same condition.
      val reason = error.downField("details").downArray.get[String]("reason").toOption
      assert(reason.exists(_.nonEmpty), response.body.toString)

  test("a created event carries its AIP-122 resource name"):
    val response = post(single, Fixtures.structuredHeaders, Fixtures.structuredBody())
    assertEquals(response.code, StatusCode.Ok)
    val name = json(response).hcursor.get[String]("name").toOption.getOrElse(fail("no name"))
    assert(name.startsWith("events/"), name)
    // Percent-encoded, so a `source` containing slashes cannot fabricate extra path segments.
    assert(!name.stripPrefix("events/").contains("/"), name)

  // --- authentication is on every route ----------------------------------------------------------------------------

  test("no token is 401 with the AIP-193 envelope, on every operation"):
    List(single, batch, validate).foreach: path =>
      val response = Await.result(
        basicRequest.post(path).body(bytes(Fixtures.structuredBody())).send(backend(Fixtures.StubPublisher())),
        5.seconds
      )
      assertEquals(response.code, StatusCode.Unauthorized, path.toString)
      assertEquals(json(response).hcursor.downField("error").get[String]("status").toOption, Some("UNAUTHENTICATED"))

  test("a token without the publish scope is 403, not 401"):
    val readOnly = Map("Authorization" -> s"Bearer ${Tokens.signed(scopes = Set("events:read"))}")
    val request = (Fixtures.structuredHeaders ++ readOnly)
      .foldLeft(basicRequest.post(single))((req, header) => req.header(header._1, header._2, true))
      .body(bytes(Fixtures.structuredBody()))
    val response = Await.result(request.send(backend(Fixtures.StubPublisher())), 5.seconds)
    assertEquals(response.code, StatusCode.Forbidden)
    assertEquals(json(response).hcursor.downField("error").get[String]("status").toOption, Some("PERMISSION_DENIED"))
