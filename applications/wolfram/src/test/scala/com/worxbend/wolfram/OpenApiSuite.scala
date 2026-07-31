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

import io.circe.ACursor
import io.circe.Json
import munit.FunSuite

/** The generated OpenAPI document.
  *
  * These assertions are deliberately about *derivation*, not about the document's literal text: each one names a fact
  * that exists only in an endpoint value (a path, a status code, a description) and checks that it reached the
  * document. A test that compared the whole JSON against a checked-in fixture would be the hand-written spec this
  * generator exists to avoid, one indirection later.
  */
final class OpenApiSuite extends FunSuite:

  private val document: Json = OpenApi.document()

  private def operation(path: String, method: String): ACursor =
    document.hcursor.downField("paths").downField(path).downField(method)

  test("it is an OpenAPI 3.1 document with an info block"):
    assertEquals(document.hcursor.get[String]("openapi").toOption, Some("3.1.0"))
    assert(document.hcursor.downField("info").get[String]("title").toOption.exists(_.contains("wolfram")))

  test("paths and methods come from the endpoints, not from a literal"):
    assertEquals(OpenApi.pathOf(Endpoints.publishEvent), "/events")
    assertEquals(OpenApi.pathOf(Endpoints.publishBatch), "/events/batch")
    assertEquals(OpenApi.methodOf(Endpoints.publishEvent), "post")
    assert(operation("/events", "post").succeeded)
    assert(operation("/events/batch", "post").succeeded)

  test("the operation carries the endpoint's own name, summary and tags"):
    val single = operation("/events", "post")
    assertEquals(single.get[String]("operationId").toOption, Some("publishEvent"))
    assertEquals(single.get[String]("summary").toOption, Endpoints.publishEvent.info.summary)
    assertEquals(single.downField("tags").values.map(_.toList), Some(List(Json.fromString("ingestion"))))

  test("the prose documents both content modes, since the binding puts them on one resource"):
    val description = operation("/events", "post").get[String]("description").toOption.getOrElse("")
    assert(description.contains("Binary mode"), description)
    assert(description.contains("Structured mode"), description)
    assert(description.contains("ce-specversion"), description)

  test("the request body advertises every media type the endpoint accepts"):
    val content = operation("/events", "post").downField("requestBody").downField("content")
    Endpoints.SingleRequestMediaTypes.foreach: mediaType =>
      assert(content.downField(mediaType).succeeded, s"$mediaType should be documented")
    assert(
      operation("/events/batch", "post")
        .downField("requestBody")
        .downField("content")
        .downField(HttpBinding.BatchMediaType)
        .succeeded
    )

  test("every modelled failure status reaches the document, with the endpoint's description"):
    val responses = operation("/events", "post").downField("responses")
    List("202", "400", "413", "503").foreach: status =>
      assert(responses.downField(status).succeeded, s"$status should be documented; got ${responses.keys}")
    assert(
      responses.downField("503").get[String]("description").toOption.exists(_.contains("broker")),
      responses.downField("503").focus.toString
    )

  test("the batch endpoint documents both of its success codes"):
    val responses = operation("/events/batch", "post").downField("responses")
    assertEquals(
      responses.downField("202").get[String]("description").toOption,
      Some("Every event in the batch was published.")
    )
    assert(responses.downField("207").succeeded)

  test("the operational endpoints are absent — they are not part of the API's contract"):
    val paths = document.hcursor.downField("paths").keys.map(_.toList).getOrElse(Nil)
    assertEquals(paths.toSet, Set("/events", "/events/batch"))

  test("the document served on /openapi.json is the generated one"):
    assertEquals(AdminRoutes.openApi.status, 200)
    assertEquals(io.circe.parser.parse(AdminRoutes.openApi.body), Right(document))
