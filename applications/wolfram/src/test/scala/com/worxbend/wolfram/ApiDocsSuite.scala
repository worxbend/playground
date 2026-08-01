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

import io.circe.Json
import munit.FunSuite

/** The generated OpenAPI document.
  *
  * **These assert on the document, not on the generator.** `tapir-openapi-docs` is a dependency and testing it is not
  * this suite's job; what is this suite's job is that the *contract* the document describes is the one intended — that
  * the AIP path shapes survived generation, that every operation is documented as authenticated, and that the response
  * schemas are real rather than the permissive placeholders the previous hand-rolled generator produced. That last one
  * is the regression this file exists for: a document that renders and describes nothing looks fine.
  */
final class ApiDocsSuite extends FunSuite:

  private val document: Json = ApiDocs.json()
  private val paths: Json = document.hcursor.downField("paths").focus.getOrElse(Json.Null)

  private def pathKeys: Set[String] = paths.asObject.map(_.keys.toSet).getOrElse(Set.empty)

  private def operation(path: String, method: String = "post"): Json =
    paths.hcursor.downField(path).downField(method).focus.getOrElse(fail(s"no $method $path in the document"))

  // --- resource-oriented shape --------------------------------------------------------------------------------

  test("every path is versioned under /v1"):
    assert(pathKeys.nonEmpty, "the document has no paths at all")
    pathKeys.foreach(path => assert(path.startsWith("/v1/"), s"$path is not under /v1"))

  test("the standard Create is POST on the collection itself"):
    assert(pathKeys.contains("/v1/events"), pathKeys.toString)

  test("custom methods use a colon, not a sub-path"):
    // The property, not the spelling: any path segment after the collection that is not a colon-verb would collide
    // with a future `GET /v1/events/{event}`, which is precisely what AIP-136 exists to prevent.
    assert(pathKeys.contains("/v1/events:batchCreate"), pathKeys.toString)
    assert(pathKeys.contains("/v1/events:validate"), pathKeys.toString)
    pathKeys.foreach: path =>
      val tail = path.stripPrefix("/v1/events")
      assert(tail.isEmpty || tail.startsWith(":"), s"$path introduces a sub-path where a custom method belongs")

  test("operation ids are the AIP method names a generated client should expose"):
    val ids = pathKeys.toList.map(path => operation(path).hcursor.get[String]("operationId").toOption)
    assertEquals(ids.flatten.toSet, Set("createEvent", "batchCreateEvents", "validateEvent"))

  // --- the schemas are real -----------------------------------------------------------------------------------

  test("component schemas exist for every wire type, with properties"):
    val schemas = document.hcursor
      .downField("components")
      .downField("schemas")
      .focus
      .flatMap(_.asObject)
      .getOrElse(fail("the document has no component schemas"))
    // Tapir names schemas by fully-qualified type, so match on the suffix rather than pinning the whole name.
    List("Event", "ErrorBody", "ErrorInfo", "BatchCreateResponse", "ValidateResponse").foreach: expected =>
      val found = schemas.keys.find(_.endsWith(expected)).getOrElse(fail(s"no schema for $expected in ${schemas.keys}"))
      val properties = schemas(found).flatMap(_.hcursor.downField("properties").focus).flatMap(_.asObject)
      assert(properties.exists(_.nonEmpty), s"$found has no properties — this is a placeholder, not a schema")

  test("the Event schema carries the resource name field, because that is the field clients store"):
    val schemas = document.hcursor.downField("components").downField("schemas").focus.flatMap(_.asObject).get
    val event = schemas.keys.find(_.endsWith("Event")).flatMap(schemas(_)).getOrElse(fail("no Event schema"))
    val properties = event.hcursor.downField("properties").focus.flatMap(_.asObject).map(_.keys.toSet).getOrElse(Set())
    assert(properties.contains("name"), properties.toString)
    assert(properties.contains("destination"), properties.toString)

  // --- security ------------------------------------------------------------------------------------------------

  test("every operation is documented as requiring the bearer scheme"):
    // The document is what a client generator reads. An endpoint that authenticates but does not *say* it
    // authenticates produces an SDK with no way to pass a token, and the first sign is a 401 nobody expected.
    pathKeys.foreach: path =>
      val security = operation(path).hcursor.downField("security").focus
      assert(security.exists(_.asArray.exists(_.nonEmpty)), s"$path has no security requirement")

  test("the credential is documented as required, not optional"):
    // Tapir reads `auth.bearer[Option[String]]` literally and emits an empty alternative alongside the real one,
    // which says "no credential is also fine". It is not: the Option exists so a missing token becomes a
    // documented 401 rather than Tapir's bodyless 400. A generated client that believed the document would make
    // every call unauthenticated and receive a 401 for each.
    pathKeys.foreach: path =>
      val alternatives = operation(path).hcursor.downField("security").values.getOrElse(Nil).toList
      assert(alternatives.nonEmpty, s"$path documents no security at all")
      alternatives.foreach: alternative =>
        assert(
          alternative.asObject.exists(_.nonEmpty),
          s"$path offers an empty security alternative, which documents the token as optional"
        )

  test("the bearer scheme is declared as HTTP bearer"):
    val schemes = document.hcursor
      .downField("components")
      .downField("securitySchemes")
      .focus
      .flatMap(_.asObject)
      .getOrElse(fail("no security schemes"))
    val scheme = schemes.values.headOption.getOrElse(fail("no security scheme defined"))
    assertEquals(scheme.hcursor.get[String]("type").toOption, Some("http"))
    assertEquals(scheme.hcursor.get[String]("scheme").toOption, Some("bearer"))

  // --- responses ------------------------------------------------------------------------------------------------

  test("every documented failure status appears on every operation"):
    // The `oneOf` is shared, so this is really asserting that sharing it worked — a variant added to `failures` and
    // silently dropped from one operation's document is exactly the drift generating the spec is supposed to prevent.
    val expected = Set("400", "401", "403", "413", "503")
    pathKeys.foreach: path =>
      val codes = operation(path).hcursor.downField("responses").focus.flatMap(_.asObject).map(_.keys.toSet)
      assert(codes.exists(expected.subsetOf(_)), s"$path documents ${codes.getOrElse(Set.empty)}, wanted $expected")

  test("the batch operation documents 207, and the others do not"):
    def codes(path: String): Set[String] =
      operation(path).hcursor.downField("responses").focus.flatMap(_.asObject).map(_.keys.toSet).getOrElse(Set.empty)
    assert(codes("/v1/events:batchCreate").contains("207"))
    assert(!codes("/v1/events").contains("207"), "a single create has nothing partial to report")

  // --- the document renders -------------------------------------------------------------------------------------

  test("the document renders as YAML as well as JSON"):
    // Both are served, and the YAML serialiser is a *separate artifact* from the model: resolving them at different
    // versions is a NoSuchMethodError on the first request for the document, not a compile error.
    val yaml = ApiDocs.yaml()
    assert(yaml.startsWith("openapi:"), yaml.take(80))
    assert(yaml.contains("/v1/events:batchCreate"), "the YAML rendering lost the custom method")

  test("info names the contract version, which is not the build version"):
    assertEquals(document.hcursor.downField("info").get[String]("version").toOption, Some(ApiDocs.ApiVersion))
    val description = document.hcursor.downField("info").get[String]("description").toOption.getOrElse("")
    assert(description.contains("AIP-136"), "the description should tell a reader what shape the API follows")
