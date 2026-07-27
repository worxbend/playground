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

package io.kzonix.wolfram

import io.circe.Json
import sttp.tapir.AnyEndpoint
import sttp.tapir.EndpointInput
import sttp.tapir.EndpointIO
import sttp.tapir.EndpointOutput
import sttp.tapir.EndpointTransput

/** An OpenAPI 3.1 document generated from the Tapir endpoint values.
  *
  * **Why this is hand-rolled.** The obvious implementation is `tapir-openapi-docs`, which also derives full JSON
  * Schemas from Tapir's `Schema[T]`. That artifact is not on this module's classpath and `project/Dependencies.scala`
  * is not this task's to edit, so the document is produced by walking the endpoint ADT — which is public, sealed, and
  * exactly the data `tapir-openapi-docs` walks. What is lost is body *schemas*: this emits permissive placeholders
  * rather than deriving `properties` from `Schema[T]`. Everything a client routes on — paths, methods, parameters,
  * request media types, and every documented status code with its description — is derived from the endpoints and
  * therefore cannot drift from the server. Adding `tapir-openapi-docs` would replace this file entirely; see the
  * follow-ups.
  *
  * **Why generated at all, rather than a checked-in YAML.** A hand-written spec is a second statement of the contract,
  * and the two disagree the first time an error output is added — usually silently, because nothing compiles the YAML.
  *
  * The document is a `Json` value and not a string so the tests can assert on structure rather than on formatting.
  */
object OpenApi:

  /** Service identity in the document. Not read from [[WolframConfig]]: the OpenAPI `info` block describes the *API*,
    * which is the same in every environment, while the config describes this deployment of it.
    */
  final case class Info(title: String, version: String, description: String)

  /** An endpoint plus the request media types it accepts.
    *
    * The second field exists because Tapir's raw-byte body reports a single media type from its codec format, and this
    * API deliberately accepts several for the same bytes (see [[Endpoints]]). Keeping it beside the endpoint rather
    * than inside the generator means the list is stated where the endpoint is, once.
    */
  final case class Route(endpoint: AnyEndpoint, requestMediaTypes: List[String])

  /** wolfram's routes, in the order they appear in the document. */
  def routes: List[Route] =
    Endpoints.all.map: endpoint =>
      Route(endpoint, endpoint.info.name.flatMap(Endpoints.requestMediaTypes.get).getOrElse(Nil))

  /** The document for this service. */
  def document(info: Info = defaultInfo): Json = document(info, routes)

  /** The default `info` block. */
  def defaultInfo: Info =
    Info(
      title = "wolfram — CloudEvents ingestion",
      version = "1.0.0",
      description =
        "Validates CloudEvents 1.0 events and publishes them to Kafka. Stateless: this service owns no storage."
    )

  /** Builds an OpenAPI 3.1 document from arbitrary routes. Parameterised so a test can document a synthetic endpoint
    * and assert on the result without going through wolfram's own contract.
    */
  def document(info: Info, routes: List[Route]): Json =
    val paths = routes
      .groupBy(route => pathOf(route.endpoint))
      .toVector
      .sortBy((path, _) => path)
      .map((path, group) => path -> Json.fromFields(group.map(route => methodOf(route.endpoint) -> operation(route))))
    Json.obj(
      "openapi" -> Json.fromString("3.1.0"),
      "info" -> Json.obj(
        "title" -> Json.fromString(info.title),
        "version" -> Json.fromString(info.version),
        "description" -> Json.fromString(info.description)
      ),
      "paths" -> Json.fromFields(paths.map((path, operations) => path -> operations))
    )

  /** One `(path, method)` operation. */
  private def operation(route: Route): Json =
    val endpoint = route.endpoint
    val inputs = flattenInput(endpoint.securityInput) ++ flattenInput(endpoint.input)
    val fields = Vector(
      endpoint.info.name.map(name => "operationId" -> Json.fromString(name)),
      endpoint.info.summary.map(summary => "summary" -> Json.fromString(summary)),
      endpoint.info.description.map(description => "description" -> Json.fromString(description)),
      Option.when(endpoint.info.tags.nonEmpty)("tags" -> Json.arr(endpoint.info.tags.map(Json.fromString)*)),
      Option.when(endpoint.info.deprecated)("deprecated" -> Json.True),
      Option.when(parameters(inputs).nonEmpty)("parameters" -> Json.arr(parameters(inputs)*)),
      requestBody(inputs, route.requestMediaTypes).map("requestBody" -> _),
      Some("responses" -> responses(endpoint))
    ).flatten
    Json.fromFields(fields)

  /** Path template in OpenAPI's `{name}` form.
    *
    * Tapir's own `showPathTemplate` renders captures as `{paramN}` when unnamed and is the right tool, but it also
    * appends query parameters by default; both are pinned explicitly here so the template is exactly what a router
    * matches on.
    */
  def pathOf(endpoint: AnyEndpoint): String =
    endpoint.showPathTemplate(
      showPathParam = (index, capture) => s"{${capture.name.getOrElse(s"param$index")}}",
      showQueryParam = None,
      includeAuth = false
    )

  /** HTTP method, lower-cased as OpenAPI wants it. Read from the endpoint's own `FixedMethod` input rather than from
    * Tapir's internals, so this generator depends only on the public ADT.
    */
  def methodOf(endpoint: AnyEndpoint): String =
    flattenInput(endpoint.input)
      .collectFirst { case method: EndpointInput.FixedMethod[?] => method.m.method.toLowerCase }
      .getOrElse("get")

  /** Path, query and header parameters, in the order the endpoint declares them.
    *
    * `EndpointIO.Headers` — the "give me every header" input this API uses to read `ce-*` attributes — is deliberately
    * not turned into a parameter: it names no header, so there is nothing to document, and emitting a parameter called
    * `headers` would be worse than emitting nothing. The `ce-*` attributes are described in the operation's prose,
    * which is where a reader of the CloudEvents binding expects them.
    */
  private def parameters(inputs: Vector[EndpointInput[?]]): Vector[Json] =
    inputs.collect:
      case capture: EndpointInput.PathCapture[?] =>
        parameter(capture.name.getOrElse("param"), "path", required = true, description(capture))
      case query: EndpointInput.Query[?] =>
        parameter(query.name, "query", required = isRequired(query), description(query))
      case header: EndpointIO.Header[?] =>
        parameter(header.name, "header", required = isRequired(header), description(header))

  private def parameter(name: String, in: String, required: Boolean, describedBy: Option[String]): Json =
    Json.fromFields(
      Vector(
        "name" -> Json.fromString(name),
        "in" -> Json.fromString(in),
        "required" -> Json.fromBoolean(required),
        "schema" -> Json.obj("type" -> Json.fromString("string"))
      ) ++ describedBy.map(text => "description" -> Json.fromString(text))
    )

  /** The request body, if the endpoint has one.
    *
    * `mediaTypes` overrides what the body codec reports; when it is empty the codec's own format is used, which is the
    * right answer for a JSON body and the only available one for anything this API does not enumerate.
    */
  private def requestBody(inputs: Vector[EndpointInput[?]], mediaTypes: List[String]): Option[Json] =
    inputs
      .collectFirst { case body: EndpointIO.Body[?, ?] => body }
      .map: body =>
        val declared = if mediaTypes.isEmpty then List(mediaTypeOf(body)) else mediaTypes
        Json.fromFields(
          Vector(
            "required" -> Json.True,
            "content" -> Json.fromFields(declared.map(mediaType => mediaType -> content(mediaType)))
          ) ++ description(body).map(text => "description" -> Json.fromString(text))
        )

  /** Every documented response of an endpoint: the success output, then each `oneOf` error variant. */
  private def responses(endpoint: AnyEndpoint): Json =
    val successes = responsesOf(endpoint.output, defaultStatus = 200, defaultDescription = "Success")
    val failures = responsesOf(endpoint.errorOutput, defaultStatus = 400, defaultDescription = "Error")
    Json.fromFields((successes ++ failures).toVector.sortBy((status, _) => status).map((status, json) =>
      status -> json
    ))

  /** Walks one output tree into `status -> response` pairs.
    *
    * Three shapes have to be handled and they compose: a `OneOf` fans out into one response per variant; a
    * `FixedStatusCode` pins the status of the branch it sits in; a dynamic `statusCode` output contributes one response
    * per code it was documented with, which is how the batch endpoint advertises both 202 and 207.
    */
  private def responsesOf(
    output: EndpointOutput[?],
    defaultStatus: Int,
    defaultDescription: String
  ): Map[String, Json] =
    val parts = flattenOutput(output)
    val variants = parts.collect { case one: EndpointOutput.OneOf[?, ?] => one }
    if variants.nonEmpty then
      variants
        .flatMap(_.variants.toVector)
        .flatMap(variant => responsesOf(variant.output, defaultStatus, defaultDescription))
        .toMap
    else
      val body = parts.collectFirst { case body: EndpointIO.Body[?, ?] => body }
      val content = body.map(b => Json.obj(mediaTypeOf(b) -> OpenApi.content(mediaTypeOf(b))))
      val documented = parts.collect { case status: EndpointOutput.StatusCode[?] => status }.flatMap(documentedCodes)
      val fixed = parts.collectFirst { case status: EndpointOutput.FixedStatusCode[?] => status }

      val codes: Vector[(Int, Option[String])] =
        if documented.nonEmpty then documented
        else
          val text = fixed.flatMap(description).orElse(body.flatMap(description))
          Vector(fixed.map(_.statusCode.code).getOrElse(defaultStatus) -> text)

      codes.map { (code, text) =>
        code.toString -> Json.fromFields(
          Vector("description" -> Json.fromString(text.getOrElse(defaultDescription))) ++
            content.map("content" -> _)
        )
      }.toMap

  /** The codes a dynamic `statusCode` output was documented with, and their descriptions. Ranges (`2xx`) are skipped:
    * OpenAPI can express them, but nothing in this API uses one and emitting an untested shape is not free.
    */
  private def documentedCodes(status: EndpointOutput.StatusCode[?]): Vector[(Int, Option[String])] =
    status.documentedCodes.toVector.collect:
      case (Left(code), info) => code.code -> info.description

  /** A permissive schema. See the object Scaladoc: deriving real JSON Schema is what `tapir-openapi-docs` is for. */
  private def content(mediaType: String): Json =
    val schema =
      if mediaType.endsWith("json") then Json.obj("type" -> Json.fromString("object"))
      else Json.obj("type" -> Json.fromString("string"), "format" -> Json.fromString("binary"))
    Json.obj("schema" -> schema)

  private def mediaTypeOf(body: EndpointIO.Body[?, ?]): String = body.codec.format.mediaType.toString

  /** An input or output is optional when its codec says so — that is where `header[Option[String]]` differs from
    * `header[String]`, and re-deriving it from the Scala type here would be a second, disagreeing source of truth.
    */
  private def isRequired(atom: EndpointTransput.Atom[?]): Boolean = !atom.codec.schema.isOptional

  private def description(atom: EndpointTransput.Atom[?]): Option[String] = atom.info.description

  /** Flattens the input tree, keeping leaves in declaration order. `Pair` is the `and`/`/` combinator and `MappedPair`
    * is what `mapTo`/`mapIn` produce; everything else is a leaf this generator either documents or ignores.
    */
  private def flattenInput(input: EndpointInput[?]): Vector[EndpointInput[?]] = input match
    case pair: EndpointInput.Pair[?, ?, ?]          => flattenInput(pair.left) ++ flattenInput(pair.right)
    case pair: EndpointInput.MappedPair[?, ?, ?, ?] => flattenInput(pair.input)
    case io: EndpointIO.Pair[?, ?, ?]               => flattenInput(io.left) ++ flattenInput(io.right)
    case io: EndpointIO.MappedPair[?, ?, ?, ?]      => flattenInput(io.io)
    case leaf                                       => Vector(leaf)

  private def flattenOutput(output: EndpointOutput[?]): Vector[EndpointOutput[?]] = output match
    case pair: EndpointOutput.Pair[?, ?, ?]          => flattenOutput(pair.left) ++ flattenOutput(pair.right)
    case pair: EndpointOutput.MappedPair[?, ?, ?, ?] => flattenOutput(pair.output)
    case io: EndpointIO.Pair[?, ?, ?]                => flattenOutput(io.left) ++ flattenOutput(io.right)
    case io: EndpointIO.MappedPair[?, ?, ?, ?]       => flattenOutput(io.io)
    case leaf                                        => Vector(leaf)
