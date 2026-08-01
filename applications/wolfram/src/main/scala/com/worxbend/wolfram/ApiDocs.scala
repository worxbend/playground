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

import scala.concurrent.Future
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.openapi.circe.*
import sttp.apispec.openapi.circe.yaml.*
import sttp.tapir.AnyEndpoint
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsOptions
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.swagger.SwaggerUI
import sttp.tapir.swagger.SwaggerUIOptions

/** The OpenAPI document and the Swagger UI that renders it.
  *
  * **Generated from the endpoint values, never written by hand.** A checked-in spec is a second statement of the
  * contract, and the two disagree the first time an error output is added — silently, because nothing compiles YAML.
  * Here the document is a projection of the same `Endpoints` values the server routes on, so "the docs are wrong" is
  * not a state this service can be in.
  *
  * **This replaced a hand-rolled walk of the Tapir endpoint ADT.** That version could describe paths, methods,
  * parameters and status codes, but it emitted permissive placeholders where the body schemas belong, because
  * deriving a JSON Schema from `Schema[T]` is the hard half and `tapir-openapi-docs` is the thing that does it. The
  * placeholder version documented everything except the part a client generator needs most.
  *
  * **Both JSON and YAML are served.** They are the same document; code generators and `curl | jq` want the first,
  * humans and `git diff` want the second, and rendering both from one model costs one line.
  */
object ApiDocs:

  /** The version of the *API contract*, which is not the build version.
    *
    * ADR-adjacent decision worth stating: `SERVICE_VERSION` changes on every deploy, and putting it here would make
    * every generated client look out of date after a patch release that changed nothing a client can see. This value
    * moves when the contract moves.
    */
  val ApiVersion: String = "1.0.0"

  val Title: String = "wolfram — CloudEvents ingestion"

  /** Where the UI and the documents are mounted. `/docs` is the human entry point. */
  val DocsPath: String = "docs"
  val JsonPath: String = "openapi.json"
  val YamlPath: String = "openapi.yaml"

  private val Description: String =
    s"""Validates CloudEvents 1.0 events and publishes them to Kafka. Stateless: this service owns no storage.
       |
       |### Resource-oriented design
       |
       |The surface follows Google's [API Improvement Proposals](https://google.aip.dev). The resource is `events`;
       |`POST /v1/events` is the standard Create (AIP-133) and returns the created resource carrying a
       |`name` of the form `events/{event}` (AIP-122). Operations that are not standard methods are **custom
       |methods** in the sense of AIP-136 and use a colon: `POST /v1/events:batchCreate`,
       |`POST /v1/events:validate`. The colon is load-bearing — `POST /v1/events/batch` would be
       |indistinguishable from acting on a resource whose id is `batch`.
       |
       |### Errors
       |
       |Every failure is the same envelope (AIP-193):
       |
       |```json
       |{ "error": { "code": 400, "message": "…", "status": "INVALID_ARGUMENT",
       |             "details": [ { "reason": "malformed", "domain": "${ApiModel.Domain}", "metadata": {} } ] } }
       |```
       |
       |Branch on `error.status`, not on the HTTP status: three distinct causes all arrive as 400, and
       |`error.details[0].reason` is the same closed vocabulary as the `ingest_events_rejected_total{reason}` metric,
       |so a client's error handling and an operator's dashboard use the same words.
       |
       |### Authentication
       |
       |A JWT bearer token carrying the `${JwtVerifier.PublishScope}` scope. See the operations' security
       |requirement below.""".stripMargin

  /** Operation ids come from the endpoint's `name`, unchanged.
    *
    * Tapir's default derives them from the path, which for a custom method produces `postV1EventsBatchCreate` — a
    * generated client method nobody would choose to call. The endpoint names are already the AIP method names
    * (`batchCreateEvents`), and those are what a generated SDK should expose.
    */
  private val options: OpenAPIDocsOptions =
    OpenAPIDocsOptions.default.copy(operationIdGenerator =
      (_, pathComponents, method) =>
        // `name` is set on every endpoint in this service; the fallback keeps the generator total rather than
        // throwing on an endpoint somebody adds without one.
        pathComponents.lastOption.getOrElse(method.method.toLowerCase)
    )

  /** The document, derived from the public endpoints. */
  def openApi(endpoints: List[AnyEndpoint] = Endpoints.all): OpenAPI =
    val generated = OpenAPIDocsInterpreter(options).toOpenAPI(endpoints, Title, ApiVersion)
    required(generated.copy(info = generated.info.copy(description = Some(Description))))

  /** Marks the bearer credential as **required**, which the generator alone will not do.
    *
    * The security input is `auth.bearer[Option[String]]`, and Tapir reads that `Option` literally: it emits
    * `security: [{}, {httpAuth: []}]`, where the empty object means "no credential is also acceptable". That is
    * false. The `Option` exists so a *missing* token becomes a documented 401 with the AIP-193 envelope instead of
    * Tapir's bodyless 400 — a decision about error shape, not about whether the credential is optional.
    *
    * Left uncorrected, a generated client would treat the token as optional and every one of its unauthenticated
    * calls would 401. So the empty alternative is dropped here, once, after generation.
    */
  private def required(document: OpenAPI): OpenAPI =
    // A ListMap, not a Map: path order in the rendered document is the order operations appear in Swagger UI, and
    // rebuilding it as an unordered Map would reshuffle the page on every JVM.
    val tightened = document.paths.pathItems.map: (path, item) =>
      path -> item.copy(post =
        item.post.map(operation => operation.copy(security = operation.security.filter(_.nonEmpty)))
      )
    document.copy(paths = document.paths.copy(pathItems = tightened))

  /** The document as JSON — what `/openapi.json` serves and what the tests assert on. */
  def json(endpoints: List[AnyEndpoint] = Endpoints.all): io.circe.Json =
    io.circe.syntax.EncoderOps(openApi(endpoints)).asJson.deepDropNullValues

  /** The document as YAML — what `/openapi.yaml` serves. */
  def yaml(endpoints: List[AnyEndpoint] = Endpoints.all): String = openApi(endpoints).toYaml

  /** Swagger UI plus the two documents, as server endpoints on this service's own port.
    *
    * Mounted through the same Vert.x interpreter as the API rather than on a second listener: the UI's "Try it out"
    * button issues a same-origin request, so there is no CORS configuration to get wrong and no second port to expose
    * in compose. The trade is that the docs share the API's lifecycle — if the service is down, so are its docs,
    * which for a document generated from the running build is the honest behaviour anyway.
    */
  def routes: List[ServerEndpoint[Any, Future]] =
    SwaggerUI[Future](
      yaml(),
      SwaggerUIOptions.default
        .pathPrefix(List(DocsPath))
        .yamlName(YamlPath)
        .copy(useRelativePaths = true)
    )
