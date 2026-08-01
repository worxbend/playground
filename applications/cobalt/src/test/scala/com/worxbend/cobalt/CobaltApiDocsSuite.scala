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

package com.worxbend.cobalt

import io.circe.Json
import munit.FunSuite
import scala.concurrent.duration.DurationInt

/** The document, and the one property that makes describing a Cask API with tapir values safe.
  *
  * **The drift test is the whole justification for this design.** cobalt's routes are Cask annotations and its
  * document is a separate list of tapir endpoint values; nothing in the compiler connects them. Left unchecked that
  * is a hand-written spec with extra steps — the failure mode a generated document is supposed to eliminate. So the
  * two sets of paths are compared in both directions, by reflecting over the annotations Cask itself routes on. A
  * route added without a description fails here, and so does a description of a route that does not exist.
  */
final class CobaltApiDocsSuite extends FunSuite:

  private val document: Json = CobaltApiDocs.json
  private val paths: Set[String] =
    document.hcursor.downField("paths").focus.flatMap(_.asObject).map(_.keys.toSet).getOrElse(Set.empty)

  /** Every path Cask actually serves, read out of the router's own dispatch table.
    *
    * `caskMetadata` is what Cask itself routes on, so this compares the document against the *served* surface rather
    * than against a second list somebody maintains. Java annotation reflection was the first attempt and silently
    * found nothing — Cask's `@cask.get` is a Scala `StaticAnnotation` with no runtime retention — which is precisely
    * the vacuous-pass this suite's third test now guards against.
    *
    * Building an instance costs a supervisor and a telemetry registry; `Fixtures.idleSupervisor` supplies one that
    * needs no broker.
    */
  private def servedPaths: Set[String] =
    val telemetry = Fixtures.telemetry()
    try
      given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic
      val handlers = AdminHandlers(
        telemetry,
        HealthChecks.create(),
        DeadLetterAdmin(
          Fixtures.StubDeadLetterStore(),
          ReplayMetrics(telemetry.registry),
          ReplayConfig(enabled = true, maxRecords = 10, maxAttempts = 3, 1.second),
          Fixtures.Topic,
          "dlq"
        ),
        SupervisorAdmin(Fixtures.idleSupervisor, 5.seconds)
      )
      CobaltRoutes(handlers).caskMetadata.value.map(_.endpoint.path).toSet
    finally telemetry.close()

  // --- the two directions -----------------------------------------------------------------------------------

  test("every route cobalt serves is described"):
    // Excluding the static assets, which are Swagger UI's own files and not part of the API's contract with a
    // client. Everything else must appear, including the two that change production.
    val undocumented = servedPaths -- paths - AdminRoutes.SwaggerAssetsPath - AdminRoutes.DocsPath -
      AdminRoutes.OpenApiJsonPath - AdminRoutes.OpenApiYamlPath
    assertEquals(undocumented, Set.empty[String], s"served but not documented: $undocumented")

  test("every path the document describes is actually served"):
    val phantom = paths -- servedPaths
    assertEquals(phantom, Set.empty[String], s"documented but not served: $phantom")

  test("the reflection found the routes at all, so neither direction can pass vacuously"):
    // A guard on the guard. If Cask's annotations ever stop being readable this way, both comparisons above become
    // `Set.empty == Set.empty` and the suite goes green over nothing.
    assert(servedPaths.size >= 12, s"only ${servedPaths.size} routes discovered: $servedPaths")
    assert(paths.size >= 12, s"the document describes only ${paths.size} paths")

  // --- the shape of what is described -----------------------------------------------------------------------

  test("custom methods use a colon, matching wolfram"):
    assert(paths.contains("/admin/consumer:pause"), paths.toString)
    assert(paths.contains("/admin/consumer:restart"), paths.toString)
    assert(paths.contains("/admin/dlq:replay"), paths.toString)

  test("the two destructive operations document their dryRun default as true"):
    // The single most important thing this document says. An operator reading it must not come away believing that
    // omitting the flag is safe in one place and not the other.
    List("/admin/consumer:restart", "/admin/dlq:replay").foreach: path =>
      val parameters = document.hcursor
        .downField("paths")
        .downField(path)
        .downField("post")
        .downField("parameters")
        .values
        .getOrElse(fail(s"$path documents no parameters"))
      val dryRun = parameters
        .find(_.hcursor.get[String]("name").toOption.contains("dryRun"))
        .getOrElse(fail(s"$path does not document dryRun"))
      assertEquals(dryRun.hcursor.downField("schema").get[Boolean]("default").toOption, Some(true), path)

  test("the restart target parameter warns that latest skips events"):
    val description = document.hcursor
      .downField("paths")
      .downField("/admin/consumer:restart")
      .downField("post")
      .downField("parameters")
      .values
      .getOrElse(Nil)
      .find(_.hcursor.get[String]("name").toOption.contains("target"))
      .flatMap(_.hcursor.get[String]("description").toOption)
      .getOrElse(fail("target has no description"))
    assert(description.contains("skips unconsumed events"), description)

  test("schemas have properties, not placeholders"):
    val schemas = document.hcursor
      .downField("components")
      .downField("schemas")
      .focus
      .flatMap(_.asObject)
      .getOrElse(fail("no component schemas"))
    List("ConsumerStatusDoc", "PartitionPosition").foreach: expected =>
      val found = schemas.keys.find(_.endsWith(expected)).getOrElse(fail(s"no schema for $expected in ${schemas.keys}"))
      val properties = schemas(found).flatMap(_.hcursor.downField("properties").focus).flatMap(_.asObject)
      assert(properties.exists(_.nonEmpty), s"$found is a placeholder, not a schema")

  test("operations are grouped so Swagger UI is navigable"):
    val tags = paths.toList.flatMap: path =>
      val item = document.hcursor.downField("paths").downField(path)
      List("get", "post").flatMap(method => item.downField(method).downField("tags").values.getOrElse(Nil))
    val names = tags.flatMap(_.asString).toSet
    assertEquals(names, Set("consumer", "dead-letters", "platform"))

  test("the document renders as YAML as well as JSON"):
    // The YAML serialiser is a separate artifact from the model; resolving them at different versions is a
    // NoSuchMethodError on the first request for the document, not a compile error.
    val yaml = CobaltApiDocs.yaml
    assert(yaml.startsWith("openapi:"), yaml.take(60))
    assert(yaml.contains("/admin/consumer:restart"), "the YAML rendering lost a custom method")

  test("the Swagger page references only assets this service serves"):
    // No CDN. A homelab behind a firewall is exactly where somebody needs the docs, and a page that silently renders
    // blank because it could not reach unpkg.com is worse than no page.
    val page = AdminRoutes.SwaggerPage
    assert(!page.contains("//unpkg.com") && !page.contains("//cdn."), page)
    assert(page.contains(s"${AdminRoutes.SwaggerAssetsPath}/swagger-ui.css"), page)
    assert(page.contains(AdminRoutes.OpenApiJsonPath), page)

  test("the swagger-ui webjar resolved to a real classpath directory"):
    // The version lives in the webjar's path, so this is discovered rather than declared. If discovery fails it
    // falls back to the un-versioned base, which 404s every asset — a blank page nobody would attribute to a
    // dependency bump.
    val root = CobaltRoutes.SwaggerResourceRoot
    assert(root.startsWith("META-INF/resources/webjars/swagger-ui/"), s"discovery fell back to '$root'")
    assert(
      getClass.getClassLoader.getResource(s"$root/swagger-ui.css") != null,
      s"$root/swagger-ui.css is not on the classpath"
    )
