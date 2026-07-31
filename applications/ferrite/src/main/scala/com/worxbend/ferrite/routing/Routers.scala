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

package com.worxbend.ferrite.routing

import com.worxbend.ferrite.controllers.EventsController
import com.worxbend.ferrite.controllers.OpsController
import com.worxbend.ferrite.web.Urls
import controllers.Assets
import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.routing.Router.Routes
import play.api.routing.SimpleRouter
import play.api.routing.sird.GET
import play.api.routing.sird.PathExtractor

/** The path patterns, built from [[Urls]] rather than written out again.
  *
  * SIRD's `p"/events/$id"` interpolator is a *pattern-position* construct: it can only appear inside a `case`, and its
  * literal parts have to be written into the pattern. That would put `"/events"` in two places — the URL builder and
  * the route — and a mismatch between them is invisible until someone clicks a link.
  *
  * `PathExtractor.cached` is the same thing the interpolator itself calls, taking the `StringContext` parts directly.
  * Handing it parts derived from [[Urls]] means the route and the URL builder are **the same string by construction**,
  * and `UrlRoutingSuite` then only has to prove that each builder's output is matched — not that two literals happen to
  * agree.
  *
  * The parts follow the interpolator's own grammar: an empty trailing part is a single decoded segment (`[^/]*`), and a
  * part beginning with `*` is a raw greedy match, which is what an asset path needs since it contains slashes.
  */
object Paths:

  val Root: PathExtractor = PathExtractor.cached(Seq(Urls.Root))
  val Events: PathExtractor = PathExtractor.cached(Seq(Urls.Events))
  val Event: PathExtractor = PathExtractor.cached(Seq(s"${Urls.Events}/", ""))
  val Asset: PathExtractor = PathExtractor.cached(Seq(s"${Urls.AssetsPrefix}/", "*"))
  val Metrics: PathExtractor = PathExtractor.cached(Seq(Urls.Metrics))
  val HealthLive: PathExtractor = PathExtractor.cached(Seq(Urls.HealthLive))
  val HealthReady: PathExtractor = PathExtractor.cached(Seq(Urls.HealthReady))

/** The web UI's routes.
  *
  * SIRD, not a compiled `conf/routes` (ADR §8.1): routing is ordinary Scala, so a router is a value a unit test can
  * construct and interrogate without booting an application, and there is no generated code between a URL and the
  * method it calls.
  */
@Singleton
final class WebRouter @Inject() (controller: EventsController) extends SimpleRouter:

  override def routes: Routes =
    case GET(Paths.Root())          => controller.index
    case GET(Paths.Events())        => controller.list
    case GET(Paths.Event(eventUid)) => controller.detail(eventUid)

/** Metrics and the two probes.
  *
  * Separate from [[WebRouter]] because they have a different audience and a different failure mode: these three must
  * keep answering when the UI cannot, so they are wired, tested and reasoned about apart from it.
  */
@Singleton
final class OpsRouter @Inject() (controller: OpsController) extends SimpleRouter:

  override def routes: Routes =
    case GET(Paths.Metrics())     => controller.metrics
    case GET(Paths.HealthLive())  => controller.live
    case GET(Paths.HealthReady()) => controller.ready

/** Static assets, in a router of their own.
  *
  * ADR §8.1 is specific about why: constructing `controllers.Assets` by hand needs an `HttpErrorHandler`, an
  * `AssetsMetadata` and a `FileMimeTypes`, so a test that only wanted to check a UI route would have to assemble the
  * whole asset pipeline first. Isolating it keeps the other two routers constructible from a single stub, while
  * [[Paths.Asset]] stays matchable without an `Assets` instance at all.
  *
  * `AssetsModule` is in Play's default `play.modules.enabled`, so `Assets` is injectable with no configuration.
  */
@Singleton
final class AssetsRouter @Inject() (assets: Assets) extends SimpleRouter:

  override def routes: Routes =
    case GET(Paths.Asset(file)) => assets.at(AssetsRouter.ClasspathRoot, file)

object AssetsRouter:

  /** The classpath prefix assets are served from. Tailwind's generated stylesheet is written here as a build resource,
    * which is why the runtime image never needs Node (ADR §8.3), and the htmx/Alpine WebJars are unpacked here by
    * sbt-web.
    */
  val ClasspathRoot: String = "/public"

/** The application's routing table: the UI, the assets, then the operational endpoints.
  *
  * Selected by `play.http.router` in `application.conf`. Composition by `orElse` rather than one large partial function
  * keeps each half independently testable and makes precedence explicit — the UI claims `/` and `/events`, assets claim
  * `/assets/…`, and the operational routes come last because they are exact paths that cannot collide with either.
  */
@Singleton
final class AppRouter @Inject() (web: WebRouter, assets: AssetsRouter, ops: OpsRouter) extends SimpleRouter:

  override def routes: Routes = web.routes.orElse(assets.routes).orElse(ops.routes)
