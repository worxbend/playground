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

package com.worxbend.ferrite

import com.worxbend.ferrite.routing.Paths
import com.worxbend.ferrite.web.Urls
import java.time.OffsetDateTime
import munit.FunSuite
import play.api.mvc.RequestHeader
import play.api.test.FakeRequest

/** The URL helper and the router must agree.
  *
  * ADR §8.1 accepts one cost for dropping `conf/routes`: no reverse router, so every link is a value in [[Urls]] and
  * nothing checks that a link is routable. This suite is that check, and it is the reason `Urls` is allowed to exist.
  *
  * It asserts against [[Paths]] — the very extractors the routers `case` on — rather than against a second copy of the
  * patterns. A test that re-spelled `"/events/"` would prove only that two literals in the test tree agree with each
  * other, which is exactly the failure mode it is supposed to prevent.
  */
final class UrlRoutingSuite extends FunSuite:

  private def request(url: String): RequestHeader = FakeRequest("GET", url)

  private val at = OffsetDateTime.parse("2026-07-26T10:15:30+02:00")

  test("the landing URL is matched by the root pattern"):
    assert(Paths.Root.unapplySeq(request(Urls.Root)).isDefined)

  test("the events URL is matched, with and without a query string"):
    assert(Paths.Events.unapplySeq(request(Urls.events(""))).isDefined)
    assert(Paths.Events.unapplySeq(request(Urls.events("v=1&type=com.worxbend.iot.alarm"))).isDefined)

  test("the overview URL is the root, with and without a range"):
    assert(Paths.Root.unapplySeq(request(Urls.overview(""))).isDefined)
    assert(Paths.Root.unapplySeq(request(Urls.overview("range=7d"))).isDefined)

  test("the live-tail URL is matched, and no other pattern claims it"):
    assert(Paths.Live.unapplySeq(request(Urls.live(""))).isDefined)
    assert(Paths.Live.unapplySeq(request(Urls.live("v=1&severity=%3E%3Derror"))).isDefined)
    // The whole reason the stream is `/live` and not `/events/stream`: under the latter the detail pattern would also
    // match it, and the tail would work only while its `case` happened to be listed first.
    assert(Paths.Event.unapplySeq(request(Urls.live(""))).isEmpty)
    assert(Paths.Events.unapplySeq(request(Urls.live(""))).isEmpty)

  test("an event detail URL is matched and yields the event id back"):
    val url = Urls.event(at, Fixtures.FirstUid)
    assertEquals(Paths.Event.unapplySeq(request(url)), Some(List(Fixtures.FirstUid.toString)))

  test("the detail URL carries occurred_at as a query parameter, so the path stays a single clean segment"):
    val url = Urls.event(at, Fixtures.FirstUid)
    val query = FakeRequest("GET", url).getQueryString(Urls.AtParam)
    // Percent-encoded on the way out, decoded by Play on the way in, and byte-identical in between.
    assertEquals(query, Some("2026-07-26T10:15:30+02:00"))

  test("an asset URL is matched and the whole sub-path is captured, slashes included"):
    val url = Urls.asset("lib/htmx.org/dist/htmx.min.js")
    assertEquals(Paths.Asset.unapplySeq(request(url)), Some(List("lib/htmx.org/dist/htmx.min.js")))

  test("the three operational URLs are matched"):
    assert(Paths.Metrics.unapplySeq(request(Urls.Metrics)).isDefined)
    assert(Paths.HealthLive.unapplySeq(request(Urls.HealthLive)).isDefined)
    assert(Paths.HealthReady.unapplySeq(request(Urls.HealthReady)).isDefined)

  test("patterns do not match each other's paths, so router composition order cannot mask a mistake"):
    assert(Paths.Events.unapplySeq(request(Urls.Metrics)).isEmpty)
    assert(Paths.Event.unapplySeq(request(Urls.events(""))).isEmpty)
    assert(Paths.Asset.unapplySeq(request(Urls.events(""))).isEmpty)
    assert(Paths.HealthLive.unapplySeq(request(Urls.HealthReady)).isEmpty)

  test("the live web router answers every URL the helper can build"):
    val router = Fixtures.webRouter()
    assert(router.routes.isDefinedAt(FakeRequest("GET", Urls.Root)))
    assert(router.routes.isDefinedAt(FakeRequest("GET", Urls.overview("range=30d"))))
    assert(router.routes.isDefinedAt(FakeRequest("GET", Urls.events(""))))
    assert(router.routes.isDefinedAt(FakeRequest("GET", Urls.events("v=1&device=kitchen-1"))))
    assert(router.routes.isDefinedAt(FakeRequest("GET", Urls.event(at, Fixtures.FirstUid))))
    assert(router.routes.isDefinedAt(FakeRequest("GET", Urls.live("v=1&device=kitchen-1"))))
    assert(!router.routes.isDefinedAt(FakeRequest("GET", "/nope")))

  test("every asset the layout references is actually on the classpath under the assets root"):
    // `Assets.at("/public", file)` resolves against the classpath, so a renamed WebJar path or a missing stylesheet
    // is a 404 in the browser and nothing at all in the build. These four are the whole asset surface.
    val assets = Vector(
      "css/app.css",
      "js/app.js",
      "lib/htmx.org/dist/htmx.min.js",
      "lib/alpinejs/dist/cdn.min.js"
    )
    assets.foreach { path =>
      val resource = s"${routing.AssetsRouter.ClasspathRoot.stripPrefix("/")}/$path"
      assert(Option(getClass.getClassLoader.getResource(resource)).isDefined, s"missing asset: $resource")
      assert(Paths.Asset.unapplySeq(request(Urls.asset(path))).contains(List(path)))
    }

  test("asset URLs are cache-busted, and the query never affects routing"):
    val url = Urls.asset("css/app.css")
    assert(url.contains("?v="), s"expected a cache-busting query in '$url'")
    assert(Paths.Asset.unapplySeq(request(url)).isDefined)
