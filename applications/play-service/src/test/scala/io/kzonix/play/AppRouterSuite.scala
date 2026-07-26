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

package io.kzonix.play

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import play.api.http.Status
import play.api.libs.json.Json
import play.api.test.FakeRequest
import play.api.test.Helpers
import play.api.test.Helpers.contentAsJson
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.status
import play.api.test.Helpers.stubControllerComponents
import play.api.test.Helpers.writeableOf_AnyContentAsEmpty
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

final class AppRouterSuite extends AnyFunSuite with BeforeAndAfterAll:

  // Play's action helpers run the result through a stream, so they need a Materializer.
  private given system: ActorSystem = ActorSystem("app-router-suite")
  private given materializer: Materializer = Materializer(system)

  override protected def afterAll(): Unit =
    Await.result(system.terminate(), 10.seconds)
    ()

  private def controller = AppController(stubControllerComponents())
  private def router = AppRouter(controller)

  test("router matches the health path"):
    assert(router.routes.isDefinedAt(FakeRequest("GET", "/health")))

  test("router matches a greet path"):
    assert(router.routes.isDefinedAt(FakeRequest("GET", "/greet/world")))

  test("router leaves unknown paths unmatched, so Play can return 404"):
    assert(!router.routes.isDefinedAt(FakeRequest("GET", "/nope")))

  test("health reports UP"):
    val result = Helpers.call(controller.health, FakeRequest("GET", "/health"))
    assert(status(result) == Status.OK)
    assert(contentAsJson(result) == Json.obj("status" -> "UP"))

  test("greet echoes the supplied name"):
    val result = Helpers.call(controller.greet("world"), FakeRequest("GET", "/greet/world"))
    assert(status(result) == Status.OK)
    assert(contentAsJson(result) == Json.obj("message" -> "Hello, world"))
