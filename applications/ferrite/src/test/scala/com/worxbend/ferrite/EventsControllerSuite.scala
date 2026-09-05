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

import com.worxbend.ferrite.web.Hx
import com.worxbend.ferrite.web.Urls
import com.worxbend.persistence.repository.EventDetail
import com.worxbend.persistence.repository.EventRef
import munit.FunSuite
import org.apache.pekko.stream.Materializer
import org.jsoup.Jsoup
import play.api.http.Status
import play.api.mvc.AnyContentAsEmpty
import play.api.mvc.Result
import play.api.test.FakeRequest
import play.api.test.Helpers
import play.api.test.Helpers.contentAsString
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.header
import play.api.test.Helpers.status
import play.api.test.Helpers.writeableOf_AnyContentAsEmpty
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

/** Fragment or page — the one decision that makes htmx work without duplicating markup.
  *
  * Every case here is a *behavioural* consequence of [[Hx.isFragment]], driven through the real controller and the real
  * templates. Asserting on the predicate alone would not catch the mistake that actually happens: a route that forgets
  * to consult it, or forgets `Vary`.
  */
final class EventsControllerSuite extends FunSuite:

  private given Materializer = Materializer(Fixtures.system)

  /** The body type is pinned to `AnyContentAsEmpty.type` rather than widened to `AnyContent`: `Helpers.call` needs a
    * `Writeable` for the request body, and only the singleton type has one.
    */
  private def get(url: String, headers: (String, String)*): FakeRequest[AnyContentAsEmpty.type] =
    FakeRequest("GET", url).withHeaders(headers*)

  private def body(result: Future[Result]): String = contentAsString(result)

  test("a browser navigation gets the whole document"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val result = Helpers.call(controller.list, get(Urls.events("")))
    assertEquals(status(result), Status.OK)
    val html = body(result)
    assert(html.contains("<!DOCTYPE html>"), "expected a full document")
    assert(html.contains("<html"), "expected a full document")

  test("an htmx request gets only the results region, with no document shell"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val result = Helpers.call(controller.list, get(Urls.events(""), Hx.RequestHeader -> "true"))
    assertEquals(status(result), Status.OK)
    val html = body(result)
    assert(!html.contains("<html"), "a fragment must never emit a document shell")
    assert(html.contains("event-rows"), "expected the results table body")

  test("an htmx history-restore request gets the whole document, not a fragment"):
    // htmx re-fetches the full page when its history cache misses, and marks that request with BOTH headers.
    // Answering it with a fragment replaces the entire document and poisons every later back-navigation.
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val request = get(Urls.events(""), Hx.RequestHeader -> "true", Hx.HistoryRestoreHeader -> "true")
    val html = body(Helpers.call(controller.list, request))
    assert(html.contains("<html"), "a history-restore request must get the full page")

  test("both representations of the same URL advertise Vary: HX-Request"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val page = Helpers.call(controller.list, get(Urls.events("")))
    val fragment = Helpers.call(controller.list, get(Urls.events(""), Hx.RequestHeader -> "true"))
    assertEquals(header(Hx.Vary, page), Some(Hx.RequestHeader))
    assertEquals(header(Hx.Vary, fragment), Some(Hx.RequestHeader))

  test("an htmx request carrying a cursor gets rows only — no histogram, no facets, no count"):
    val repository = Fixtures.StubRepository(nextCursor = Some("NEXT"))
    val controller = Fixtures.controller(repository)
    val first = SearchQueryFixtures.cursorFor("v=1&type=com.worxbend.iot.telemetry")
    val request = get(Urls.events(s"v=1&type=com.worxbend.iot.telemetry&cursor=$first"), Hx.RequestHeader -> "true")
    val html = body(Helpers.call(controller.list, request))
    // Bare <tr> elements are dropped by an HTML parser outside a table, so the fragment is parsed in the context
    // it is actually swapped into.
    val document = Jsoup.parseBodyFragment(s"<table><tbody>$html</tbody></table>")
    assert(document.select("tr.event-row").size() > 0, "expected rows")
    assert(document.select(".histogram").isEmpty, "a paging fragment must not re-render the histogram")
    assert(document.select(".facets").isEmpty, "a paging fragment must not re-render the facets")
    assert(document.select("#load-more").size() == 1, "expected exactly one fresh sentinel")

  test("a non-htmx request carrying a cursor still gets the whole page, so the link stays shareable"):
    val repository = Fixtures.StubRepository(nextCursor = Some("NEXT"))
    val controller = Fixtures.controller(repository)
    val cursor = SearchQueryFixtures.cursorFor("v=1&type=com.worxbend.iot.telemetry")
    val html =
      body(Helpers.call(controller.list, get(Urls.events(s"v=1&type=com.worxbend.iot.telemetry&cursor=$cursor"))))
    assert(html.contains("<html"), "expected a full page")

  test("the fragment response pushes the canonical permalink so the address bar matches what is shown"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val result = Helpers.call(controller.list, get(Urls.events("v=1&type=a"), Hx.RequestHeader -> "true"))
    val pushed = header(Hx.PushUrlHeader, result).getOrElse(fail("expected a push-url header"))
    assert(pushed.startsWith(s"${Urls.Events}?"), pushed)
    assertEquals(
      search.SearchQuery.parse(pushed.drop(Urls.Events.length + 1)).map(_.filter),
      search.SearchQuery.parse("v=1&type=a").map(_.filter)
    )

  test("the URL a filter-bar submit produces is a search, end to end"):
    // Not a constructed query string: the one the rendered form serialises. Every earlier assertion about the bar is
    // about a piece of this loop, and the loop is what a user does — type in the box, get results, keep the sort.
    val repository = Fixtures.StubRepository()
    val controller = Fixtures.controller(repository)
    val arrived = Jsoup.parse(body(Helpers.call(controller.list, get(Urls.events("v=1&device=kitchen-1&sort=oldest")))))
    val submit = arrived
      .select("form#filter-form input[name], form#filter-form select[name]")
      .asScala
      .toVector
      .map { control =>
        val value =
          if control.tagName() == "select" then
            Option(control.selectFirst("option[selected]")).map(_.attr("value")).getOrElse("")
          else if control.id() == "q" then "kitchen" // what the user just typed
          else control.attr("value")
        control.attr("name") -> value
      }
    val next = Helpers.call(controller.list, get(Urls.events(web.Query.render(submit))))
    // A 400 here is the whole of the original defect: `from=&until=&severity=` read as three invalid values.
    assertEquals(status(next), Status.OK, body(next))
    val request = repository.lastSearch.get().getOrElse(fail("the repository was never asked for a page"))
    // Everything the user had is still applied: the facet they clicked, the order they chose, and the text typed.
    assertEquals(request.sort, com.worxbend.persistence.search.SortDirection.Oldest)
    val leaves = request.filter.toVector.flatMap(com.worxbend.kernel.search.Filter.leaves)
    assert(
      leaves.exists {
        case com.worxbend.kernel.search.Filter.DeviceIn(vs) => vs.contains("kitchen-1"); case _ => false
      },
      leaves.toString
    )
    assert(
      leaves.exists { case com.worxbend.kernel.search.Filter.FullText(_) => true; case _ => false },
      leaves.toString
    )

  test("a rejected permalink is a 400 that still renders the filter bar with the offending value"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val result = Helpers.call(controller.list, get(Urls.events("v=1&from=yesterday")))
    assertEquals(status(result), Status.BAD_REQUEST)
    val document = Jsoup.parse(body(result))
    assertEquals(document.select("#from").attr("value"), "yesterday")
    assert(document.select(".filter-problems").size() == 1, "the rejected parameter must be named in the bar")

  test("a rejected permalink over htmx is a 400 fragment, so the error lands in the results region"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val result = Helpers.call(controller.list, get(Urls.events("v=1&from=yesterday"), Hx.RequestHeader -> "true"))
    assertEquals(status(result), Status.BAD_REQUEST)
    val html = body(result)
    assert(!html.contains("<html"), "an htmx error must still be a fragment")
    assert(html.contains("could not be read"), html)

  test("the querystring reaches the repository as a filter, a sort and a limit"):
    val repository = Fixtures.StubRepository()
    val controller = Fixtures.controller(repository)
    // `status` awaits, which matters: the recorded request is written while the action's Future is running.
    val result = Helpers.call(controller.list, get(Urls.events("v=1&device=kitchen-1&limit=7&sort=oldest")))
    assertEquals(status(result), Status.OK)
    val request = repository.lastSearch.get().getOrElse(fail("the repository was never asked for a page"))
    assertEquals(request.limit, 7)
    assertEquals(request.sort, com.worxbend.persistence.search.SortDirection.Oldest)
    assert(request.filter.isDefined, "the device filter must reach the repository")
    // The bounded count and the facet pass see the same filter — a page whose total described a different search
    // would be worse than no total at all.
    assertEquals(repository.lastCount.get().map(_._1), Some(request.filter))
    assertEquals(repository.lastFacets.get().map(_.filter), Some(request.filter))

  test("the detail route needs both halves of the partitioned primary key"):
    val summary = Fixtures.summary()
    val repository = Fixtures.StubRepository(detail = Some(EventDetail(summary, Fixtures.rawEvent)))
    val controller = Fixtures.controller(repository)
    val url = Urls.event(summary.occurredAt, summary.eventUid, "")
    val result = Helpers.call(controller.detail(summary.eventUid.toString), get(url))
    assertEquals(status(result), Status.OK)
    assertEquals(repository.lastRef.get(), Some(EventRef(summary.occurredAt, summary.eventUid)))

  test("a drill-down and the way back both carry the search the operator was looking at"):
    // `backUrl` reconstructs the list URL from the detail request's own query string, so the drill-down link is the
    // only thing that can carry the filter. Emitting `?at=…` alone made "Back to results" land on an unfiltered
    // list every time — an operator triaging a filtered feed lost the filter on every event they opened.
    val summary = Fixtures.summary()
    val repository = Fixtures.StubRepository(detail = Some(EventDetail(summary, Fixtures.rawEvent)))
    val controller = Fixtures.controller(repository)
    val list = Jsoup.parse(body(Helpers.call(controller.list, get(Urls.events("v=1&device=kitchen-1&sort=oldest")))))
    val detailUrl = list.select("tr.event-row a.row-open").attr("href")
    assert(detailUrl.contains("device=kitchen-1"), s"the drill-down dropped the search: $detailUrl")

    val page = Jsoup.parse(body(Helpers.call(controller.detail(summary.eventUid.toString), get(detailUrl))))
    val back = page.select(".detail-header a.button").attr("href")
    val restored = search.SearchQuery.parse(back.dropWhile(_ != '?').drop(1)).getOrElse(fail(s"back url: $back"))
    assertEquals(restored.filter, search.SearchQuery.parse("v=1&device=kitchen-1").getOrElse(fail("filter")).filter)
    assertEquals(restored.sort, com.worxbend.persistence.search.SortDirection.Oldest)

  test("a repository failure is rendered like every other response, not swapped in as a whole document"):
    // A 2 s statement timeout or an exhausted pool used to propagate out of the action, so Play's error handler
    // answered with a full <html> 500 carrying no Vary — which htmx then spliced inside <section id="results">,
    // and which a shared proxy may cache against the un-varied URL.
    val controller = Fixtures.controller(Fixtures.FailingRepository())
    val fragment = Helpers.call(controller.list, get(Urls.events("v=1"), Hx.RequestHeader -> "true"))
    assertEquals(status(fragment), Status.SERVICE_UNAVAILABLE)
    assertEquals(header(Hx.Vary, fragment), Some(Hx.RequestHeader))
    assert(!body(fragment).contains("<html"), "an htmx failure must not splice a document into the results region")

    val page = Helpers.call(controller.list, get(Urls.events("v=1")))
    assertEquals(status(page), Status.SERVICE_UNAVAILABLE)
    assertEquals(header(Hx.Vary, page), Some(Hx.RequestHeader))
    assert(body(page).contains("<html"), "a browser navigation still gets a document")

  test("a detail lookup that fails is the same rendered failure, not an unhandled exception"):
    val controller = Fixtures.controller(Fixtures.FailingRepository())
    val url = Urls.event(Fixtures.Now, Fixtures.FirstUid, "")
    val result = Helpers.call(controller.detail(Fixtures.FirstUid.toString), get(url))
    assertEquals(status(result), Status.SERVICE_UNAVAILABLE)
    assertEquals(header(Hx.Vary, result), Some(Hx.RequestHeader))

  test("a detail URL without 'at' is a bad request, not a full-partition scan"):
    val controller = Fixtures.controller(Fixtures.StubRepository())
    val result = Helpers.call(controller.detail(Fixtures.FirstUid.toString), get(s"/events/${Fixtures.FirstUid}"))
    assertEquals(status(result), Status.BAD_REQUEST)

  test("an unknown event is a 404 with a way back"):
    val controller = Fixtures.controller(Fixtures.StubRepository(detail = None))
    val url = Urls.event(Fixtures.Now, Fixtures.FirstUid, "")
    val result = Helpers.call(controller.detail(Fixtures.FirstUid.toString), get(url))
    assertEquals(status(result), Status.NOT_FOUND)
    assert(body(result).contains("No such event"))

/** Mints a cursor that belongs to a given query, so paging tests exercise the real fingerprint check. */
object SearchQueryFixtures:

  def cursorFor(queryString: String): String =
    val query = search.SearchQuery.parse(queryString).getOrElse(throw IllegalArgumentException(queryString))
    val request = query.toRequest.getOrElse(throw IllegalArgumentException(queryString))
    com.worxbend.persistence.search.Cursor(Fixtures.Now, Fixtures.FirstUid, request.fingerprint).encode
