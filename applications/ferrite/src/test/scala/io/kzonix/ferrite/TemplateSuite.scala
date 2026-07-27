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

package io.kzonix.ferrite

import io.kzonix.ferrite.search.SearchOutcome
import io.kzonix.ferrite.search.SearchQuery
import io.kzonix.ferrite.search.SearchService
import io.kzonix.ferrite.search.TimeWindow
import io.kzonix.ferrite.web.Csrf
import io.kzonix.ferrite.web.view.EventsPage
import io.kzonix.ferrite.web.view.Presenter
import io.kzonix.persistence.repository.EventDetail
import io.kzonix.persistence.repository.SearchPage
import munit.FunSuite
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import play.api.mvc.RequestHeader
import play.api.test.CSRFTokenHelper.*
import play.api.test.FakeRequest
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** Rendered markup, asserted **structurally** and never by string match (ADR §9.3).
  *
  * A string assertion on HTML fails the moment an attribute is reordered and passes when a `<th>` loses its `scope`,
  * which is precisely backwards. These tests parse the output and ask questions about the document: does the results
  * region announce itself, is the CSRF token where htmx will find it, is user input escaped.
  */
final class TemplateSuite extends FunSuite:

  private val now = Fixtures.Now

  private val request: RequestHeader = FakeRequest("GET", "/events").withCSRFToken

  private def page(queryString: String, rows: Vector[io.kzonix.persistence.repository.EventSummary]): Document =
    val query = SearchQuery.lenient(queryString)
    val outcome = SearchOutcome(
      SearchPage(rows, Some("NEXT")),
      Fixtures.facets,
      Fixtures.buckets,
      12L,
      TimeWindow(now.minusHours(24), now),
      1.hour
    )
    val model = EventsPage(Presenter.filterBar(query, Vector.empty), Presenter.results(outcome, query, now))
    Jsoup.parse(views.html.pages.events(model)(request).body)

  // ------------------------------------------------------------------------------------------ landmarks and a11y

  test("the page has semantic landmarks and a skip link that targets main"):
    val document = page("v=1", Vector(Fixtures.summary()))
    assertEquals(document.select("header[role=banner]").size(), 1)
    assertEquals(document.select("main#main").size(), 1)
    assertEquals(document.select("footer[role=contentinfo]").size(), 1)
    assertEquals(document.select("nav[aria-label]").size() >= 1, true)
    assertEquals(document.select("a.skip-link").attr("href"), "#main")
    // Focus has to be movable to <main> and to the results heading after a swap, or a keyboard user is returned to
    // the top of the document on every keystroke in the filter bar.
    assertEquals(document.select("main#main").attr("tabindex"), "-1")
    assertEquals(document.select("#results-heading").attr("tabindex"), "-1")

  test("the results region is a live region with a busy state"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val region = document.select("section#results")
    assertEquals(region.size(), 1)
    assertEquals(region.attr("aria-live"), "polite")
    assertEquals(region.attr("aria-busy"), "false")
    assertEquals(region.attr("aria-labelledby"), "results-heading")

  test("the results table is a real table with column headers and a caption"):
    val document = page("v=1", Vector(Fixtures.summary()))
    assertEquals(document.select("table.event-table caption.sr-only").size(), 1)
    val headers = document.select("table.event-table thead th").eachAttr("scope").asScala.toVector
    assertEquals(headers.size, 9)
    assert(headers.forall(_ == "col"), "every column header needs scope=col")
    val labels = document.select("table.event-table thead th").eachText().asScala.toVector
    assertEquals(labels, Vector("Time", "Severity", "Type", "Source", "Subject", "Device", "Room", "Person", "Value"))

  test("the sortable header is a button inside a th carrying aria-sort"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val header = document.select("table.event-table thead th.cell-time").first()
    assertEquals(header.attr("aria-sort"), "descending")
    assertEquals(header.select("button.sort-button").size(), 1)

  test("every htmx trigger is a link or a button, so all of them are keyboard reachable"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val triggers = document.select("[hx-get]")
    assert(triggers.size() > 0, "expected htmx triggers")
    triggers.asScala.foreach { element =>
      val tag = element.tagName()
      assert(tag == "a" || tag == "button" || tag == "form", s"hx-get on a <$tag> is not keyboard reachable")
    }

  // -------------------------------------------------------------------------------------------------------- csrf

  test("the CSRF token is on the body as hx-headers, once, where every htmx request inherits it"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val headers = document.select("body").attr("hx-headers")
    assert(headers.contains(Csrf.HeaderName), headers)
    assert(headers.contains(Csrf.token(request)), "the rendered token must be the request's token")
    assertEquals(document.select("[hx-headers]").size(), 1, "the token belongs in exactly one place")

  test("a request without a CSRF filter renders an empty token rather than throwing"):
    val bare: RequestHeader = FakeRequest("GET", "/events")
    val query = SearchQuery.lenient("v=1")
    val outcome = SearchOutcome(SearchPage(Vector.empty, None), SearchService.EmptyFacets, Vector.empty, 0L,
      TimeWindow(now.minusHours(24), now), 1.hour)
    val model = EventsPage(Presenter.filterBar(query, Vector.empty), Presenter.results(outcome, query, now))
    val document = Jsoup.parse(views.html.pages.events(model)(bare).body)
    assert(document.select("body").attr("hx-headers").contains(Csrf.HeaderName))

  // ------------------------------------------------------------------------------------------------- escaping

  test("user-controlled values are escaped, never emitted as markup"):
    val hostile = Fixtures.summary(subject = Some(Fixtures.hostileSubject))
    val document = page("v=1", Vector(hostile))
    // jsoup would report a real <script> element if Twirl's escaping had been defeated with @Html.
    assertEquals(document.select("td.cell-subject script").size(), 0)
    assertEquals(document.select("td.cell-subject").first().text(), Fixtures.hostileSubject)
    assertEquals(document.select("script:not([src])").size(), 0, "no inline script: the CSP forbids it")

  test("a hostile filter value is escaped in the input it is echoed into"):
    val document = page(s"v=1&q=${io.kzonix.ferrite.web.Query.encode(Fixtures.hostileSubject)}", Vector.empty)
    assertEquals(document.select("#q").attr("value"), Fixtures.hostileSubject)
    assertEquals(document.select("#q script").size(), 0)

  // ------------------------------------------------------------------------------------------------ filter bar

  test("the filter bar is a real GET form that also drives htmx, and always sends the grammar version"):
    val document = page("v=1&type=io.kzonix.iot.alarm", Vector(Fixtures.summary()))
    val form = document.select("form#filter-form").first()
    assertEquals(form.attr("method"), "get")
    assertEquals(form.attr("action"), "/events")
    assertEquals(form.attr("role"), "search")
    assertEquals(form.attr("hx-target"), "#results")
    assertEquals(form.attr("hx-push-url"), "true")
    assertEquals(form.select("input[type=hidden][name=v]").attr("value"), "1")
    // A facet selection has no visible input; without a hidden field it would vanish on the next keystroke.
    assertEquals(form.select("input[type=hidden][name=type]").attr("value"), "io.kzonix.iot.alarm")

  test("every filter input is labelled"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val controls = document.select("form#filter-form input:not([type=hidden]), form#filter-form select")
    assert(controls.size() >= 4, "expected the visible filter controls")
    controls.asScala.foreach { control =>
      val id = control.id()
      assert(id.nonEmpty, s"a filter control has no id: $control")
      assertEquals(document.select(s"label[for=$id]").size(), 1, s"no label for #$id")
    }

  test("active filters appear as dismissible chips"):
    val document = page("v=1&device=kitchen-1", Vector(Fixtures.summary()))
    val chip = document.select(".filter-chips .chip").first()
    assertEquals(chip.select(".chip-value").text(), "kitchen-1")
    assert(chip.select("a.chip-remove").attr("aria-label").nonEmpty, "the remove control must be labelled")

  // -------------------------------------------------------------------------------------------------- paging

  test("the keyset sentinel is a button, appears once, and swaps itself"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val sentinel = document.select("tr#load-more")
    assertEquals(sentinel.size(), 1)
    val button = sentinel.select("button.load-more").first()
    assertEquals(button.attr("hx-target"), "#load-more")
    assertEquals(button.attr("hx-swap"), "outerHTML")
    assert(button.attr("hx-trigger").contains("revealed"), "expected infinite scroll on top of the click trigger")
    assert(button.attr("hx-get").contains("cursor="), button.attr("hx-get"))

  // ------------------------------------------------------------------------------------------- histogram/empty

  test("the histogram is an ordered list of linked bars with screen-reader text"):
    val document = page("v=1", Vector(Fixtures.summary()))
    val bars = document.select(".histogram-bars li.histogram-bar")
    assertEquals(bars.size(), Fixtures.buckets.size)
    assert(bars.first().className().contains("bar-h-"), bars.first().className())
    assertEquals(bars.select("a .sr-only").size(), Fixtures.buckets.size)

  test("the empty state explains itself and offers a way out"):
    val document = page("v=1&device=nowhere", Vector.empty)
    val empty = document.select(".empty-state")
    assertEquals(empty.size(), 1)
    assert(empty.select(".empty-explanation").text().nonEmpty)
    assert(empty.select(".empty-suggestions a").size() >= 1, "an empty state without a next step is an apology")
    assertEquals(document.select("table.event-table").size(), 0)

  // -------------------------------------------------------------------------------------------------- detail

  test("the detail view shows the decoded observation and the raw CloudEvent"):
    val detail = Presenter.detail(EventDetail(Fixtures.summary(), Fixtures.rawEvent), now, "/events")
    val document = Jsoup.parse(views.html.pages.detail(detail)(request).body)
    assertEquals(document.select("section[aria-labelledby=observation-heading] .observation-kind").text(), "Telemetry")
    assertEquals(document.select("section[aria-labelledby=raw-heading] pre.raw-json").size(), 1)
    // The raw JSON is database content: it must arrive as text, not as markup.
    val raw = document.select("pre.raw-json code").text()
    assert(raw.contains("\"specversion\""), raw)
    assertEquals(document.select("pre.raw-json script").size(), 0)
    assert(document.select("section[aria-labelledby=extensions-heading]").text().contains("tenantid"))

  // -------------------------------------------------------------------------------------------------- failure

  test("the error page names the rejected parameters and links to a search that works"):
    val errors = SearchQuery.parse("v=1&from=yesterday").swap.getOrElse(Vector.empty)
    val model = Presenter.badQuery(errors)
    val bar = Presenter.filterBar(SearchQuery.lenient("v=1&from=yesterday"), errors)
    val document = Jsoup.parse(views.html.pages.failure(model, Some(bar))(request).body)
    assertEquals(document.select(".failure[role=alert]").size(), 1)
    assertEquals(document.select(".failure-status").text(), "400")
    assert(document.select(".failure-problems").text().contains("from"))
    assert(document.select(".failure-suggestions a").size() >= 1)
    assertEquals(document.select("#from").attr("value"), "yesterday")

  // ------------------------------------------------------------------------------------- fragment vs page shape

  test("a fragment emits no document shell and no duplicate results region"):
    val query = SearchQuery.lenient("v=1")
    val outcome = SearchOutcome(SearchPage(Vector(Fixtures.summary()), None), Fixtures.facets, Fixtures.buckets, 3L,
      TimeWindow(now.minusHours(24), now), 1.hour)
    val fragment = views.html.fragments.results(Presenter.results(outcome, query, now)).body
    assert(!fragment.contains("<html"), "a fragment must never emit a document shell")
    assert(!fragment.contains("id=\"results\""), "the live region belongs to the page and must survive the swap")
    assert(fragment.contains("event-rows"))
