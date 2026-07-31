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

import com.worxbend.ferrite.overview.OverviewRange
import com.worxbend.ferrite.search.SearchQuery
import com.worxbend.ferrite.web.Urls
import com.worxbend.ferrite.web.view.OverviewPresenter
import com.worxbend.persistence.repository.RollupStep
import munit.FunSuite
import org.apache.pekko.stream.Materializer
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import play.api.http.Status
import play.api.mvc.AnyContentAsEmpty
import play.api.test.FakeRequest
import play.api.test.Helpers
import play.api.test.Helpers.contentAsString
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.status
import play.api.test.Helpers.writeableOf_AnyContentAsEmpty
import scala.jdk.CollectionConverters.*

/** The overview: the rollup's reader, and the page it feeds.
  *
  * Two properties are worth more than the rest here and most of the suite is about them.
  *
  * **It must read the rollup and not the fact table.** That is the entire justification for the materialized view; a
  * page that quietly went back to `events.cloud_event` would look identical and cost hundreds of times more, and
  * nothing but a test that counts who was asked can tell the difference.
  *
  * **Every element must be a way into `/events`, over the same window.** A dashboard number that links to a different
  * period shows a different total when clicked, and a user is right to conclude the dashboard is wrong.
  */
final class OverviewSuite extends FunSuite:

  private given Materializer = Materializer(Fixtures.system)

  private def get(url: String): FakeRequest[AnyContentAsEmpty.type] = FakeRequest("GET", url)

  private def render(url: String = Urls.Root): Document =
    val controller = Fixtures.overviewController()
    val result = Helpers.call(controller.index, get(url))
    assertEquals(status(result), Status.OK)
    Jsoup.parse(contentAsString(result))

  // ------------------------------------------------------------------------------------------------ the data path

  test("the page is served from the rollup, and the fact table is touched only for the alert feed"):
    val rollup = Fixtures.StubOverviewRepository()
    val events = Fixtures.StubRepository()
    val controller = Fixtures.overviewController(rollup, events)
    assertEquals(status(Helpers.call(controller.index, get(Urls.Root))), Status.OK)

    assert(rollup.lastRequest.get().isDefined, "the volume series did not come from the rollup")
    assertEquals(
      rollup.asked.get().sorted(Ordering.by(_.ordinal)),
      Vector(
        com.worxbend.persistence.repository.RollupDimension.Type,
        com.worxbend.persistence.repository.RollupDimension.Source,
        com.worxbend.persistence.repository.RollupDimension.Severity
      ).sorted(Ordering.by(_.ordinal)),
      "every breakdown must come from the rollup"
    )
    // One fact-table read, and it is the alert feed: newest-first, bounded, and over the page's own window.
    val search = events.lastSearch.get().getOrElse(fail("the alert feed never queried the fact table"))
    assertEquals(search.sort, com.worxbend.persistence.search.SortDirection.Newest)
    assertEquals(search.limit, com.worxbend.ferrite.overview.OverviewService.AlertCount)
    assert(search.filter.isDefined, "the alert feed must be filtered, not a bare 'newest events'")
    // The facet and histogram passes belong to search and have no business running for a dashboard.
    assertEquals(events.lastFacets.get(), None)
    assertEquals(events.lastHistogram.get(), None)

  test("the range parameter chooses the window and the bucket width, and an unknown one falls back"):
    def windowOf(url: String): (Long, RollupStep) =
      val rollup = Fixtures.StubOverviewRepository()
      val controller = Fixtures.overviewController(rollup)
      assertEquals(status(Helpers.call(controller.index, get(url))), Status.OK)
      val request = rollup.lastRequest.get().getOrElse(fail("no overview request was built"))
      (java.time.Duration.between(request.from, request.until).toHours, request.step)

    val (dayHours, dayStep) = windowOf(Urls.Root)
    assertEquals(dayStep, RollupStep.Hour)
    assert(dayHours >= 24 && dayHours < 25, s"expected roughly a day, got $dayHours hours")

    val (monthHours, monthStep) = windowOf(Urls.overview(s"${OverviewRange.Key}=30d"))
    assertEquals(monthStep, RollupStep.Day)
    assert(monthHours >= 720 && monthHours < 744, s"expected roughly 30 days, got $monthHours hours")

    // A hand-edited range is not an error: the page says which range it is showing, so the worst case is that it
    // shows the default one. Refusing to render a dashboard over a typo would be the worse outcome.
    val (fallbackHours, fallbackStep) = windowOf(Urls.overview(s"${OverviewRange.Key}=fortnight"))
    assertEquals(fallbackStep, OverviewRange.Default.step)
    assertEquals(fallbackHours, dayHours)

  test("the window start is aligned to the bucket grid, so bars do not shift between reloads"):
    val rollup = Fixtures.StubOverviewRepository()
    val controller = Fixtures.overviewController(rollup)
    assertEquals(status(Helpers.call(controller.index, get(Urls.Root))), Status.OK)
    val request = rollup.lastRequest.get().getOrElse(fail("no overview request was built"))
    assertEquals(request.from.getMinute, 0)
    assertEquals(request.from.getSecond, 0)

  // ------------------------------------------------------------------------------------------------------ links

  test("every tile links to a search over the page's own window"):
    val document = render()
    val tiles = document.select(".tiles .tile a.tile-link")
    assert(tiles.size() >= 3, "expected the headline tiles")
    tiles.asScala.foreach { tile =>
      val href = tile.attr("href")
      assert(href.startsWith(s"${Urls.Events}?"), href)
      val query = SearchQuery.parse(href.drop(Urls.Events.length + 1)).getOrElse(fail(s"unparseable link: $href"))
      assert(query.raw.exists((key, _) => key == SearchQuery.FromKey), href)
      assert(query.raw.exists((key, _) => key == SearchQuery.UntilKey), href)
    }

  test("the errors tile narrows to the same severity the alert feed uses"):
    val document = render()
    val href = document.select(".tiles .tile a.tile-link").asScala.toVector(1).attr("href")
    val query = SearchQuery.parse(href.drop(Urls.Events.length + 1)).getOrElse(fail(href))
    assertEquals(
      query.raw.collectFirst { case (SearchQuery.SeverityKey, value) => value },
      Some(s">=${com.worxbend.ferrite.overview.OverviewService.AlertLevel.label}")
    )

  test("a breakdown row is a link to the filtered search, and carries the dimension's own parameter"):
    val document = render()
    val row = document.select("section[aria-labelledby=panel-source] .meter-row a.meter-label").first()
    assert(row != null, "expected a source breakdown row")
    val href = row.attr("href")
    val query = SearchQuery.parse(href.drop(Urls.Events.length + 1)).getOrElse(fail(href))
    assertEquals(
      query.raw.collectFirst { case ("source", value) => value },
      Some("https://gateway.worxbend.io/hall")
    )

  test("a severity the filter grammar cannot express is rendered as text, not as a link that 400s"):
    // The rollup stores `coalesce(severity, 'none')`, and there is no `severity=>=none`. A link would be a 400 the
    // moment anybody clicked it.
    val document = render()
    val labels = document.select("section[aria-labelledby=panel-severity] .meter-label").asScala.toVector
    val none = labels.find(_.text() == "none").getOrElse(fail("expected the 'none' severity row"))
    assertEquals(none.tagName(), "span")
    val error = labels.find(_.text() == "error").getOrElse(fail("expected the 'error' severity row"))
    assertEquals(error.tagName(), "a")
    assert(error.attr("href").contains("severity"), error.attr("href"))

  test("every volume bar links to its own bucket, not to the whole window"):
    val document = render()
    val bars = document.select(".volume .histogram-bar a")
    assertEquals(bars.size(), Fixtures.volume.size)
    val hrefs = bars.eachAttr("href").asScala.toVector
    assertEquals(hrefs.distinct.size, hrefs.size, "two bars linked to the same window")
    hrefs.foreach(href => assert(href.contains("from=") && href.contains("until="), href))

  test("the overview drives nothing through htmx, because it has no results region to swap"):
    // An hx-target that matches nothing is a click that silently does nothing — the worst possible failure for a
    // control, because it looks like the data is missing rather than like the page is wrong.
    val document = render()
    assertEquals(document.select("[hx-get]").size(), 0)
    assertEquals(document.select("[hx-target]").size(), 0)

  // ----------------------------------------------------------------------------------------------------- honesty

  test("the page always states how fresh the rollup is"):
    val document = render()
    assert(document.select(".overview-freshness").text().nonEmpty)

  test("an unrefreshed rollup is called out as an operational fact, not left to be inferred from zeroes"):
    val rollup = Fixtures.StubOverviewRepository(newest = None)
    val controller = Fixtures.overviewController(rollup)
    val document = Jsoup.parse(contentAsString(Helpers.call(controller.index, get(Urls.Root))))
    val note = document.select(".overview-freshness")
    assertEquals(note.attr("role"), "alert")
    assert(note.text().contains("no rows"), note.text())

  test("a rollup that merely looks old is not called an alarm"):
    // On a quiet system the newest bucket being hours old is the correct answer. An alarm that fires on quiet is an
    // alarm people turn off.
    val note = OverviewPresenter.freshness(Some(Fixtures.Now.minusDays(3)))
    assert(note.contains("rollup-refresh"), note)
    assertEquals(OverviewPresenter.page(overviewValue, Fixtures.Now).stale, false)

  // ----------------------------------------------------------------------------------------------- accessibility

  test("the page has landmarks, a focusable heading and a labelled range selector"):
    val document = render()
    assertEquals(document.select("main#main").size(), 1)
    assertEquals(document.select("#overview-heading").attr("tabindex"), "-1")
    assertEquals(document.select("nav.range-picker[aria-label]").size(), 1)
    assertEquals(document.select("nav.range-picker a[aria-current=page]").size(), 1)
    // Every panel is a section named by its own heading, so a screen reader can jump between them.
    document.select("section.panel").asScala.foreach { panel =>
      val labelled = panel.attr("aria-labelledby")
      assert(labelled.nonEmpty, "a panel with no accessible name")
      assertEquals(document.select(s"#$labelled").size(), 1, s"no heading with id $labelled")
    }

  test("the nav marks the overview as the current page"):
    val document = render()
    assertEquals(document.select("nav.app-nav a[aria-current=page]").text(), "Overview")

  private def overviewValue: com.worxbend.ferrite.overview.Overview =
    com.worxbend.ferrite.overview.Overview(
      range = OverviewRange.Default,
      window = com.worxbend.ferrite.search.TimeWindow(Fixtures.Now.minusHours(24), Fixtures.Now),
      totals = com.worxbend.persistence.repository.RollupTotals(17L, 5L),
      volume = Fixtures.volume,
      types = Fixtures.slices(com.worxbend.persistence.repository.RollupDimension.Type),
      sources = Fixtures.slices(com.worxbend.persistence.repository.RollupDimension.Source),
      severities = Fixtures.slices(com.worxbend.persistence.repository.RollupDimension.Severity),
      alerts = Vector(Fixtures.summary()),
      freshness = Some(Fixtures.Now.minusDays(3))
    )
