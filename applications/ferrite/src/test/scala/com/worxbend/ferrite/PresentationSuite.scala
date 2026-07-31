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

import com.worxbend.ferrite.search.SearchOutcome
import com.worxbend.ferrite.search.SearchQuery
import com.worxbend.ferrite.search.SearchService
import com.worxbend.ferrite.search.TimeWindow
import com.worxbend.ferrite.web.Hx
import com.worxbend.ferrite.web.Query
import com.worxbend.ferrite.web.view.Format
import com.worxbend.ferrite.web.view.Presenter
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.Observation
import com.worxbend.persistence.repository.FacetDimension
import com.worxbend.persistence.repository.SearchPage
import java.time.OffsetDateTime
import munit.FunSuite
import play.api.mvc.Results
import play.api.test.FakeRequest
import scala.concurrent.duration.DurationInt

/** The presentation layer, exercised without a database, a browser or a Play application.
  *
  * Everything a template displays is decided here, which is the whole reason [[Presenter]] exists: a decision made in
  * Twirl is a decision no test can reach.
  */
final class PresentationSuite extends FunSuite:

  private val now = Fixtures.Now

  // ------------------------------------------------------------------------------------------------ formatting

  test("relative time is terse, and a future timestamp is shown as future rather than clamped"):
    assertEquals(Format.relative(now, now), "just now")
    assertEquals(Format.relative(now.minusMinutes(3), now), "3 m ago")
    assertEquals(Format.relative(now.minusHours(5), now), "5 h ago")
    assertEquals(Format.relative(now.minusDays(2), now), "2 d ago")
    // A clock-skewed device is a real diagnosis; hiding it behind "just now" would suppress the evidence.
    assertEquals(Format.relative(now.plusHours(3), now), "in 3 h")

  test("counts above the cap are rendered with a plus and never as a number"):
    assertEquals(Format.boundedCount(9999L, 10000L), "9,999")
    assertEquals(Format.boundedCount(10001L, 10000L), "10,000+")

  test("a non-zero bucket never rounds away to nothing"):
    assertEquals(Format.heightPercent(0L, 100L), 0)
    assertEquals(Format.heightPercent(1L, 100000L), 2)
    assertEquals(Format.heightPercent(100L, 100L), 100)
    assertEquals(Format.heightPercent(5L, 0L), 0)

  test("absent values render as an em dash, so an empty cell is never ambiguous"):
    assertEquals(Format.orAbsent(None), Format.Absent)
    assertEquals(Format.orAbsent(Some("")), Format.Absent)
    assertEquals(Format.orAbsent(Some("kitchen-1")), "kitchen-1")

  // --------------------------------------------------------------------------------------------------- rows

  test("severity aliases colour the way they rank, so the UI agrees with its own filter"):
    assertEquals(Presenter.tone(Some("warning")), "warn")
    assertEquals(Presenter.tone(Some("WARN")), "warn")
    assertEquals(Presenter.tone(Some("emerg")), "fatal")
    assertEquals(Presenter.tone(None), Presenter.UnknownSeverity)
    assertEquals(Presenter.tone(Some("not-a-severity")), Presenter.UnknownSeverity)

  test("a row links to a detail URL carrying both halves of the primary key"):
    val row = Presenter.row(Fixtures.summary(), now)
    assert(row.detailUrl.startsWith(s"/events/${Fixtures.FirstUid}?"), row.detailUrl)
    assert(row.detailUrl.contains("at="), row.detailUrl)
    assertEquals(row.relative, "4 m ago")

  // ----------------------------------------------------------------------------------------------- histogram

  test("bar heights are stylesheet classes, quantised and prefixed away from Tailwind's own utilities"):
    assertEquals(Presenter.heightClass(0), "bar-h-0")
    assertEquals(Presenter.heightClass(2), "bar-h-5")
    assertEquals(Presenter.heightClass(100), "bar-h-100")
    assertEquals(Presenter.heightClass(43), "bar-h-45")

  test("each bar narrows the search to its own half-open window"):
    val query = SearchQuery.parse("v=1&type=a").getOrElse(fail("query"))
    val histogram = Presenter.histogram(Fixtures.buckets, 1.hour, now.minusHours(2), now, query)
    assertEquals(histogram.bars.size, 3)
    assertEquals(histogram.empty, false)
    val first = histogram.bars.head
    val pairs = Query.parse(first.url.dropWhile(_ != '?').drop(1))
    assertEquals(pairs.collectFirst { case ("from", v) => v }, Some("2026-07-26T10:00:00Z"))
    assertEquals(pairs.collectFirst { case ("until", v) => v }, Some("2026-07-26T11:00:00Z"))

  test("an all-zero window is flagged rather than drawn as 90 invisible bars"):
    val query = SearchQuery.parse("").getOrElse(fail("query"))
    val flat = Fixtures.buckets.map(_.copy(count = 0L))
    assertEquals(Presenter.histogram(flat, 1.hour, now.minusHours(2), now, query).empty, true)

  // -------------------------------------------------------------------------------------------------- facets

  test("every dimension is emitted even when empty, so the panel does not reflow as the search narrows"):
    val query = SearchQuery.parse("v=1").getOrElse(fail("query"))
    val facets = Presenter.facets(Fixtures.facets, query)
    assertEquals(facets.map(_.key), Presenter.FacetKeys.map((_, key) => key) :+ Presenter.TagKey)
    assertEquals(facets.find(_.key == "person").map(_.entries.isEmpty), Some(true))

  test("a selected facet value is marked and its link removes it again"):
    val query = SearchQuery.parse("v=1&device=kitchen-1").getOrElse(fail("query"))
    val device = Presenter.facets(Fixtures.facets, query).find(_.key == "device").getOrElse(fail("device facet"))
    val selected = device.entries.find(_.value == "kitchen-1").getOrElse(fail("kitchen-1"))
    assertEquals(selected.selected, true)
    assert(!selected.url.contains("device=kitchen-1"), selected.url)
    val other = device.entries.find(_.value == "hall-2").getOrElse(fail("hall-2"))
    assertEquals(other.selected, false)
    assert(other.url.contains("device=hall-2"), other.url)

  test("severity is a threshold, not a set, so its facet link sets '>=' rather than accumulating values"):
    val query = SearchQuery.parse("v=1").getOrElse(fail("query"))
    val severity = Presenter.facets(Fixtures.facets, query).find(_.key == "severity").getOrElse(fail("severity"))
    val warn = severity.entries.find(_.value == "warn").getOrElse(fail("warn"))
    assert(warn.url.contains("severity=%3E%3Dwarn"), warn.url)

  test("capped facet counts are rendered as lower bounds"):
    val query = SearchQuery.parse("v=1").getOrElse(fail("query"))
    val capped = Fixtures.facets.copy(capped = true)
    val types = Presenter.facets(capped, query).find(_.key == "type").getOrElse(fail("type facet"))
    assertEquals(types.entries.head.count, "12+")

  // ---------------------------------------------------------------------------------------------- filter bar

  test("the filter bar shows what was typed, and carries everything it has no input for as a hidden field"):
    val query = SearchQuery.lenient("v=1&q=kitchen&from=yesterday&type=a&device=kitchen-1")
    val bar = Presenter.filterBar(query, Vector.empty)
    assertEquals(bar.text, "kitchen")
    assertEquals(bar.from, "yesterday")
    assertEquals(bar.hidden, Vector("type" -> "a", "device" -> "kitchen-1"))
    assertEquals(bar.chips.map(_.value), Vector("a", "kitchen-1"))

  test("severity is shown without its '>=' prefix, because the label already says 'at least'"):
    val query = SearchQuery.lenient("v=1&severity=>=warn")
    assertEquals(Presenter.filterBar(query, Vector.empty).severity, "warn")

  test("each rejected parameter is reported against the input that produced it"):
    val errors = SearchQuery.parse("v=1&from=yesterday").swap.getOrElse(Vector.empty)
    val problems = Presenter.problems(errors)
    assertEquals(problems.map(_.parameter), Vector("from"))
    assert(problems.head.message.contains("yesterday"), problems.head.message)

  // --------------------------------------------------------------------------------------------------- results

  test("the empty state distinguishes 'nothing ingested' from 'this filter matched nothing'"):
    val unfiltered = SearchQuery.parse("").getOrElse(fail("query"))
    val filtered = SearchQuery.parse("v=1&device=kitchen-1").getOrElse(fail("query"))
    val outcome = SearchOutcome(
      SearchPage(Vector.empty, None),
      SearchService.EmptyFacets,
      Vector.empty,
      0L,
      TimeWindow(now.minusHours(24), now),
      15.minutes
    )
    assert(Presenter.emptiness(unfiltered, outcome).headline.contains("last 24 hours"))
    assert(Presenter.emptiness(filtered, outcome).headline.contains("match this search"))
    assert(Presenter.emptiness(filtered, outcome).suggestions.nonEmpty)

  test("the results view reports an approximate total honestly"):
    val query = SearchQuery.parse("").getOrElse(fail("query"))
    val over = SearchOutcome(
      SearchPage(Vector(Fixtures.summary()), None),
      Fixtures.facets,
      Fixtures.buckets,
      SearchService.TotalCap.toLong + 1,
      TimeWindow(now.minusHours(24), now),
      1.hour
    )
    val results = Presenter.results(over, query, now)
    assertEquals(results.totalIsExact, false)
    assertEquals(results.totalLabel, "10,000+")

  test("the sort control flips direction and announces it"):
    val query = SearchQuery.parse("v=1&type=a").getOrElse(fail("query"))
    val outcome = SearchOutcome(
      SearchPage(Vector(Fixtures.summary()), None),
      Fixtures.facets,
      Fixtures.buckets,
      3L,
      TimeWindow(now.minusHours(24), now),
      1.hour
    )
    val results = Presenter.results(outcome, query, now)
    assertEquals(results.sortAria, "descending")
    assert(results.sortUrl.contains("sort=oldest"), results.sortUrl)

  // ---------------------------------------------------------------------------------------------------- detail

  test("a decodable event shows its refined observation"):
    val envelope = Envelope.decoder.decodeJson(Fixtures.rawEvent).getOrElse(fail("expected a CloudEvent"))
    val view = Presenter.observation(Observation.from(envelope))
    assertEquals(view.kind, "Telemetry")
    assertEquals(view.unrecognised, false)
    assertEquals(view.fields.map(_.label), Vector("device", "metric", "value", "unit"))

  test("an unrecognised event is not an error state and still carries a reason"):
    val raw = Fixtures.rawEvent.mapObject(_.add("type", io.circe.Json.fromString("com.example.never-seen")))
    val envelope = Envelope.decoder.decodeJson(raw).getOrElse(fail("expected a CloudEvent"))
    val view = Presenter.observation(Observation.from(envelope))
    assertEquals(view.kind, "Unrecognised")
    assertEquals(view.unrecognised, true)
    assert(view.reason.nonEmpty)

  // -------------------------------------------------------------------------------------------------------- hx

  test("only a genuine htmx request, and never a history restore, asks for a fragment"):
    assertEquals(Hx.isFragment(FakeRequest("GET", "/events")), false)
    assertEquals(Hx.isFragment(FakeRequest("GET", "/events").withHeaders(Hx.RequestHeader -> "true")), true)
    assertEquals(Hx.isFragment(FakeRequest("GET", "/events").withHeaders(Hx.RequestHeader -> "TRUE")), true)
    assertEquals(Hx.isFragment(FakeRequest("GET", "/events").withHeaders(Hx.RequestHeader -> "false")), false)
    assertEquals(
      Hx.isFragment(
        FakeRequest("GET", "/events")
          .withHeaders(Hx.RequestHeader -> "true", Hx.HistoryRestoreHeader -> "true")
      ),
      false
    )

  test("varyOnHx merges rather than replacing an existing Vary"):
    import com.worxbend.ferrite.web.Hx.varyOnHx
    assertEquals(Results.Ok("x").varyOnHx.header.headers.get(Hx.Vary), Some(Hx.RequestHeader))
    assertEquals(
      Results.Ok("x").withHeaders(Hx.Vary -> "Accept-Encoding").varyOnHx.header.headers.get(Hx.Vary),
      Some(s"Accept-Encoding, ${Hx.RequestHeader}")
    )

  // ---------------------------------------------------------------------------------------------------- window

  test("a filter without time bounds gets the default window; one with bounds keeps them"):
    val service = Fixtures.service(Fixtures.StubRepository())
    val unbounded = service.windowOf(None)
    assertEquals(unbounded.until, now)
    assertEquals(unbounded.from, now.minusHours(24))

    val from = OffsetDateTime.parse("2026-07-01T00:00:00Z")
    val until = OffsetDateTime.parse("2026-07-02T00:00:00Z")
    val bounded = com.worxbend.kernel.search.Filter.occurred(Some(from), Some(until)).getOrElse(fail("filter"))
    assertEquals(service.windowOf(Some(bounded)), TimeWindow(from, until))

  test("facet dimension keys cover every dimension the repository can return"):
    assertEquals(Presenter.FacetKeys.map((dimension, _) => dimension).toSet, FacetDimension.values.toSet)

  test("tags are a facet of their own, counted by a separate pass and toggled like a dimension"):
    val query = SearchQuery.parse("v=1").getOrElse(fail("query"))
    val tags = Presenter.facets(Fixtures.facets, query).find(_.key == Presenter.TagKey).getOrElse(fail("tag facet"))
    val indoor = tags.entries.find(_.value == "indoor").getOrElse(fail("indoor"))
    assertEquals(indoor.selected, false)
    assert(indoor.url.contains("tag=indoor"), indoor.url)
