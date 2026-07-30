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

import io.kzonix.ferrite.search.SearchQuery
import io.kzonix.ferrite.search.SearchShape
import io.kzonix.kernel.search.Filter
import io.kzonix.kernel.search.UserText
import io.kzonix.observability.Meters
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.OffsetDateTime
import munit.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/** The query-shape tag and the two search meters.
  *
  * The shape tag is the part worth testing hardest. It is a Prometheus label derived from user input, and the failure
  * mode is not a wrong number on a panel — it is an unbounded label set, which degrades the whole Prometheus instance
  * and not just this dashboard.
  */
final class SearchMetricsSuite extends FunSuite:

  private def shapeOf(queryString: String): String =
    SearchShape.of(SearchQuery.parse(queryString).getOrElse(fail(s"unparseable: $queryString")).filter)

  test("the empty filter has its own shape, because it is the baseline query"):
    assertEquals(shapeOf(""), SearchShape.Unfiltered)

  test("a time bound is the shape that decides partition pruning"):
    assertEquals(shapeOf("?v=1&from=2026-01-01T00:00:00Z"), SearchShape.Time)

  test("attribute filters collapse into one token — they are all cheap equality"):
    assertEquals(shapeOf("?v=1&type=io.kzonix.iot.alarm"), SearchShape.Attrs)
    assertEquals(shapeOf("?v=1&device=kitchen-1&room=kitchen&severity=%3E%3Dwarn"), SearchShape.Attrs)

  test("tokens are emitted in a fixed order, so one shape cannot have two spellings"):
    val filter = Filter
      .and(
        Vector(
          Filter.FullText(UserText("boiler").getOrElse(fail("unusable text"))),
          Filter.occurred(Some(OffsetDateTime.parse("2026-01-01T00:00:00Z")), None).getOrElse(fail("bad range"))
        )
      )
      .getOrElse(fail("unusable filter"))
    assertEquals(SearchShape.of(Some(filter)), s"${SearchShape.Time}+${SearchShape.Text}")

  test("every shape a filter can produce is in the enumerated set"):
    val queries = Vector(
      "",
      "?v=1&type=a",
      "?v=1&from=2026-01-01T00:00:00Z",
      "?v=1&q=boiler",
      "?v=1&q=boiler&from=2026-01-01T00:00:00Z&device=kitchen-1"
    )
    queries.foreach: query =>
      val shape = shapeOf(query)
      assert(SearchShape.all.contains(shape), s"$query produced $shape, which is outside the enumerated set")

  test("the enumerated set is small enough to be a Prometheus label"):
    // Four plan-relevant dimensions, so sixteen combinations with the empty one renamed. If this number grows, the
    // reason had better be a new access path and not a new filter leaf.
    assertEquals(SearchShape.all.size, 16)

  test("a search records its duration under the shape it ran with"):
    val registry = SimpleMeterRegistry()
    val query = SearchQuery.parse("?v=1&type=io.kzonix.iot.alarm").getOrElse(fail("unparseable"))
    val service = Fixtures.service(Fixtures.StubRepository(), registry)
    val result = Await.result(service.search(query), 5.seconds)
    assert(result.isRight, result.toString)

    val timer = Option(registry.find(Meters.SearchQueryDuration).tag(Meters.TagKeys.Shape, SearchShape.Attrs).timer())
    assertEquals(timer.map(_.count()), Some(1L))

  test("capped facets are counted, because a silently approximate count gets quoted in a meeting"):
    val registry = SimpleMeterRegistry()
    val query = SearchQuery.parse("").getOrElse(fail("unparseable"))
    val capped = Fixtures.facets.copy(capped = true)
    val service = Fixtures.service(Fixtures.StubRepository(facetResult = capped), registry)
    assert(Await.result(service.search(query), 5.seconds).isRight)
    assertEquals(Option(registry.find(Meters.SearchFacetsCapped).counter()).map(_.count()), Some(1.0))

  test("uncapped facets are not counted — the meter is a rate, not a request count"):
    val registry = SimpleMeterRegistry()
    val query = SearchQuery.parse("").getOrElse(fail("unparseable"))
    val service = Fixtures.service(Fixtures.StubRepository(), registry)
    assert(Await.result(service.search(query), 5.seconds).isRight)
    assertEquals(Option(registry.find(Meters.SearchFacetsCapped).counter()), None)
