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
import io.kzonix.kernel.search.Filter
import io.kzonix.kernel.search.FilterError
import io.kzonix.kernel.search.Severity
import io.kzonix.persistence.repository.SearchRequest
import io.kzonix.persistence.search.SortDirection
import java.time.OffsetDateTime
import munit.FunSuite

/** Query string in, kernel `Filter` out, repository `SearchRequest` out.
  *
  * This is the seam the whole web tier rests on, and it is where a mistake is least visible: a dropped parameter does
  * not fail, it returns a *wider* result set that looks perfectly plausible. Every case below is therefore about what
  * happens to a parameter, not about whether the call succeeded.
  */
final class SearchQuerySuite extends FunSuite:

  test("an empty query string is a valid search with no filter, not an error"):
    assertEquals(SearchQuery.parse(""), Right(SearchQuery(None, SortDirection.Newest, 50, None, Vector.empty)))

  test("a versioned query string becomes the kernel filter it describes"):
    val parsed = SearchQuery.parse("?v=1&type=io.kzonix.iot.alarm&device=kitchen-1&severity=%3E%3Dwarn")
    val filter = parsed.map(_.filter).getOrElse(fail("expected a parsed query"))
    val leaves = filter.toVector.flatMap(Filter.leaves)
    assert(leaves.exists { case Filter.TypeIn(vs) => vs.contains("io.kzonix.iot.alarm"); case _ => false })
    assert(leaves.exists { case Filter.DeviceIn(vs) => vs.contains("kitchen-1"); case _ => false })
    assert(leaves.contains(Filter.SeverityAtLeast(Severity.Warn)))

  test("control parameters are consumed here and never reach the filter codec"):
    val parsed = SearchQuery.parse("v=1&type=a&limit=25&sort=oldest&cursor=abc").getOrElse(fail("expected a query"))
    assertEquals(parsed.limit, 25)
    assertEquals(parsed.sort, SortDirection.Oldest)
    assertEquals(parsed.cursor, Some("abc"))
    // The filter half is untouched by them: `limit` is not a search term.
    assertEquals(parsed.raw, Vector("v" -> "1", "type" -> "a"))

  test("the repository request carries the filter, the sort and the bounded limit"):
    val parsed = SearchQuery.parse("v=1&type=io.kzonix.iot.alarm&limit=10&sort=oldest").getOrElse(fail("query"))
    val request = parsed.toRequest.getOrElse(fail("expected a valid request"))
    assertEquals(request.limit, 10)
    assertEquals(request.sort, SortDirection.Oldest)
    assertEquals(request.filter, parsed.filter)
    assertEquals(request.cursor, None)

  test("a limit above the repository maximum is rejected in the web tier, not silently clamped"):
    val errors = SearchQuery.parse(s"v=1&limit=${SearchRequest.MaxLimit + 1}").swap.getOrElse(Vector.empty)
    assert(errors.contains(FilterError.Invalid(
      SearchQuery.LimitKey,
      s"limit must be at most ${SearchRequest.MaxLimit}"
    )))

  test("a cursor minted for a different filter is refused by the repository request, not by parsing"):
    val other = SearchQuery.parse("v=1&type=a").getOrElse(fail("query"))
    val cursor = io.kzonix.persistence.search
      .Cursor(
        OffsetDateTime.parse("2026-07-26T10:00:00Z"),
        Fixtures.FirstUid,
        other.toRequest.map(_.fingerprint).getOrElse(fail("fingerprint"))
      )
      .encode
    val changed = SearchQuery.parse(s"v=1&type=b&cursor=$cursor").getOrElse(fail("query"))
    assert(changed.toRequest.isLeft, "a cursor from another filter must not resume this search")

  test("every failure is reported, not just the first"):
    val errors = SearchQuery.parse("v=1&limit=nope&sort=sideways&from=yesterday").swap.getOrElse(Vector.empty)
    assert(errors.exists { case FilterError.Invalid(SearchQuery.LimitKey, _) => true; case _ => false })
    assert(errors.exists { case FilterError.Invalid(SearchQuery.SortKey, _) => true; case _ => false })
    assert(errors.exists { case FilterError.Invalid("from", _) => true; case _ => false })

  test("a missing version is an error, so a future grammar change stays detectable"):
    assertEquals(SearchQuery.parse("type=a").swap.toOption, Some(Vector(FilterError.MissingVersion)))

  test("the lenient parse keeps the raw pairs so the filter bar can show what was rejected"):
    val lenient = SearchQuery.lenient("v=1&from=yesterday&limit=nope")
    assertEquals(lenient.filter, None)
    assertEquals(lenient.raw, Vector("v" -> "1", "from" -> "yesterday"))

  test("permalinks round-trip and never carry a cursor"):
    val parsed = SearchQuery.parse("v=1&type=a&cursor=abc&limit=10").getOrElse(fail("query"))
    val permalink = parsed.permalink
    assert(!permalink.contains("cursor"), s"'$permalink' must not carry a cursor")
    assert(permalink.contains("limit=10"), s"'$permalink' should preserve a non-default limit")
    assertEquals(SearchQuery.parse(permalink).map(_.filter), Right(parsed.filter))

  test("editing a filter always restarts at page one"):
    val parsed = SearchQuery.parse("v=1&type=a&cursor=abc").getOrElse(fail("query"))
    assert(!parsed.toggled("device", "kitchen-1").contains("cursor"))
    assert(!parsed.without("type", "a").contains("cursor"))
    assert(!parsed.within("2026-07-26T00:00:00Z", "2026-07-27T00:00:00Z").contains("cursor"))

  test("toggling a facet adds it once and removes it again"):
    val parsed = SearchQuery.parse("v=1").getOrElse(fail("query"))
    val added = parsed.toggled("device", "kitchen-1")
    assert(added.contains("device=kitchen-1"), added)
    val reparsed = SearchQuery.parse(added).getOrElse(fail("query"))
    assertEquals(reparsed.toggled("device", "kitchen-1").contains("device=kitchen-1"), false)

  test("a continuation appends the cursor to the permalink and nothing else"):
    val parsed = SearchQuery.parse("v=1&type=a").getOrElse(fail("query"))
    val next = parsed.continuation("CURSOR")
    assertEquals(next, s"${parsed.permalink}&cursor=CURSOR")

  test("the version is re-stamped on any non-empty edited link, even if the source URL omitted it"):
    val parsed = SearchQuery.lenient("type=a")
    assert(parsed.toggled("device", "kitchen-1").contains("v=1"))
