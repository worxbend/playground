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

package io.kzonix.persistence.repository

import io.kzonix.kernel.search.Filter
import io.kzonix.persistence.Filters.force
import io.kzonix.persistence.search.Cursor
import io.kzonix.persistence.search.Fingerprint
import io.kzonix.persistence.search.SortDirection
import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.*

/** The validated request types.
  *
  * Everything here is a boundary check that exists because the failure it prevents is quiet: an unbounded `limit`
  * exhausts a pool rather than erroring, a facet mask that does not match the query's argument order relabels every
  * facet, and a histogram width chosen by division rather than from a ladder produces bars that line up with nothing.
  */
final class RequestSuite extends munit.FunSuite:

  private val filter = force(Filter.typeIn(Vector("io.kzonix.iot.telemetry")))

  test("a page size must be positive and bounded"):
    assert(SearchRequest.first(None, SortDirection.Newest, 0).isLeft)
    assert(SearchRequest.first(None, SortDirection.Newest, -1).isLeft)
    assert(SearchRequest.first(None, SortDirection.Newest, SearchRequest.MaxLimit + 1).isLeft)
    assert(SearchRequest.first(None, SortDirection.Newest, SearchRequest.MaxLimit).isRight)
    assert(SearchRequest.first(None, SortDirection.Newest, SearchRequest.DefaultLimit).isRight)

  test("a cursor is accepted for the filter it was minted for"):
    val request = force(SearchRequest.first(Some(filter), SortDirection.Newest, 10))
    val cursor = Cursor(OffsetDateTime.parse("2026-07-01T00:00:00Z"), UUID.randomUUID(), request.fingerprint)
    val next = SearchRequest.of(Some(filter), SortDirection.Newest, 10, Some(cursor.encode))
    assertEquals(next.map(_.cursor), Right(Some(cursor)))

  test("a cursor from a different filter is refused with a message about the filter"):
    val other = force(Filter.typeIn(Vector("io.kzonix.iot.alarm")))
    val cursor = Cursor(
      OffsetDateTime.parse("2026-07-01T00:00:00Z"),
      UUID.randomUUID(),
      Fingerprint.of(Some(other), SortDirection.Newest)
    )
    val refused = SearchRequest.of(Some(filter), SortDirection.Newest, 10, Some(cursor.encode))
    assert(refused.isLeft, refused.toString)
    assert(refused.swap.exists(_.contains("different filter")), refused.toString)

  test("facet grouping masks match PostgreSQL's GROUPING() for the argument order the query uses"):
    // GROUPING(a, b, c, d, e, f) sets a bit for every argument NOT in the grouping set, most significant first.
    val arity = FacetDimension.values.length
    FacetDimension.values.zipWithIndex.foreach: (dimension, index) =>
      val expected = ((1 << arity) - 1) - (1 << (arity - 1 - index))
      assertEquals(dimension.groupingMask, expected, s"${dimension.label} has the wrong GROUPING mask")
    assertEquals(FacetDimension.TotalMask, (1 << arity) - 1)
    assertEquals(FacetDimension.values.map(_.groupingMask).distinct.length, arity)
    assert(!FacetDimension.values.map(_.groupingMask).contains(FacetDimension.TotalMask))

  test("a facet mask round-trips through the lookup, and the total is not a dimension"):
    FacetDimension.values.foreach: dimension =>
      assertEquals(FacetDimension.fromMask(dimension.groupingMask), Some(dimension))
    assertEquals(FacetDimension.fromMask(FacetDimension.TotalMask), None)

  test("facet requests are bounded on both the candidate cap and the facet size"):
    assert(FacetRequest.of(None, 0, 10).isLeft)
    assert(FacetRequest.of(None, FacetRequest.MaxCandidateCap + 1, 10).isLeft)
    assert(FacetRequest.of(None, 10, 0).isLeft)
    assertEquals(FacetRequest.of(None).map(_.candidateCap), Right(FacetRequest.DefaultCandidateCap))

  test("histogram width is drawn from the ladder and keeps the bucket count in range"):
    val from = OffsetDateTime.parse("2026-07-01T00:00:00Z")
    Vector(1.minute, 1.hour, 6.hours, 1.day, 7.days, 30.days, 365.days).foreach: span =>
      val until = from.plusSeconds(span.toSeconds)
      val width = HistogramRequest.widthFor(from, until)
      assert(HistogramRequest.Ladder.contains(width), s"$span produced an off-ladder width of $width")
      val buckets = span.toSeconds / width.toSeconds
      assert(buckets <= HistogramRequest.MaxBuckets, s"$span produced $buckets buckets")

  test("an empty or inverted histogram window is rejected"):
    val instant = OffsetDateTime.parse("2026-07-01T00:00:00Z")
    assert(HistogramRequest.of(None, instant, instant).isLeft)
    assert(HistogramRequest.of(None, instant, instant.minusHours(1)).isLeft)

  test("the last bucket starts before the window ends, so no trailing empty bar is emitted"):
    val from = OffsetDateTime.parse("2026-07-01T00:00:00Z")
    val until = from.plusHours(24)
    val request = force(HistogramRequest.of(None, from, until))
    assert(request.lastBucketStart.isBefore(until), request.lastBucketStart.toString)
    // Aligned to the window start, which is also the `date_bin` origin — otherwise the skeleton and the counts
    // would be bucketed on different boundaries and every join would miss.
    val offset = java.time.Duration.between(from, request.lastBucketStart).getSeconds
    assertEquals(offset % request.width.toSeconds, 0L)
