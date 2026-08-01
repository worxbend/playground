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

package com.worxbend.persistence.repository

import com.augustnagro.magnum.Frag
import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.Filters
import com.worxbend.persistence.Filters.force
import com.worxbend.persistence.Recording
import com.worxbend.persistence.SqlText
import com.worxbend.persistence.search.Cursor
import com.worxbend.persistence.search.SortDirection
import io.circe.Json
import java.time.OffsetDateTime
import java.util.UUID
import org.scalacheck.Prop.forAll

/** The repository's statements, checked without a database.
  *
  * These exist because of a real defect: the first version of `searchSql` had no `FROM` clause. It compiled, the
  * injection properties passed, and only PostgreSQL would have objected — on a Docker host, in the slow tier, possibly
  * days later. Every assertion below is a shape a compiler cannot check and a database should not be the first to
  * notice.
  */
final class RepositorySqlSuite extends munit.ScalaCheckSuite:

  private val filter = force(Filter.deviceIn(Vector("kitchen-1")))
  private val at = OffsetDateTime.parse("2026-07-15T12:00:00Z")
  private val request = force(SearchRequest.first(Some(filter), SortDirection.Newest, 50))

  private def wellFormed(frag: Frag): Unit =
    assertEquals(
      SqlText.placeholders(frag.sqlString),
      frag.params.size,
      s"placeholder/parameter mismatch in: ${frag.sqlString}"
    )
    val (next, bindings) = Recording.bind(frag)
    assertEquals(bindings.positions, (1 to frag.params.size).toVector, frag.sqlString)
    assertEquals(next, frag.params.size + 1, frag.sqlString)
    assert(SqlText.balanced(frag.sqlString), frag.sqlString)
    assert(!frag.sqlString.contains('\''), frag.sqlString)
    assert(!frag.sqlString.contains(';'), frag.sqlString)

  test("every read statement names the table it reads"):
    Vector(
      PostgresEventRepository.searchSql(request),
      PostgresEventRepository.facetsSql(force(FacetRequest.of(Some(filter)))),
      PostgresEventRepository.histogramSql(force(HistogramRequest.of(Some(filter), at, at.plusDays(1)))),
      PostgresEventRepository.findSql(EventRef(at, UUID.randomUUID())),
      PostgresEventRepository.countSql(Some(filter), 100)
    ).foreach(frag => assert(frag.sqlString.contains("FROM events.cloud_event"), frag.sqlString))

  test("every statement is well formed, whatever the request"):
    Vector(
      PostgresEventRepository.searchSql(request),
      PostgresEventRepository.searchSql(force(SearchRequest.first(None, SortDirection.Oldest, 10))),
      PostgresEventRepository.facetsSql(force(FacetRequest.of(Some(filter)))),
      PostgresEventRepository.facetsSql(force(FacetRequest.of(None))),
      PostgresEventRepository.histogramSql(force(HistogramRequest.of(Some(filter), at, at.plusDays(7)))),
      PostgresEventRepository.findSql(EventRef(at, UUID.randomUUID())),
      PostgresEventRepository.countSql(None, 10000),
      PostgresEventRepository.insertSql(NewEvent.of(at, Json.obj("id" -> Json.fromString("x"))))
    ).foreach(wellFormed)

  property("a search statement stays well formed for any filter and any cursor"):
    forAll(Filters.genNested): generated =>
      val cursor = Cursor(
        at,
        UUID.randomUUID(),
        Filters.force(SearchRequest.first(Some(generated), SortDirection.Newest, 10)).fingerprint
      )
      val paged = force(SearchRequest.of(Some(generated), SortDirection.Newest, 10, Some(cursor.encode)))
      val frag = PostgresEventRepository.searchSql(paged)
      val (next, bindings) = Recording.bind(frag)
      SqlText.placeholders(frag.sqlString) == frag.params.size &&
      bindings.positions == (1 to frag.params.size).toVector &&
      next == frag.params.size + 1 &&
      !frag.sqlString.contains('\'')

  test("the keyset predicate is a row-value comparison, not two independent column seeks"):
    // `a < x AND b < y` is a different predicate: it skips every row with a smaller timestamp but a larger uid, which
    // silently drops rows from the middle of a page whenever two events share a timestamp.
    val cursor = Cursor(at, UUID.randomUUID(), request.fingerprint)
    val paged = force(SearchRequest.of(Some(filter), SortDirection.Newest, 50, Some(cursor.encode)))
    val sql = PostgresEventRepository.searchSql(paged).sqlString
    assert(sql.contains("(occurred_at, event_uid) < (?, ?)"), sql)
    assert(sql.contains("ORDER BY occurred_at DESC, event_uid DESC"), sql)

  test("ascending pages seek in the other direction, and sort to match"):
    val ascending = force(SearchRequest.first(None, SortDirection.Oldest, 10))
    val cursor = Cursor(at, UUID.randomUUID(), ascending.fingerprint)
    val paged = force(SearchRequest.of(None, SortDirection.Oldest, 10, Some(cursor.encode)))
    val sql = PostgresEventRepository.searchSql(paged).sqlString
    assert(sql.contains("(occurred_at, event_uid) > (?, ?)"), sql)
    assert(sql.contains("ORDER BY occurred_at ASC, event_uid ASC"), sql)

  test("the first page carries no keyset predicate at all"):
    val sql = PostgresEventRepository.searchSql(request).sqlString
    assert(!sql.contains("(occurred_at, event_uid)"), sql)

  test("the list projection omits the payload columns, and the detail projection adds exactly one"):
    // ADR §6.3: including `data` or `raw` in the list would de-TOAST a payload for every row on the page, including
    // the ones a user scrolls straight past.
    val list = PostgresEventRepository.searchSql(request).sqlString
    assert(!list.contains(" raw"), list)
    assert(!list.contains(" data"), list)
    val detail = PostgresEventRepository.findSql(EventRef(at, UUID.randomUUID())).sqlString
    assert(detail.contains(", raw FROM"), detail)

  test("the facet pass materialises its candidate set and asks for every grouping set in one query"):
    val sql = PostgresEventRepository.facetsSql(force(FacetRequest.of(Some(filter)))).sqlString
    assert(sql.contains("WITH cand AS MATERIALIZED"), sql)
    assert(sql.contains("GROUP BY GROUPING SETS"), sql)
    // The trailing `()` set is the grand total the cap flag is derived from; without it the cap is unreportable.
    assert(sql.contains("(severity), ())"), sql)
    FacetDimension.values.foreach(dimension => assert(sql.contains(s"(${columnOf(dimension)})"), dimension.label))

  test("the tag facet rides on the same candidate set rather than spliced into a second statement"):
    // The regression this pins is a performance one: two statements each carrying `WITH cand AS MATERIALIZED (...)`
    // ran the capped fact-table scan twice per page view. One statement referencing the CTE twice runs it once.
    val sql = PostgresEventRepository.facetsSql(force(FacetRequest.of(Some(filter)))).sqlString
    assertEquals("WITH cand AS MATERIALIZED".r.findAllMatchIn(sql).size, 1, sql)
    assertEquals("FROM cand".r.findAllMatchIn(sql).size, 2, sql)
    assert(sql.contains("UNION ALL"), sql)
    assert(sql.contains("unnest(cand.tags)"), sql)
    // The candidate cap is bound once, not once per pass, and the tag cap follows it.
    assertEquals(
      PostgresEventRepository.facetsSql(force(FacetRequest.of(Some(filter)))).params.toVector,
      Vector[Any](Vector("kitchen-1"), FacetRequest.DefaultCandidateCap, FacetRequest.DefaultPerDimension)
    )

  test("the grouping() argument order is the order the masks were computed for"):
    val sql = PostgresEventRepository.facetsSql(force(FacetRequest.of(None))).sqlString
    val arguments = "grouping\\(([^)]+)\\)".r.findFirstMatchIn(sql).map(_.group(1).split(',').map(_.trim).toVector)
    assertEquals(arguments, Some(FacetDimension.values.toVector.map(columnOf)))

  test("the bounded count limits the inner scan rather than counting the whole table"):
    val frag = PostgresEventRepository.countSql(None, 10000)
    assert(frag.sqlString.contains("SELECT count(*) FROM (SELECT 1 FROM"), frag.sqlString)
    assertEquals(frag.params.toVector, Vector[Any](10001))

  test("the histogram binds its window and its width, and joins the skeleton to the counts"):
    val request = force(HistogramRequest.of(None, at, at.plusHours(24)))
    val frag = PostgresEventRepository.histogramSql(request)
    assert(frag.sqlString.contains("generate_series("), frag.sqlString)
    assert(frag.sqlString.contains("LEFT JOIN"), frag.sqlString)
    assert(frag.sqlString.contains("date_bin("), frag.sqlString)
    // from, lastBucketStart, width, width, origin, from, until — in the order the text mentions them.
    assertEquals(
      frag.params.toVector,
      Vector[Any](at, request.lastBucketStart, "900 seconds", "900 seconds", at, at, at.plusHours(24))
    )

  test("the insert names the dedup index and does nothing on conflict"):
    val frag = PostgresEventRepository.insertSql(NewEvent.of(at, Json.obj("id" -> Json.fromString("x"))))
    assert(frag.sqlString.contains("ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING"), frag.sqlString)
    assertEquals(frag.params.size, 3)
    // The bound document is the rendering NewEvent already hashed, not a second one produced at bind time.
    val event = NewEvent.of(at, Json.obj("id" -> Json.fromString("x")))
    assertEquals(PostgresEventRepository.insertSql(event).params.toVector(1), event.canonical)
    assertEquals(event.canonical, event.raw.noSpaces)

  private def columnOf(dimension: FacetDimension): String = dimension match
    case FacetDimension.Type     => "ce_type"
    case FacetDimension.Source   => "ce_source"
    case FacetDimension.Device   => "device_id"
    case FacetDimension.Room     => "room_id"
    case FacetDimension.Person   => "person_id"
    case FacetDimension.Severity => "severity"
