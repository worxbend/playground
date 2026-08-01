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
import com.worxbend.persistence.Recording
import java.time.OffsetDateTime
import java.time.ZoneOffset
import munit.FunSuite

/** The rollup reader's statements, asserted as values.
  *
  * **This suite exists because the overview repository was the one query path with no adversarial test.** `FilterSql`
  * has `FilterSqlInjectionSuite` and the fact-table repository has `RepositorySqlSuite`; the four statements here were
  * added later and had neither. They are the shape most likely to acquire a spliced identifier, because a breakdown is
  * *by* a column and the obvious way to write that is to put the column name in the string.
  *
  * The property is the same one the filter compiler is held to: **the statement text does not move.** Every request
  * value — the window, the bucket width, the breakdown size — must reach the database as a bind parameter, so two
  * requests that differ in every field compile to byte-identical SQL. Checking that the text "looks safe" would only
  * prove today's inputs were handled; equality proves none of them is handled, because none of them is in the text.
  */
final class OverviewSqlSuite extends FunSuite:

  private def at(year: Int, month: Int, day: Int): OffsetDateTime =
    OffsetDateTime.of(year, month, day, 0, 0, 0, 0, ZoneOffset.UTC)

  private def request(step: RollupStep, topN: Int = OverviewRequest.DefaultTopN): OverviewRequest =
    OverviewRequest
      .of(at(2026, 7, 1), at(2026, 7, 20), step, topN)
      .fold(problem => fail(problem), identity)

  /** Every request shape a caller can construct, so the invariants below are asserted over the whole surface. */
  private val requests: Vector[OverviewRequest] =
    for
      step <- RollupStep.values.toVector
      topN <- Vector(1, OverviewRequest.DefaultTopN, OverviewRequest.MaxTopN)
    yield request(step, topN)

  private def statements(request: OverviewRequest): Vector[(String, Frag)] =
    Vector(
      "volume" -> PostgresOverviewRepository.volumeSql(request),
      "totals" -> PostgresOverviewRepository.totalsSql(request)
    ) ++ RollupDimension.values.toVector.map(dimension =>
      s"breakdown/${dimension.label}" -> PostgresOverviewRepository.breakdownSql(dimension, request)
    ) :+ ("freshness" -> PostgresOverviewRepository.freshnessSql)

  test("the statement text is independent of every value in the request"):
    // The reference is one arbitrary request; every other one — a different window, a different bucket width, a
    // different breakdown size — must compile to exactly the same text.
    val reference = statements(requests.head).map((name, frag) => name -> frag.sqlString).toMap
    requests.foreach: candidate =>
      statements(candidate).foreach: (name, frag) =>
        assertEquals(frag.sqlString, reference(name), s"$name moved for $candidate")

  test("every value is a bind parameter, and the count matches the placeholders"):
    // `params` and the placeholder count come from independent code paths — one is accumulated by `++`, the other is
    // the text those fragments happened to carry — so a mismatch is a real defect and not a tautology. A shifted
    // parameter list produces plausible wrong numbers rather than an error, which on a dashboard is the worst kind.
    requests.foreach: candidate =>
      statements(candidate).foreach: (name, frag) =>
        assertEquals(frag.sqlString.count(_ == '?'), frag.params.size, s"$name: ${frag.sqlString}")
        assertEquals(Recording.bind(frag)._2.all.size, frag.params.size, s"$name wrote a different number of values")

  test("no statement contains a quote character, so nothing was rendered into it"):
    // The same invariant `FilterSql` holds, and for the same reason: an invariant with an exception is one nobody
    // checks. `date_bin`'s interval and the text-search configuration are both bound rather than quoted.
    requests.foreach: candidate =>
      statements(candidate).foreach: (name, frag) =>
        assert(!frag.sqlString.contains('\''), s"$name: ${frag.sqlString}")
        assert(!frag.sqlString.contains('"'), s"$name: ${frag.sqlString}")
        assert(!frag.sqlString.contains(';'), s"$name: ${frag.sqlString}")

  test("the breakdown column comes from the enum, and is one of exactly three names"):
    // The one place an identifier legitimately varies. It is a `match` over literals rather than
    // `Sql.lit(dimension.column)`, because `Sql.lit` takes compile-time constants only — so a dimension added to the
    // enum without a case here is a compile error, not a spliced column name.
    val columns = RollupDimension.values.toVector.map: dimension =>
      PostgresOverviewRepository.breakdownSql(dimension, requests.head).sqlString.split(' ')(1).stripSuffix(",")
    assertEquals(columns.toSet, Set("ce_type", "ce_source", "severity"))
    assertEquals(columns.distinct.size, RollupDimension.values.length, "two dimensions read the same column")

  test("the bucket width is a bound interval, never text"):
    // `'6 hours'::interval` spliced in would be the obvious spelling and would put a string built from a duration
    // into the statement. It is bound twice — once for the skeleton, once for `date_bin` — and both must be there or
    // the join matches nothing and the chart renders as a row of zeroes.
    val frag = PostgresOverviewRepository.volumeSql(request(RollupStep.SixHours))
    val values = Recording.bind(frag)._2.all
    assertEquals(values.count(_ == "21600 seconds"), 2, values.toString)

  test("every statement reads the rollup and nothing reads the fact table"):
    // The whole point of the separate repository: this page must not scan `events.cloud_event`. A statement that
    // acquired a join to it would still return correct numbers, and would cost a full scan per dashboard load.
    requests.foreach: candidate =>
      statements(candidate).foreach: (name, frag) =>
        assert(frag.sqlString.contains("events.event_rollup_hourly"), s"$name: ${frag.sqlString}")
        assert(!frag.sqlString.contains("cloud_event"), s"$name reads the fact table: ${frag.sqlString}")

  test("the breakdown size is bounded on both sides, and refused rather than clamped"):
    // Each breakdown is `ORDER BY count DESC LIMIT ?` over the whole window. Unbounded, it is both a slow sort and a
    // response nobody reads the end of — and the number comes off a query string.
    assert(OverviewRequest.of(at(2026, 7, 1), at(2026, 7, 20), RollupStep.Hour, 0).isLeft)
    assert(OverviewRequest.of(at(2026, 7, 1), at(2026, 7, 20), RollupStep.Hour, -1).isLeft)
    assert(OverviewRequest.of(at(2026, 7, 1), at(2026, 7, 20), RollupStep.Hour, OverviewRequest.MaxTopN + 1).isLeft)
    assert(OverviewRequest.of(at(2026, 7, 1), at(2026, 7, 20), RollupStep.Hour, OverviewRequest.MaxTopN).isRight)

  test("an empty or inverted window is refused"):
    assert(OverviewRequest.of(at(2026, 7, 20), at(2026, 7, 1), RollupStep.Hour).isLeft)
    assert(OverviewRequest.of(at(2026, 7, 20), at(2026, 7, 20), RollupStep.Day).isLeft)
