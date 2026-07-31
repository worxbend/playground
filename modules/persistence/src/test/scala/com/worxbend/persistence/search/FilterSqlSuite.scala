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

package com.worxbend.persistence.search

import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.NumOp
import com.worxbend.kernel.search.Severity
import com.worxbend.persistence.Filters
import com.worxbend.persistence.Filters.force
import com.worxbend.persistence.Recording
import com.worxbend.persistence.SqlText
import io.circe.Json
import java.time.OffsetDateTime
import org.scalacheck.Prop.forAll

/** The invariants of the filter compiler, over generated ASTs.
  *
  * ADR §9.3 calls this the module's priority test and it runs without a database, because these are properties of the
  * *text and parameter list*, not of the query's results. A differential test against real Postgres lives in `src/it`;
  * it can only run where Docker does, and an injection guarantee that is only checked on a Docker runner is a guarantee
  * that stops being checked the first time that job is skipped.
  *
  * The generators deliberately draw string leaves from [[Filters.hostile]], so every property below is asserted over
  * quotes, comment introducers, statement terminators, dollar quotes and unicode escape syntax rather than over
  * well-behaved identifiers.
  */
final class FilterSqlSuite extends munit.ScalaCheckSuite:

  override def scalaCheckTestParameters = super.scalaCheckTestParameters.withMinSuccessfulTests(500)

  /** Characters the compiler is allowed to emit. Everything a filter's *values* could contain is outside it.
    *
    * An allow-list rather than a deny-list, deliberately: a deny-list has to anticipate the next attack, an allow-list
    * only has to anticipate the next column name.
    */
  private val allowed: Set[Char] = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet ++
    Set(' ', '_', '(', ')', ',', '<', '>', '=', '@', '?', ':', '[', ']', '-', '.')

  private def sqlOf(filter: Filter): String = FilterSql.compile(filter).sqlString

  property("parameter count equals placeholder count"):
    forAll(Filters.genNested): filter =>
      val frag = FilterSql.compile(filter)
      SqlText.placeholders(frag.sqlString) == frag.params.size

  property("the writer fills exactly those placeholders, in order, starting at position one"):
    forAll(Filters.genNested): filter =>
      val frag = FilterSql.compile(filter)
      val (next, bindings) = Recording.bind(frag)
      bindings.positions == (1 to frag.params.size).toVector && next == frag.params.size + 1

  property("compiled SQL contains no quote character of any kind"):
    forAll(Filters.genNested): filter =>
      val sql = sqlOf(filter)
      !sql.contains('\'') && !sql.contains('"') && !sql.contains('`') && !sql.contains('\\')

  property("compiled SQL contains no statement terminator, comment introducer or dollar quote"):
    forAll(Filters.genNested): filter =>
      val sql = sqlOf(filter)
      !sql.contains(';') && !sql.contains("--") && !sql.contains("/*") && !sql.contains('$')

  property("compiled SQL is drawn from a fixed alphabet"):
    forAll(Filters.genNested): filter =>
      sqlOf(filter).forall(allowed.contains)

  property("no distinctive hostile input ever appears in the SQL text"):
    forAll(Filters.genNested): filter =>
      val sql = sqlOf(filter)
      // `?`, `??` and `@?` are also legitimate SQL the compiler emits, so only payloads that could not possibly be
      // part of a fragment's own text are checked here.
      Filters.hostile.filter(_.length > 4).forall(payload => !sql.contains(payload))

  property("parentheses balance at every nesting depth"):
    forAll(Filters.genNested): filter =>
      SqlText.balanced(sqlOf(filter))

  property("compilation is deterministic"):
    forAll(Filters.genNested): filter =>
      sqlOf(filter) == sqlOf(filter)

  property("compilation never throws, at any depth"):
    forAll(Filters.genFilter(5)): filter =>
      scala.util.Try(sqlOf(filter)).isSuccess

  test("an absent filter compiles to a clause the planner discards, not to nothing"):
    val frag = FilterSql.whereOf(None)
    assertEquals(frag.sqlString, " WHERE TRUE")
    assertEquals(frag.params.size, 0)

  test("each leaf maps to the shape ADR 6.2 specifies"):
    assertEquals(sqlOf(force(Filter.typeIn(Vector("a")))), "(ce_type = ANY(?::text[]))")
    assertEquals(sqlOf(force(Filter.sourceIn(Vector("a")))), "(ce_source = ANY(?::text[]))")
    assertEquals(sqlOf(force(Filter.deviceIn(Vector("a")))), "(device_id = ANY(?::text[]))")
    assertEquals(sqlOf(force(Filter.roomIn(Vector("a")))), "(room_id = ANY(?::text[]))")
    assertEquals(sqlOf(force(Filter.personIn(Vector("a")))), "(person_id = ANY(?::text[]))")
    assertEquals(sqlOf(force(Filter.tagsAll(Vector("a")))), "tags @> ?::text[]")
    assertEquals(sqlOf(Filter.severityAtLeast(Severity.Warn)), "severity_rank >= ?")
    assertEquals(sqlOf(force(Filter.payloadContains(Json.obj("a" -> Json.True)))), "data @> ?::jsonb")
    assertEquals(sqlOf(force(Filter.payloadCmp("value", NumOp.Gt, BigDecimal(21)))), "data @?? ?::jsonpath")
    assertEquals(
      sqlOf(force(Filter.extensionEq("tenantid", "x"))),
      "(extensions ?? ? AND extensions ->> ? = ?)"
    )
    assertEquals(sqlOf(force(Filter.fullText("kitchen"))), "search_doc @@ websearch_to_tsquery(?::regconfig, ?)")

  test("the jsonpath operator is written with pgjdbc's escape, so it reaches the server as @?"):
    val sql = sqlOf(force(Filter.payloadCmp("value", NumOp.Gt, BigDecimal(1))))
    // Three `?` characters, one placeholder. If this ever becomes two placeholders the statement fails at runtime
    // with a parameter-count mismatch that nothing else in the suite would catch.
    assertEquals(sql.count(_ == '?'), 3)
    assertEquals(SqlText.placeholders(sql), 1)

  test("an extension filter carries an indexable key-existence conjunct alongside the value test"):
    // `->>` is in no GIN operator class, so on its own this leaf had no access path to index (8) and every extension
    // filter was a sequential scan. `??` is pgjdbc's escape for the `?` operator, which IS in jsonb_ops. The name is
    // bound twice — once per conjunct — and the value once; anything else means the two halves have drifted apart.
    val frag = FilterSql.compile(force(Filter.extensionEq("tenantid", "acme")))
    assertEquals(frag.sqlString, "(extensions ?? ? AND extensions ->> ? = ?)")
    assertEquals(SqlText.placeholders(frag.sqlString), 3)
    assertEquals(frag.params.toVector, Vector[Any]("tenantid", "tenantid", "acme"))
    // Five `?` characters, three placeholders: the other two are the `??` the driver folds into one.
    assertEquals(frag.sqlString.count(_ == '?'), 5)

  test("severity compiles to the rank, and the rank matches the SQL function's threshold"):
    val frag = FilterSql.compile(Filter.severityAtLeast(Severity.Error))
    assertEquals(frag.params.toVector, Vector[Any](50))
    assertEquals(Severity.AlertThreshold.rank, 50)

  test("a time window with both bounds is half-open, so adjacent windows tile"):
    val from = OffsetDateTime.parse("2026-07-01T00:00:00Z")
    val until = OffsetDateTime.parse("2026-08-01T00:00:00Z")
    val frag = FilterSql.compile(force(Filter.occurred(Some(from), Some(until))))
    assertEquals(frag.sqlString, "(occurred_at >= ? AND occurred_at < ?)")
    assertEquals(frag.params.toVector, Vector[Any](from, until))

  test("full text binds the search configuration rather than quoting it"):
    val frag = FilterSql.compile(force(Filter.fullText("smoke")))
    assertEquals(frag.params.toVector, Vector[Any](FilterSql.TextSearchConfig, "smoke"))
