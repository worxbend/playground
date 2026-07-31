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

package com.worxbend.persistence

import com.worxbend.kernel.event.AttrValue
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.NumOp
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.PostgresEventRepository
import com.worxbend.persistence.repository.SearchRequest
import com.worxbend.persistence.search.FilterSql
import com.worxbend.persistence.search.SortDirection
import io.circe.Json
import java.sql.PreparedStatement
import java.time.OffsetDateTime
import scala.util.Using

/** The two open-ended search dimensions — a numeric comparison against a payload path, and equality against a
  * CloudEvents extension attribute — checked for the two things that can independently be wrong.
  *
  * **Does it return the right rows?** `EventRepositoryIT` establishes that both leaves are legal SQL. This suite is
  * about the answers: every comparison operator against a spread of real values, and an extension filter against a
  * corpus where most events carry no extensions at all.
  *
  * **Can it reach an index?** That question cannot be asked without a planner, and it is the one a filter fails
  * silently: `extensions ->> $1 = $2` is correct SQL, passes every unit test, returns the right rows on ten seeded
  * events, and sequentially scans the fact table forever, because `->>` is in no GIN operator class. The plan
  * assertions below use `enable_seqscan = off` deliberately: with sequential scans priced out of reach, PostgreSQL
  * falls back to one only when the predicate has **no index path at all**, which turns a question about cost — table
  * size, statistics, the planner's mood — into a question about capability, and makes the answer the same on ten rows
  * and on ten million.
  */
final class FilterAccessPathIT extends PostgresSuite:

  private lazy val repository: EventRepository =
    PostgresEventRepository(database.read.transactor, database.write.transactor)

  private val base = OffsetDateTime.parse("2026-07-10T00:00:00Z")

  /** The corpus, seeded once. Bigger than the other suites' handful for one reason: a plan is only interesting on a
    * table the planner has statistics for, and `ANALYZE` on three rows tells it nothing.
    */
  private val corpus = 300

  private val tenanted = 5

  override def beforeAll(): Unit =
    super.beforeAll()
    truncateEvents()
    val _ = await(repository.insertAll((0 until corpus).toVector.map(reading)))
    val _ = await(repository.insertAll((0 until tenanted).toVector.map(tenant)))
    analyse()

  private def force[A](result: Either[String, A]): A = result.fold(message => fail(message), identity)

  /** A telemetry event whose payload carries a nested numeric path as well as a top-level one, so the tests can tell
    * "the jsonpath works" apart from "the jsonpath works at depth one".
    */
  private def reading(index: Int): NewEvent =
    val envelope = Envelope(
      id = force(EventId(s"reading-$index")),
      source = force(Source("/gateways/1")),
      eventType = force(EventType("com.worxbend.iot.telemetry")),
      time = Some(base.plusSeconds(index.toLong)),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(
        Json.obj(
          "deviceId" -> Json.fromString(s"kitchen-${index % 3}"),
          "value" -> Json.fromInt(index),
          "sensor" -> Json.obj("temperature" -> Json.fromBigDecimal(BigDecimal(index) / 10))
        )
      )
    )
    force(NewEvent.from(envelope))

  /** An event carrying extensions, including one whose value is a JSON *number* rather than a string. That is the case
    * `extensions @> jsonb_build_object(?, ?)` would silently stop matching, and the reason the compiler keeps `->>`.
    */
  private def tenant(index: Int): NewEvent =
    val envelope = Envelope(
      id = force(EventId(s"tenanted-$index")),
      source = force(Source("/gateways/2")),
      eventType = force(EventType("com.worxbend.iot.telemetry")),
      time = Some(base.plusHours(1).plusSeconds(index.toLong)),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map(
        "tenantid" -> AttrValue.Text(if index == 0 then "acme" else "globex"),
        "sequence" -> AttrValue.Num(index)
      ),
      payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("gateway-2")))
    )
    force(NewEvent.from(envelope))

  private def matching(filter: Filter): Vector[String] =
    await(repository.search(force(SearchRequest.first(Some(filter), SortDirection.Newest, 500)))).rows.map(_.ceId)

  private def cmp(path: String, op: NumOp, value: BigDecimal): Vector[String] =
    matching(force(Filter.payloadCmp(path, op, value)))

  // ------------------------------------------------------------------------------------------------ right rows

  test("every comparison operator selects the rows it names, at the top level and nested"):
    // `value` runs 0..299. Each assertion is a count rather than a list because the point is the boundary: `>` and
    // `>=` must differ by exactly the one row sitting on it, and an off-by-one there is a filter that quietly hides
    // the reading somebody is looking at.
    assertEquals(cmp("value", NumOp.Gt, BigDecimal(297)).size, 2)
    assertEquals(cmp("value", NumOp.Gte, BigDecimal(297)).size, 3)
    assertEquals(cmp("value", NumOp.Lt, BigDecimal(3)).size, 3)
    assertEquals(cmp("value", NumOp.Lte, BigDecimal(3)).size, 4)
    assertEquals(cmp("value", NumOp.Eq, BigDecimal(150)), Vector("reading-150"))
    assertEquals(cmp("value", NumOp.Ne, BigDecimal(150)).size, corpus - 1)
    // A fractional bound against a nested path: 0.0, 0.1, … 29.9. The jsonpath literal is rendered from a BigDecimal
    // precisely so `21.7` is `21.7` and not `21.699999999999999`, which would not match the row it names.
    assertEquals(cmp("sensor.temperature", NumOp.Eq, BigDecimal("21.7")), Vector("reading-217"))
    assertEquals(cmp("sensor.temperature", NumOp.Gte, BigDecimal("29.8")).size, 2)

  test("a comparison never matches an event that does not carry the path, or carries it as a string"):
    // jsonpath comparison between a string and a number is *unknown*, not an error and not a coercion — so the five
    // extension-carrying events, whose payload has no `value` at all, are absent from every comparison above.
    assertEquals(cmp("value", NumOp.Gte, BigDecimal(0)).size, corpus)
    assertEquals(cmp("value", NumOp.Ne, BigDecimal(-1)).size, corpus)
    assertEquals(cmp("nosuchpath", NumOp.Gt, BigDecimal(0)), Vector.empty)
    assertEquals(cmp("deviceId", NumOp.Gt, BigDecimal(0)), Vector.empty)

  test("extension equality matches the attribute's rendered text, whatever JSON type it was written as"):
    assertEquals(matching(force(Filter.extensionEq("tenantid", "acme"))), Vector("tenanted-0"))
    assertEquals(matching(force(Filter.extensionEq("tenantid", "globex"))).size, tenanted - 1)
    assertEquals(matching(force(Filter.extensionEq("tenantid", "nobody"))), Vector.empty)
    // `sequence` is an AttrValue.Num, so `extensions` holds `"sequence": 3` — a JSON number. `->>` renders it as text
    // and the filter matches. Containment against `{"sequence": "3"}` would not, which is why the compiler keeps the
    // `->>` conjunct alongside the indexable one.
    assertEquals(matching(force(Filter.extensionEq("sequence", "3"))), Vector("tenanted-3"))
    // The 300 events with no extensions at all are matched by nothing, rather than by an empty-string comparison.
    assertEquals(matching(force(Filter.extensionEq("tenantid", "0"))), Vector.empty)

  test("a reserved CloudEvents attribute is not an extension, however the filter spells it"):
    assertEquals(matching(force(Filter.extensionEq("type", "com.worxbend.iot.telemetry"))), Vector.empty)
    assertEquals(matching(force(Filter.extensionEq("id", "reading-1"))), Vector.empty)

  // ---------------------------------------------------------------------------------------------- access paths

  test("an extension filter has an index path — the whole reason it is compiled as two conjuncts"):
    // Index (8), `gin (extensions)` with the default jsonb_ops. The `??` conjunct is `extensions ? $1`, which IS in
    // that operator class; the `->>` conjunct is not in any, and on its own left this filter with nothing to scan.
    val compiled = planOf(force(Filter.extensionEq("tenantid", "acme")))
    assert(compiled.contains("Index Scan"), s"the extension filter has no index path:\n$compiled")
    assert(!compiled.contains("Seq Scan"), s"the extension filter still falls back to a scan:\n$compiled")

  test("the ->> spelling alone has no index path, which is the regression the second conjunct prevents"):
    // Written out here rather than compiled, because this is the shape the compiler must never emit again. With
    // sequential scans priced out of reach the planner still picks one, which is only possible when nothing else is
    // available: `->>` is a function-backed operator in no GIN operator class, so index (8) cannot serve it.
    val naive = rawPlan("extensions ->> 'tenantid' = 'acme'")
    assert(naive.contains("Seq Scan"), s"expected the un-indexable spelling to scan; it planned as:\n$naive")

  test("the existence conjunct is redundant in meaning and not in plan"):
    // Same rows either way — `extensions ->> $1` is NULL when the key is absent, so the AND cannot narrow anything.
    // The whole difference is which of the two the index can answer first.
    val bothWays = rawRows("extensions ->> 'tenantid' = 'globex'")
    assertEquals(bothWays, matching(force(Filter.extensionEq("tenantid", "globex"))).size.toLong)

  test("a payload EQUALITY reaches index (7) with a search key; a RANGE reaches it with none"):
    // The finding this suite exists to record. A GIN index over jsonb answers `@?` by pulling clauses of the form
    // `accessors_chain = constant` out of the jsonpath: `$.value ? (@ == 150)` yields one, and the bitmap index scan
    // returns the handful of rows carrying it. `$.value ? (@ > 150)` yields none, so the same index is scanned in
    // full and every row in it is handed to the recheck — correct, and no cheaper than reading the table.
    //
    // Asserted as a row *ratio* off EXPLAIN ANALYZE rather than as a plan shape, because both forms use the index and
    // only the number of entries it returns tells them apart. If a future PostgreSQL learns to extract range clauses
    // this test fails, which is the right way to find out.
    val selective = indexRows(force(Filter.payloadCmp("value", NumOp.Eq, BigDecimal(150))))
    val unselective = indexRows(force(Filter.payloadCmp("value", NumOp.Gt, BigDecimal(150))))
    assert(selective < 10.0, s"the equality form should return a handful of index entries, got $selective")
    assert(
      unselective >= corpus.toDouble,
      s"expected the range form to scan every entry ($corpus); it returned $unselective"
    )

  // ---------------------------------------------------------------------------------------------------- plumbing

  /** The plan for a compiled filter, with sequential scans priced out. */
  private def planOf(filter: Filter): String =
    val frag = FilterSql.compile(filter)
    explain(frag.sqlString, ps => val _ = frag.writer.write(ps, 1), analyse = false)

  /** The plan for a hand-written predicate with no parameters. */
  private def rawPlan(predicate: String): String = explain(predicate, _ => (), analyse = false)

  /** The rows a hand-written predicate actually selects, for the equivalence check. */
  private def rawRows(predicate: String): Long =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        Using.resource(statement.executeQuery(s"SELECT count(*) FROM events.cloud_event WHERE $predicate")): rs =>
          if rs.next() then rs.getLong(1) else -1L

  /** The number of rows the index scan under a compiled filter's plan actually returned.
    *
    * The `Bitmap Index Scan` node, or whichever node carries the index condition — its `actual rows` is the size of the
    * candidate set the index produced, before any recheck. That is the number this suite is about.
    */
  private def indexRows(filter: Filter): Double =
    val frag = FilterSql.compile(filter)
    val text = explain(frag.sqlString, ps => val _ = frag.writer.write(ps, 1), analyse = true)
    // Every partition contributes a node; the corpus lives in one, so the largest index-scan count is the answer.
    text.linesIterator
      .filter(_.contains("Bitmap Index Scan"))
      .flatMap(line => ActualRows.findFirstMatchIn(line).map(_.group(2).toDouble))
      .maxOption
      .getOrElse(fail(s"no bitmap index scan reported a row count, so there is nothing to measure:\n$text"))

  private val ActualRows = """actual (time=[0-9.]+\.\.[0-9.]+ )?rows=([0-9.]+)""".r

  /** `EXPLAIN` with `enable_seqscan = off`, reset on the way out.
    *
    * The reset matters: these run on a pooled connection, and a leaked `enable_seqscan = off` would silently change the
    * plans every later suite gets.
    */
  private def explain(predicate: String, bind: PreparedStatement => Unit, analyse: Boolean): String =
    withConnection: connection =>
      set(connection, "SET enable_seqscan = off")
      try
        val options = if analyse then "(ANALYZE, TIMING OFF, SUMMARY OFF, COSTS OFF)" else "(COSTS OFF)"
        Using.resource(
          connection.prepareStatement(s"EXPLAIN $options SELECT event_uid FROM events.cloud_event WHERE $predicate")
        ): statement =>
          bind(statement)
          Using.resource(statement.executeQuery()): rs =>
            Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).mkString("\n")
      finally set(connection, "RESET enable_seqscan")

  private def set(connection: java.sql.Connection, sql: String): Unit =
    Using.resource(connection.createStatement()): statement =>
      val _ = statement.execute(sql)

  /** Statistics, without which every plan below is a guess about a table the planner has never looked at. */
  private def analyse(): Unit =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute("ANALYZE events.cloud_event")
