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

import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.persistence.maintenance.PartitionMaintenance
import com.worxbend.persistence.maintenance.PartitionPolicy
import com.worxbend.persistence.maintenance.RollupRefresh
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.OverviewRepository
import com.worxbend.persistence.repository.OverviewRequest
import com.worxbend.persistence.repository.PostgresEventRepository
import com.worxbend.persistence.repository.PostgresOverviewRepository
import com.worxbend.persistence.repository.RollupDimension
import com.worxbend.persistence.repository.RollupStep
import io.circe.Json
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt

/** The overview's queries against the real materialized view.
  *
  * **Only a database can answer any of this.** `events.event_rollup_hourly` is a relation PostgreSQL computes: whether
  * `sum(event_count)` over it equals the number of rows inserted, whether `severity` really is never `NULL` because of
  * the view's own `coalesce`, whether `date_bin` and `generate_series` land on the same boundaries so the `LEFT JOIN`
  * matches — every one of those is a claim about the view's definition, and a unit test can only restate the SQL it is
  * checking.
  *
  * **The clock is the real one, deliberately.** The view's own `WHERE occurred_at >= now() - interval '90 days'` means
  * a suite pinned to a fixed date silently stops covering anything the day it falls out of that window: the refresh
  * would succeed, the view would be empty, and every assertion below would have to be "0 == 0". Seeding relative to now
  * is the only version of this test that keeps working.
  *
  * Everything is seeded inside the CURRENT hour so it cannot straddle a month boundary and land in the `DEFAULT`
  * partition, which `MigrationIT` requires to stay empty. The partition job runs first for the same reason: on any run
  * after August 2026 the month `V1__events.sql` hard-coded is long past.
  */
final class OverviewRepositoryIT extends PostgresSuite:

  private lazy val repository: OverviewRepository = PostgresOverviewRepository(database.read.transactor)

  private lazy val events = PostgresEventRepository(database.read.transactor, database.write.transactor)

  private val now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)

  /** The start of the current hour: the bucket every seeded event falls into, and the grain the view groups on. */
  private val hour: OffsetDateTime = now.truncatedTo(ChronoUnit.HOURS)

  private def force[A](result: Either[String, A]): A = result.fold(message => fail(message), identity)

  private def event(id: String, source: String, severity: Option[String], minute: Int): NewEvent =
    val envelope = Envelope(
      id = force(EventId(id)),
      source = force(Source(source)),
      eventType = force(EventType("com.worxbend.iot.telemetry")),
      time = Some(hour.plusMinutes(minute.toLong)),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(
        Json.obj(
          "deviceId" -> Json.fromString("kitchen-1"),
          "value" -> Json.fromDoubleOrNull(21.5)
        ).deepMerge(severity.fold(Json.obj())(level => Json.obj("severity" -> Json.fromString(level))))
      )
    )
    force(NewEvent.from(envelope))

  /** Five events in the current hour: three from one gateway (one of them an error), two from another, and one of those
    * carrying no severity at all — which is the row that proves the view's `coalesce(severity, 'none')`.
    */
  private val seeded: Vector[NewEvent] = Vector(
    event("ov-1", "/gateways/hall", Some("info"), 1),
    event("ov-2", "/gateways/hall", Some("error"), 2),
    event("ov-3", "/gateways/hall", Some("info"), 3),
    event("ov-4", "/gateways/shed", Some("critical"), 4),
    event("ov-5", "/gateways/shed", None, 5)
  )

  /** Two errors by the domain's definition: `error` ranks 50 and `critical` ranks 60, and the view counts
    * `severity_rank >= 50`. If `events.severity_rank()` and `com.worxbend.kernel.search.Severity` ever disagree about a
    * spelling, this number is what changes.
    */
  private val expectedErrors: Long = 2L

  override def beforeAll(): Unit =
    super.beforeAll()
    // The migration hard-codes 2026-07 and 2026-08. On any later run this is the only reason the current month has
    // anywhere to write.
    val _ = PartitionMaintenance(database.write.get(), PartitionPolicy(3, None, 5.seconds)).run(now.toInstant)
    truncateEvents()
    val written = await(events.insertAll(seeded))
    assertEquals(written, seeded.size.toLong)
    // Nothing in PostgreSQL updates a materialized view. Without this every assertion below reads a snapshot of an
    // empty table — successfully, which is exactly why the refresh job exists.
    val report = RollupRefresh(database.write.get()).run().getOrElse(fail("the rollup lock was already held"))
    assert(report.rows > 0L, "the refresh produced no rollup rows for events that were definitely written")

  /** A window covering the previous two hours and the current one, so the skeleton has three buckets and only one of
    * them can be non-empty.
    */
  private def request(step: RollupStep = RollupStep.Hour): OverviewRequest =
    force(OverviewRequest.of(hour.minusHours(2), hour.plusHours(1), step))

  test("the volume series comes back zero-filled, so a quiet hour is a bar and not a gap"):
    val points = await(repository.volume(request()))
    assertEquals(points.size, 3, s"expected three hourly buckets, got ${points.map(_.bucket)}")
    // Instants, not `OffsetDateTime`s: two values for the same moment in different offsets are not `equals`, and the
    // offset a `timestamptz` comes back in depends on the session's time zone rather than on the data.
    assertEquals(
      points.map(_.bucket.toInstant),
      Vector(hour.minusHours(2), hour.minusHours(1), hour).map(_.toInstant)
    )
    assertEquals(points.map(_.events).sum, seeded.size.toLong)
    assertEquals(points.map(_.errors).sum, expectedErrors)
    // The two empty hours are present with a zero, which is the whole point of the generate_series skeleton.
    assertEquals(points.take(2).map(_.events), Vector(0L, 0L))
    assertEquals(points.last.events, seeded.size.toLong)

  test("a wider bucket aggregates the same events without changing the totals"):
    // The rollup's grain is an hour, so a day bucket is a whole number of them. If `date_bin` and `generate_series`
    // ever disagreed about an origin the join would match nothing and this total would be zero.
    val daily = force(OverviewRequest.of(hour.minusHours(23), hour.plusHours(1), RollupStep.Day))
    val points = await(repository.volume(daily))
    assertEquals(points.map(_.events).sum, seeded.size.toLong)
    assertEquals(points.map(_.errors).sum, expectedErrors)

  test("the totals over the window are the totals of the series"):
    val totals = await(repository.totals(request()))
    assertEquals(totals.events, seeded.size.toLong)
    assertEquals(totals.errors, expectedErrors)

  test("an empty window is zero and not an empty result set"):
    // `sum()` over no rows is NULL, and a NULL read through a Long codec is 0 by accident rather than by intent. The
    // coalesce in the statement is what makes it deliberate.
    val empty = force(OverviewRequest.of(hour.minusHours(50), hour.minusHours(48), RollupStep.Hour))
    val totals = await(repository.totals(empty))
    assertEquals(totals.events, 0L)
    assertEquals(totals.errors, 0L)
    assertEquals(await(repository.volume(empty)).map(_.events), Vector(0L, 0L))

  test("the source breakdown ranks by volume and carries the error counts with it"):
    val slices = await(repository.breakdown(RollupDimension.Source, request()))
    assertEquals(slices.map(_.value), Vector("/gateways/hall", "/gateways/shed"))
    assertEquals(slices.map(_.events), Vector(3L, 2L))
    assertEquals(slices.map(_.errors), Vector(1L, 1L))

  test("the type breakdown is one row, because every seeded event shares a type"):
    val slices = await(repository.breakdown(RollupDimension.Type, request()))
    assertEquals(slices.map(_.value), Vector("com.worxbend.iot.telemetry"))
    assertEquals(slices.map(_.events), Vector(seeded.size.toLong))

  test("an event with no severity appears as 'none' rather than vanishing from the mix"):
    // The view writes `coalesce(severity, 'none')`, and it must: a NULL in a unique-index column is what would stop
    // `REFRESH ... CONCURRENTLY` being legal. The consequence for the UI is a breakdown value the filter grammar
    // cannot express, which is why `OverviewPresenter` renders that row without a link.
    val slices = await(repository.breakdown(RollupDimension.Severity, request()))
    assertEquals(slices.map(_.value).sorted, Vector("critical", "error", "info", "none"))
    assertEquals(slices.find(_.value == "none").map(_.events), Some(1L))
    assertEquals(slices.find(_.value == "info").map(_.events), Some(2L))

  test("the breakdown honours its limit, which is what keeps a leaderboard from becoming a table"):
    val one = force(OverviewRequest.of(hour.minusHours(2), hour.plusHours(1), RollupStep.Hour, topN = 1))
    assertEquals(await(repository.breakdown(RollupDimension.Severity, one)).size, 1)

  test("freshness reports the newest hour the view holds, which is how the page states its own staleness"):
    assertEquals(await(repository.freshness()).map(_.toInstant), Some(hour.toInstant))
