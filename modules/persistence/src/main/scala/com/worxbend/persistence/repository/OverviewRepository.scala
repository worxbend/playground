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

import com.augustnagro.magnum.DbCodec
import com.augustnagro.magnum.Frag
import com.augustnagro.magnum.Transactor
import com.augustnagro.magnum.connect
import com.worxbend.persistence.Sql
import com.worxbend.persistence.Sql.++
import java.time.Duration
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** One point of the overview's volume series.
  *
  * `errors` is `count(*) FILTER (WHERE severity_rank >= 50)` as the rollup already computed it, not a second query: the
  * two numbers have to describe the same rows or the "3 % of traffic is errors" reading on the page is a comparison
  * between two different populations.
  */
final case class VolumePoint(bucket: OffsetDateTime, events: Long, errors: Long) derives DbCodec

/** One row of a rollup breakdown: a dimension value with its event and error counts over the window. */
final case class RollupSlice(value: String, events: Long, errors: Long) derives DbCodec

/** The window's totals, from the same relation as everything else on the page.
  *
  * Deliberately does **not** carry a device count. The rollup's `device_count` is `count(DISTINCT device_id)` *within*
  * each `(hour, type, source, severity)` group, so summing it counts a device once per group it appears in — a number
  * that is always too large and looks plausible. Answering "how many devices are active" honestly means a
  * `count(DISTINCT device_id)` over the fact table, which is the scan this view exists to avoid.
  */
final case class RollupTotals(events: Long, errors: Long) derives DbCodec

/** The dimensions `events.event_rollup_hourly` can be sliced by.
  *
  * Three, because three is what the view groups on. `device_id` is *not* a column of the rollup and cannot be added to
  * this enum without changing the view: a device leaderboard is a fact-table aggregate, and ADR §0 decision 7 put the
  * rollup here precisely so a dashboard never does that.
  *
  * `filterKey` is the permalink parameter a web tier must use to turn a slice into a search. It lives here rather than
  * in the web tier so that a dimension added to the view arrives with the link it implies, instead of the two being
  * matched up by hand at the call site.
  */
enum RollupDimension(val label: String, val filterKey: String):
  case Type extends RollupDimension("type", "type")
  case Source extends RollupDimension("source", "source")
  case Severity extends RollupDimension("severity", "severity")

/** The bucket width of the volume series.
  *
  * A closed set of three, not an arbitrary interval. The rollup's own grain is one hour, so an hour is the finest
  * anything can be, and every wider width has to be a whole multiple of it or a bucket would straddle two rollup rows
  * and be double- or half-counted at its edges.
  */
enum RollupStep(val width: FiniteDuration):
  case Hour extends RollupStep(1.hour)
  case SixHours extends RollupStep(6.hours)
  case Day extends RollupStep(24.hours)

/** An overview query, validated.
  *
  * The constructor is private so `from` is always aligned to `step`. An unaligned origin puts the `generate_series`
  * skeleton and `date_bin`'s output on different boundaries, the `LEFT JOIN` then matches nothing, and the chart
  * renders as a row of zeroes over a database full of events — a failure that looks like "no data" rather than like a
  * bug.
  *
  * @param topN
  *   entries per breakdown. Bounded because each breakdown is `ORDER BY count DESC LIMIT ?` over the whole window and
  *   an unbounded list is both a slow sort and a page nobody reads the end of.
  */
final case class OverviewRequest private (
  from: OffsetDateTime,
  until: OffsetDateTime,
  step: RollupStep,
  topN: Int
):

  /** The start of the last bucket that begins before `until`.
    *
    * Same reasoning as `HistogramRequest.lastBucketStart`: `generate_series(from, until, width)` emits a bucket
    * starting *at* `until` whenever the span divides evenly, which is a permanent trailing zero bar on every chart
    * whose window is a round number of hours — and every window here is.
    */
  def lastBucketStart: OffsetDateTime =
    val span = Duration.between(from, until).getSeconds
    val stride = step.width.toSeconds
    val steps = if span <= 0 then 0L else (span - 1) / stride
    from.plusSeconds(steps * stride)

  /** The interval literal both `generate_series` and `date_bin` are given, as a bind parameter and never as text. */
  def interval: String = s"${step.width.toSeconds} seconds"

object OverviewRequest:

  val MaxTopN: Int = 50

  val DefaultTopN: Int = 8

  /** Rejects an empty or inverted window and aligns `from` down onto the step's grid.
    *
    * **Aligned to the Unix epoch, not to `from`.** `date_bin` would happily take an arbitrary origin, and passing an
    * un-aligned `now - 24h` works — but then every reload shifts every bucket boundary by however many seconds have
    * passed, so two screenshots of the same dashboard cannot be compared and no bar starts on a time a human reads off
    * a clock. Flooring the epoch second to a multiple of the width gives whole clock hours, whole six-hour blocks and
    * whole UTC days, and it gives them identically on every replica because the epoch is not a local concept.
    */
  def of(
    from: OffsetDateTime,
    until: OffsetDateTime,
    step: RollupStep,
    topN: Int = DefaultTopN
  ): Either[String, OverviewRequest] =
    val stride = step.width.toSeconds
    val aligned = from.minusSeconds(Math.floorMod(from.toEpochSecond, stride))
    if !aligned.isBefore(until) then Left(s"overview window is empty: '$aligned' is not before '$until'")
    else if topN < 1 then Left("overview breakdown size must be at least 1")
    else if topN > MaxTopN then Left(s"overview breakdown size must be at most $MaxTopN")
    else Right(OverviewRequest(aligned, until, step, topN))

/** The reader `events.event_rollup_hourly` never had.
  *
  * **Why this is a separate interface from [[EventRepository]].** They answer different questions from different
  * relations with different cost models. Everything here reads the materialized view — one row per
  * `(hour, type, source, severity)`, at most a few thousand rows for a 30-day window — so a query that would scan
  * millions of fact rows becomes an index range scan over a small aggregate. Everything in `EventRepository` reads the
  * fact table. Keeping them apart makes "this page must not touch the fact table" a property of a *type* rather than a
  * comment on a method.
  *
  * **What the caller is buying, and what it costs.** The view is refreshed on a schedule (cobalt's `RollupRefresh`,
  * every few minutes), so every number from here is stale by up to one refresh interval and the current hour is always
  * partial. That is the trade: a 90-day chart that costs nothing, in exchange for numbers that are not to-the-second. A
  * UI built on this **must say so** — see [[freshness]], which exists so the page can state the staleness rather than
  * leave an operator to infer it from a count that disagrees with search.
  */
trait OverviewRepository:

  /** Event and error counts per bucket, with empty buckets materialised as zero.
    *
    * The zero-filling is the same decision the fact-table histogram makes: a quiet hour that is simply absent from the
    * result set disappears from the chart, and the shape of the data becomes a lie told by omission.
    */
  def volume(request: OverviewRequest): Future[Vector[VolumePoint]]

  /** The top `topN` values of one dimension over the window, busiest first. */
  def breakdown(dimension: RollupDimension, request: OverviewRequest): Future[Vector[RollupSlice]]

  /** Events and errors over the whole window. */
  def totals(request: OverviewRequest): Future[RollupTotals]

  /** The newest hour the view holds, or `None` when it has never been refreshed against a populated fact table.
    *
    * This is the honesty knob. `None` means the rollup is empty, which on a system that is definitely ingesting means
    * the refresh job is not running — the single most likely way for this page to be confidently wrong.
    */
  def freshness(): Future[Option[OffsetDateTime]]

/** The PostgreSQL implementation, bound to the read pool.
  *
  * One transactor, not two: nothing here writes, and there is no `insert` on the interface to make the question arise.
  *
  * **Every statement is built by a pure function in the companion**, for the same reason as
  * [[PostgresEventRepository]]: a `Frag` is a value, so parameter order and placeholder count are assertable without a
  * database, and only the behaviour of the *view* needs the slow tier.
  */
final class PostgresOverviewRepository(read: Transactor)(using ec: ExecutionContext) extends OverviewRepository:

  import PostgresOverviewRepository.*

  def volume(request: OverviewRequest): Future[Vector[VolumePoint]] =
    Future(connect(read)(volumeSql(request).query[VolumePoint].run()))

  def breakdown(dimension: RollupDimension, request: OverviewRequest): Future[Vector[RollupSlice]] =
    Future(connect(read)(breakdownSql(dimension, request).query[RollupSlice].run()))

  def totals(request: OverviewRequest): Future[RollupTotals] =
    Future(connect(read)(totalsSql(request).query[RollupTotals].run()).headOption.getOrElse(RollupTotals(0L, 0L)))

  def freshness(): Future[Option[OffsetDateTime]] =
    Future(connect(read)(freshnessSql.query[Option[OffsetDateTime]].run()).headOption.flatten)

object PostgresOverviewRepository:

  /** The view every statement in this object reads, spelled once. */
  private val fromRollup: Frag = Sql.lit(" FROM events.event_rollup_hourly")

  /** `bucket >= ? AND bucket < ?` — half-open, and the reason `event_rollup_hourly_bucket_ix` exists. */
  private def window(request: OverviewRequest): Frag =
    Sql.lit(" WHERE bucket >= ") ++ Sql.bind(request.from) ++
      Sql.lit(" AND bucket < ") ++ Sql.bind(request.until)

  /** The volume series, zero-filled by a `generate_series` skeleton.
    *
    * `date_bin`, not `date_trunc`. `date_trunc('day', ts)` resolves in the *session* time zone, so the same rows bucket
    * differently depending on which replica ran the query and what `TimeZone` pgjdbc negotiated; `date_bin` takes an
    * explicit origin and is therefore the same answer everywhere. The origin is the window start, which is also the
    * skeleton's first element — that is what makes the join match.
    *
    * The counts are cast: `sum(bigint)` is `numeric` in PostgreSQL, and reading a `numeric` through a `Long` codec is a
    * driver-dependent coercion nobody should be relying on.
    */
  private[persistence] def volumeSql(request: OverviewRequest): Frag =
    Sql.lit("SELECT s.bucket, coalesce(v.events, 0)::bigint, coalesce(v.errors, 0)::bigint FROM generate_series(") ++
      Sql.bind(request.from) ++ Sql.lit(", ") ++ Sql.bind(request.lastBucketStart) ++ Sql.lit(", ") ++
      Sql.bind(request.interval) ++ Sql.lit("::interval) AS s(bucket) LEFT JOIN (SELECT date_bin(") ++
      Sql.bind(request.interval) ++ Sql.lit("::interval, bucket, ") ++ Sql.bind(request.from) ++
      Sql.lit(") AS bucket, sum(event_count) AS events, sum(error_count) AS errors") ++
      fromRollup ++
      window(request) ++
      Sql.lit(" GROUP BY 1) AS v ON v.bucket = s.bucket ORDER BY s.bucket")

  /** One dimension, busiest first.
    *
    * The value column is never `NULL`, and that is a property of the schema rather than an assumption: `ce_type` and
    * `ce_source` are guarded by `cloud_event_required_ck`, and the view itself writes `coalesce(severity, 'none')` —
    * which it must, because a `NULL` in a unique-index column is what would stop `REFRESH ... CONCURRENTLY` being
    * legal.
    *
    * `ORDER BY 2 DESC, 1` and not `ORDER BY 2 DESC` alone: ties otherwise come back in whatever order the aggregate
    * produced them, so two loads of the same page can show the same numbers in a different order.
    */
  private[persistence] def breakdownSql(dimension: RollupDimension, request: OverviewRequest): Frag =
    Sql.lit("SELECT ") ++ column(dimension) ++
      Sql.lit(", coalesce(sum(event_count), 0)::bigint, coalesce(sum(error_count), 0)::bigint") ++
      fromRollup ++
      window(request) ++
      Sql.lit(" GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ") ++ Sql.bind(request.topN)

  private[persistence] def totalsSql(request: OverviewRequest): Frag =
    Sql.lit("SELECT coalesce(sum(event_count), 0)::bigint, coalesce(sum(error_count), 0)::bigint") ++
      fromRollup ++
      window(request)

  /** No window: "how fresh is the rollup" is a question about the whole view, and bounding it would answer a different
    * question — an empty answer would then be ambiguous between "not refreshed" and "nothing happened lately".
    */
  private[persistence] val freshnessSql: Frag = Sql.lit("SELECT max(bucket)") ++ fromRollup

  /** Dimension to column, as a match over literals.
    *
    * Not `Sql.lit(dimension.column)`: [[Sql.lit]] takes compile-time constants only, and that is the whole security
    * property of the fragment algebra. A column name reaching SQL has to be written out here, where the compiler can
    * see it is a literal.
    */
  private def column(dimension: RollupDimension): Frag = dimension match
    case RollupDimension.Type     => Sql.lit("ce_type")
    case RollupDimension.Source   => Sql.lit("ce_source")
    case RollupDimension.Severity => Sql.lit("severity")
