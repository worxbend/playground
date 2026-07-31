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

import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.search.Cursor
import com.worxbend.persistence.search.CursorError
import com.worxbend.persistence.search.Fingerprint
import com.worxbend.persistence.search.SortDirection
import java.time.Duration
import java.time.OffsetDateTime
import scala.concurrent.Future
import scala.concurrent.duration.*

/** One page of search results.
  *
  * `nextCursor` is present only when the page was full. A page shorter than the limit is the end of the result set, and
  * offering a cursor for it would produce an empty page and a "load more" control that does nothing.
  */
final case class SearchPage(rows: Vector[EventSummary], nextCursor: Option[String])

/** A search, validated.
  *
  * The constructor is private so that a `limit` can never arrive unbounded: this query is served from a pool of eight
  * connections behind a two-second statement timeout, and one request asking for a million rows is a way to spend the
  * whole pool's time budget and then serialise the result into the web tier's heap.
  */
final case class SearchRequest private (
  filter: Option[Filter],
  sort: SortDirection,
  limit: Int,
  cursor: Option[Cursor]
):

  /** Identifies the query this page belongs to; stamped into the cursor of the next page. Lazy because most requests
    * never mint a cursor, and it is a SHA-256 over the AST's canonical form.
    */
  lazy val fingerprint: Fingerprint = Fingerprint.of(filter, sort)

object SearchRequest:

  val DefaultLimit: Int = 50

  /** Chosen to keep one page inside a single Postgres page-cache-friendly index scan and one HTML response inside a
    * size a browser renders without jank. It is a product limit, not a technical one, and it is enforced here rather
    * than in the web tier so cobalt and any future caller inherit it.
    */
  val MaxLimit: Int = 500

  def first(filter: Option[Filter], sort: SortDirection, limit: Int): Either[String, SearchRequest] =
    checkLimit(limit).map(SearchRequest(filter, sort, _, None))

  /** Builds a request from an encoded cursor, rejecting one minted for a different filter.
    *
    * The cursor is checked against *this* request's fingerprint, which is why decoding lives here and not in the web
    * tier: the only place that knows both the filter and the cursor is the place that is about to run the query.
    */
  def of(
    filter: Option[Filter],
    sort: SortDirection,
    limit: Int,
    cursor: Option[String]
  ): Either[String, SearchRequest] =
    for
      checked <- checkLimit(limit)
      request = SearchRequest(filter, sort, checked, None)
      decoded <- cursor.fold[Either[String, Option[Cursor]]](Right(None)) { encoded =>
        Cursor.decode(encoded, request.fingerprint).left.map(_.message).map(Some.apply)
      }
    yield request.copy(cursor = decoded)

  /** The same decode, keeping the structured reason. ferrite distinguishes them: a malformed cursor is a bad request, a
    * changed filter is a silent restart at page one.
    */
  def decodeCursor(
    filter: Option[Filter],
    sort: SortDirection,
    encoded: String
  ): Either[CursorError, Cursor] =
    Cursor.decode(encoded, Fingerprint.of(filter, sort))

  private def checkLimit(limit: Int): Either[String, Int] =
    if limit < 1 then Left("limit must be at least 1")
    else if limit > MaxLimit then Left(s"limit must be at most $MaxLimit")
    else Right(limit)

/** The dimensions the facet panel offers.
  *
  * `groupingMask` is the value PostgreSQL's `GROUPING()` returns for the row belonging to this dimension: a bit is set
  * for every argument *not* in that grouping set. Encoding it here rather than emitting one query per dimension is the
  * whole point of ADR §6.3's single-pass facet query — six queries over the same candidate set is six scans.
  *
  * The masks assume the argument order `(ce_type, ce_source, device_id, room_id, person_id, severity)` used in
  * `PostgresEventRepository`. Changing that order without changing these numbers silently relabels every facet.
  */
enum FacetDimension(val groupingMask: Int, val label: String):
  case Type extends FacetDimension(31, "type")
  case Source extends FacetDimension(47, "source")
  case Device extends FacetDimension(55, "device")
  case Room extends FacetDimension(59, "room")
  case Person extends FacetDimension(61, "person")
  case Severity extends FacetDimension(62, "severity")

object FacetDimension:

  /** The grand total row of the same `GROUPING SETS`, which is how the candidate count comes back for free. */
  val TotalMask: Int = 63

  private val ByMask: Map[Int, FacetDimension] = values.iterator.map(d => d.groupingMask -> d).toMap

  def fromMask(mask: Int): Option[FacetDimension] = ByMask.get(mask)

/** Facet counts, plus the honesty flag.
  *
  * @param candidates
  *   how many rows the capped candidate set actually held.
  * @param capped
  *   true when the cap was reached, in which case every count is a lower bound and the UI must render "50,000+" rather
  *   than a number. ADR §6.3 makes this a signed-off product decision, not an implementation detail — a count that is
  *   silently approximate is a count that will be quoted in a meeting.
  */
final case class Facets(
  dimensions: Map[FacetDimension, Vector[FacetValue]],
  tags: Vector[FacetValue],
  candidates: Long,
  capped: Boolean
)

/** A facet query, validated. */
final case class FacetRequest private (filter: Option[Filter], candidateCap: Int, perDimension: Int)

object FacetRequest:

  /** The cap from ADR §6.3. Above it the UI renders "50,000+". */
  val DefaultCandidateCap: Int = 50000

  val MaxCandidateCap: Int = 200000

  val DefaultPerDimension: Int = 20

  def of(
    filter: Option[Filter],
    candidateCap: Int = DefaultCandidateCap,
    perDimension: Int = DefaultPerDimension
  ): Either[String, FacetRequest] =
    if candidateCap < 1 then Left("candidate cap must be at least 1")
    else if candidateCap > MaxCandidateCap then Left(s"candidate cap must be at most $MaxCandidateCap")
    else if perDimension < 1 then Left("facet size must be at least 1")
    else Right(FacetRequest(filter, candidateCap, perDimension))

/** A histogram query over a half-open window, with a server-chosen bucket width. */
final case class HistogramRequest private (
  filter: Option[Filter],
  from: OffsetDateTime,
  until: OffsetDateTime,
  width: FiniteDuration
):

  /** The start of the last bucket that begins before `until`.
    *
    * Computed here rather than left to `generate_series(from, until, width)` because that form emits a bucket *starting
    * at* `until` whenever the span divides evenly — a trailing always-zero bar on every chart whose window is a round
    * number of hours, which is most of them.
    */
  def lastBucketStart: OffsetDateTime =
    val span = Duration.between(from, until).getSeconds
    val step = width.toSeconds
    val steps = if span <= 0 then 0L else (span - 1) / step
    from.plusSeconds(steps * step)

object HistogramRequest:

  /** Kibana-shaped: enough bars to show shape, few enough that each is wider than a pixel. ADR §6.3 fixes the range at
    * 60–120; the target sits in the middle so that a window growing slightly does not flip the width every reload.
    */
  val TargetBuckets: Int = 90

  val MaxBuckets: Int = 120

  /** A fixed ladder rather than `span / TargetBuckets` rounded.
    *
    * An arbitrary width — 7 minutes, 43 seconds — produces buckets whose boundaries fall in the middle of minutes and
    * hours, so two charts of overlapping windows cannot be compared and no bar lines up with anything a human reads off
    * a clock.
    */
  val Ladder: Vector[FiniteDuration] = Vector(
    1.second,
    5.seconds,
    15.seconds,
    30.seconds,
    1.minute,
    5.minutes,
    15.minutes,
    30.minutes,
    1.hour,
    3.hours,
    6.hours,
    12.hours,
    1.day,
    7.days,
    30.days
  )

  def widthFor(from: OffsetDateTime, until: OffsetDateTime): FiniteDuration =
    val span = math.max(1L, Duration.between(from, until).getSeconds)
    Ladder.find(step => span / step.toSeconds <= MaxBuckets).getOrElse(Ladder.last)

  def of(filter: Option[Filter], from: OffsetDateTime, until: OffsetDateTime): Either[String, HistogramRequest] =
    if !from.isBefore(until) then Left(s"histogram window is empty: '$from' is not before '$until'")
    else Right(HistogramRequest(filter, from, until, widthFor(from, until)))

/** The database surface the two consuming services need, and nothing else.
  *
  * Split by direction on purpose: ferrite calls only the read methods, cobalt only [[insertAll]]. Keeping them in one
  * trait rather than two is a bet that the split is a fact about *callers* and not about the schema — both halves talk
  * to the same table and any divergence between how a row is written and how it is read is a bug this interface should
  * make obvious rather than hide behind two files.
  *
  * **No Magnum type appears in any signature** (ADR §5). Callers see domain values and `Future`, so the ADR's budgeted
  * Magnum 2.0 migration rewrites implementations and touches nothing above them.
  *
  * Every method returns `Future` because the implementation is blocking JDBC on a bounded dispatcher (ADR §0, decision
  * 8): the pool, not the caller, is where load is shed.
  */
trait EventRepository:

  /** A keyset page. Never `OFFSET` — see [[Cursor]]. */
  def search(request: SearchRequest): Future[SearchPage]

  /** Facet counts over a capped candidate set, in a single pass. */
  def facets(request: FacetRequest): Future[Facets]

  /** Bucketed counts with empty buckets materialised as zero, so the chart shows the shape of the data rather than a
    * shape invented by collapsing gaps.
    */
  def histogram(request: HistogramRequest): Future[Vector[HistogramBucket]]

  /** One event with its payload, by primary key. */
  def find(ref: EventRef): Future[Option[EventDetail]]

  /** `count(*)` bounded by `cap`, returning up to `cap + 1`.
    *
    * An exact total over a partitioned fact table is a full scan, and nobody reads past "10,000+" (ADR §6.3). A caller
    * that gets `cap + 1` back renders the cap with a plus sign.
    */
  def countAtMost(filter: Option[Filter], cap: Int): Future[Long]

  /** Idempotent batch insert.
    *
    * Returns the number of rows actually written, which for a replayed batch is less than `events.size` — that
    * difference is the `consume.records.duplicate` metric of ADR §7 and the direct evidence that at-least-once delivery
    * is being absorbed rather than duplicated.
    *
    * `Long` rather than `Int` because that is what JDBC's large-batch update count is, and narrowing it here would put
    * a silent truncation on the one number that proves deduplication works.
    */
  def insertAll(events: Vector[NewEvent]): Future[Long]
