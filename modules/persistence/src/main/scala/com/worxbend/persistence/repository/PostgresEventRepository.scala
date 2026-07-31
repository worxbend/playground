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

import com.augustnagro.magnum.BatchUpdateResult
import com.augustnagro.magnum.DbCodec
import com.augustnagro.magnum.Frag
import com.augustnagro.magnum.Transactor
import com.augustnagro.magnum.batchUpdate
import com.augustnagro.magnum.connect
import com.augustnagro.magnum.transact
import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.Codecs
import com.worxbend.persistence.Sql
import com.worxbend.persistence.Sql.++
import com.worxbend.persistence.search.Cursor
import com.worxbend.persistence.search.FilterSql
import com.worxbend.persistence.search.SortDirection
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** One row of the `GROUPING SETS` facet pass.
  *
  * Six nullable dimension columns of which exactly one is meaningful per row, selected by `gid`. That shape is what a
  * single-pass grouping-sets query returns; reshaping it into something sane happens in Scala rather than as six
  * `UNION ALL` branches in SQL, each of which would be another scan of the candidate set.
  */
final private[repository] case class FacetRow(
  gid: Int,
  ceType: Option[String],
  ceSource: Option[String],
  deviceId: Option[String],
  roomId: Option[String],
  personId: Option[String],
  severity: Option[String],
  count: Long
) derives DbCodec

/** The PostgreSQL implementation.
  *
  * Two transactors, not one: the read side is bound to the search pool (read-only, `statement_timeout = 2s`) and the
  * write side to the ingest pool, so ADR §5's isolation between a runaway search and durable persistence is expressed
  * in the constructor rather than trusted to convention.
  *
  * The `ExecutionContext` is expected to be the bounded `CustomExecutionContext` sized to the pool (ADR §0, decision
  * 8). Handing this class the global pool would work, and would relocate the queue from somewhere with a timeout to
  * somewhere without one.
  *
  * **Every statement is built by a pure function in the companion**, not inline here. That is what makes the SQL
  * testable without a database — a missing `FROM` clause or a parameter bound out of order is a defect a unit test can
  * find, and it is exactly the kind of defect that otherwise waits for an integration run on a Docker host.
  */
final class PostgresEventRepository(read: Transactor, write: Transactor)(using ec: ExecutionContext)
    extends EventRepository:

  import PostgresEventRepository.*

  def search(request: SearchRequest): Future[SearchPage] =
    Future:
      val rows = connect(read)(searchSql(request).query[EventSummary].run())
      SearchPage(rows, nextCursor(request, rows))

  def facets(request: FacetRequest): Future[Facets] =
    Future:
      connect(read):
        val dimensionRows = facetDimensionsSql(request).query[FacetRow].run()
        val tagRows = facetTagsSql(request).query[FacetValue].run()
        reshape(dimensionRows, tagRows, request)

  def histogram(request: HistogramRequest): Future[Vector[HistogramBucket]] =
    Future(connect(read)(histogramSql(request).query[HistogramBucket].run()))

  def find(ref: EventRef): Future[Option[EventDetail]] =
    Future(connect(read)(findSql(ref).query[EventDetail].run()).headOption)

  def countAtMost(filter: Option[Filter], cap: Int): Future[Long] =
    Future(connect(read)(countSql(filter, cap).query[Long].run()).headOption.getOrElse(0L))

  def insertAll(events: Vector[NewEvent]): Future[Long] =
    Future:
      if events.isEmpty then 0L
      else
        transact(write):
          batchUpdate(events)(event => insertSql(event).update) match
            case BatchUpdateResult.Success(rows) => rows
            // The driver may report SUCCESS_NO_INFO for a batch. Assuming every row landed is the conservative
            // direction: it can only under-report duplicates, never claim a write that did not happen.
            case BatchUpdateResult.SuccessNoInfo => events.size.toLong

  private def nextCursor(request: SearchRequest, rows: Vector[EventSummary]): Option[String] =
    if rows.sizeIs < request.limit then None
    else rows.lastOption.map(row => Cursor(row.occurredAt, row.eventUid, request.fingerprint).encode)

  private def reshape(rows: Vector[FacetRow], tags: Vector[FacetValue], request: FacetRequest): Facets =
    val candidates = rows.find(_.gid == FacetDimension.TotalMask).map(_.count).getOrElse(0L)
    val dimensions = rows
      .flatMap: row =>
        FacetDimension
          .fromMask(row.gid)
          // A NULL dimension value means "these rows have no room at all", which is not a facet a user can select.
          .flatMap(dimension => valueOf(dimension, row).map(value => dimension -> FacetValue(value, row.count)))
      .groupMap((dimension, _) => dimension)((_, value) => value)
      .view
      .mapValues(values => values.sortBy(value => (-value.count, value.value)).take(request.perDimension))
      .toMap
    Facets(dimensions, tags, candidates, candidates >= request.candidateCap)

/** The statement builders.
  *
  * Deliberately pure, package-visible and separate from the class that runs them. A `Frag` is a value: it can be
  * asserted on for placeholder count, parameter order and the presence of a `FROM` clause without a connection, which
  * is the only kind of test that runs on the fast CI tier.
  */
object PostgresEventRepository:

  /** The list projection of ADR §6.3 — everything except `data` and `raw`.
    *
    * Split across two `lit` calls only to stay inside the line limit. The column order is the field order of
    * [[EventSummary]] and Magnum's derived codec reads positionally, so the two must be edited together.
    */
  private val summaryColumns: Frag =
    Sql.lit("SELECT occurred_at, event_uid, ingested_at, ce_id, ce_source, ce_type, ce_subject, ") ++
      Sql.lit("device_id, room_id, person_id, severity, severity_rank, metric_value")

  /** A keyset page over `events.cloud_event`. */
  private[persistence] def searchSql(request: SearchRequest): Frag =
    summaryColumns ++
      Sql.lit(" FROM events.cloud_event") ++
      FilterSql.whereOf(request.filter) ++
      keyset(request) ++
      order(request.sort) ++
      Sql.lit(" LIMIT ") ++ Sql.bind(request.limit)

  /** The capped candidate set both facet passes share.
    *
    * `MATERIALIZED` is not optional: without it the planner is free to inline the CTE into each grouping set and
    * re-apply the filter, turning one capped scan into several uncapped ones and deleting the cap's whole purpose.
    */
  private def candidates(request: FacetRequest): Frag =
    Sql.lit("WITH cand AS MATERIALIZED (SELECT ce_type, ce_source, device_id, room_id, person_id, ") ++
      Sql.lit("severity, tags FROM events.cloud_event") ++
      FilterSql.whereOf(request.filter) ++
      Sql.lit(" LIMIT ") ++ Sql.bind(request.candidateCap) ++ Sql.lit(") ")

  /** Six facets in one pass.
    *
    * The trailing `()` grouping set is the grand total, which is how the candidate count — and therefore the "was the
    * cap reached" flag — comes back without a second query. The argument order of `grouping(...)` is what
    * [[FacetDimension.groupingMask]] encodes; changing one without the other relabels every facet.
    */
  private[persistence] def facetDimensionsSql(request: FacetRequest): Frag =
    candidates(request) ++
      Sql.lit("SELECT grouping(ce_type, ce_source, device_id, room_id, person_id, severity), ") ++
      Sql.lit("ce_type, ce_source, device_id, room_id, person_id, severity, count(*) FROM cand ") ++
      Sql.lit("GROUP BY GROUPING SETS ((ce_type), (ce_source), (device_id), (room_id), (person_id), ") ++
      Sql.lit("(severity), ())")

  /** Tags need their own pass: one row can carry several, so they cannot share a `GROUP BY` with scalar dimensions. */
  private[persistence] def facetTagsSql(request: FacetRequest): Frag =
    candidates(request) ++
      Sql.lit("SELECT t, count(*) FROM cand CROSS JOIN LATERAL unnest(cand.tags) AS t ") ++
      Sql.lit("GROUP BY t ORDER BY 2 DESC, 1 LIMIT ") ++ Sql.bind(request.perDimension)

  /** Bucketed counts with a `generate_series` skeleton.
    *
    * The skeleton is what makes an empty hour render as a zero bar. Without it a quiet period disappears from the chart
    * and the shape of the data becomes a lie told by omission (ADR §6.3).
    */
  private[persistence] def histogramSql(request: HistogramRequest): Frag =
    val step = s"${request.width.toSeconds} seconds"
    Sql.lit("SELECT s.bucket, coalesce(c.n, 0) FROM generate_series(") ++
      Sql.bind(request.from) ++ Sql.lit(", ") ++ Sql.bind(request.lastBucketStart) ++ Sql.lit(", ") ++
      Sql.bind(step) ++ Sql.lit("::interval) AS s(bucket) LEFT JOIN (SELECT date_bin(") ++
      Sql.bind(step) ++ Sql.lit("::interval, occurred_at, ") ++ Sql.bind(request.from) ++
      Sql.lit(") AS bucket, count(*) AS n FROM events.cloud_event") ++
      FilterSql.whereOf(request.filter) ++
      // Bounded here rather than trusted to the filter: the window is what the chart is *of*, and a caller whose
      // filter is broader would otherwise bin months of events into the first bucket.
      Sql.lit(" AND occurred_at >= ") ++ Sql.bind(request.from) ++
      Sql.lit(" AND occurred_at < ") ++ Sql.bind(request.until) ++
      Sql.lit(" GROUP BY 1) AS c ON c.bucket = s.bucket ORDER BY s.bucket")

  /** One event with its payload, by primary key — both columns, so the lookup prunes to a single partition. */
  private[persistence] def findSql(ref: EventRef): Frag =
    summaryColumns ++
      Sql.lit(", raw FROM events.cloud_event WHERE occurred_at = ") ++ Sql.bind(ref.occurredAt) ++
      Sql.lit(" AND event_uid = ") ++ Sql.bind(ref.eventUid)

  /** `count(*)` bounded by a cap.
    *
    * `SELECT 1` inside the subquery rather than `count(*)` outside a `LIMIT`: the `LIMIT` is what stops the scan, and
    * it can only stop a scan that is producing rows.
    */
  private[persistence] def countSql(filter: Option[Filter], cap: Int): Frag =
    Sql.lit("SELECT count(*) FROM (SELECT 1 FROM events.cloud_event") ++
      FilterSql.whereOf(filter) ++
      Sql.lit(" LIMIT ") ++ Sql.bind(cap + 1) ++ Sql.lit(") AS bounded")

  /** The idempotent insert.
    *
    * `ON CONFLICT (occurred_at, ce_source, ce_id)` names index (1) of ADR §5, whose columns include the partition key
    * because a unique index on a partitioned table cannot be enforced without it. Two of the three conflict columns are
    * *generated* from `raw`, which is what makes this dedup key impossible to disagree with the payload: the writer
    * cannot supply an identity different from the one the event carries.
    */
  private[persistence] def insertSql(event: NewEvent): Frag =
    Sql.lit("INSERT INTO events.cloud_event (occurred_at, raw, payload_sha256) VALUES (") ++
      Sql.bind(event.occurredAt) ++ Sql.lit(", ") ++
      Sql.bind(event.raw)(using Codecs.jsonb) ++ Sql.lit("::jsonb, ") ++
      Sql.bind(event.payloadSha256) ++
      Sql.lit(") ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING")

  /** The keyset predicate, compiled by hand.
    *
    * Magnum's `Spec.seek` ANDs single-column seeks, which is a strictly different predicate: `a < x AND b < y` skips
    * every row with `a < x` but `b >= y`. Only the row-value form `(occurred_at, event_uid) < (?, ?)` is a seek that
    * Postgres pushes into the composite btree as a single descent (ADR §6.3).
    */
  private def keyset(request: SearchRequest): Frag =
    request.cursor.fold(Sql.empty): cursor =>
      val comparison = request.sort match
        case SortDirection.Newest => Sql.lit(" AND (occurred_at, event_uid) < (")
        case SortDirection.Oldest => Sql.lit(" AND (occurred_at, event_uid) > (")
      comparison ++ Sql.bind(cursor.occurredAt) ++ Sql.lit(", ") ++ Sql.bind(cursor.eventUid) ++ Sql.lit(")")

  private def order(sort: SortDirection): Frag = sort match
    case SortDirection.Newest => Sql.lit(" ORDER BY occurred_at DESC, event_uid DESC")
    case SortDirection.Oldest => Sql.lit(" ORDER BY occurred_at ASC, event_uid ASC")

  private def valueOf(dimension: FacetDimension, row: FacetRow): Option[String] = dimension match
    case FacetDimension.Type     => row.ceType
    case FacetDimension.Source   => row.ceSource
    case FacetDimension.Device   => row.deviceId
    case FacetDimension.Room     => row.roomId
    case FacetDimension.Person   => row.personId
    case FacetDimension.Severity => row.severity
