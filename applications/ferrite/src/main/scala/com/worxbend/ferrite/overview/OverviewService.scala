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

package com.worxbend.ferrite.overview

import com.worxbend.ferrite.search.SearchExecutionContext
import com.worxbend.ferrite.search.TimeWindow
import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.Severity
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.EventSummary
import com.worxbend.persistence.repository.OverviewRepository
import com.worxbend.persistence.repository.OverviewRequest
import com.worxbend.persistence.repository.RollupDimension
import com.worxbend.persistence.repository.RollupSlice
import com.worxbend.persistence.repository.RollupStep
import com.worxbend.persistence.repository.RollupTotals
import com.worxbend.persistence.repository.SearchRequest
import com.worxbend.persistence.repository.VolumePoint
import com.worxbend.persistence.search.SortDirection
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.OffsetDateTime
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** How far back the overview looks, and how coarsely.
  *
  * A closed set of three, each pairing a span with a bucket width that keeps the chart between roughly twenty-four and
  * thirty bars. That range is not cosmetic: fewer and the shape of the traffic is not visible, more and each bar is
  * thinner than the gap beside it. The rollup's grain is one hour, so every width here is a whole number of hours.
  *
  * `key` is what appears in the URL. Short and stable, because it is the one piece of state the overview keeps in a
  * link somebody may paste into a runbook.
  */
enum OverviewRange(val key: String, val label: String, val span: FiniteDuration, val step: RollupStep):
  case Day extends OverviewRange("24h", "24 hours", 24.hours, RollupStep.Hour)
  case Week extends OverviewRange("7d", "7 days", 7.days, RollupStep.SixHours)
  case Month extends OverviewRange("30d", "30 days", 30.days, RollupStep.Day)

object OverviewRange:

  val Key: String = "range"

  val Default: OverviewRange = Day

  /** Unknown values fall back to the default rather than failing.
    *
    * The opposite of how `/events` treats a bad parameter, and deliberately: a rejected *search* silently narrowed
    * would show the user results their filter did not ask for, which is a correctness problem. A rejected *range* has
    * no such hazard — the page says which range it is showing, and refusing to render a dashboard because someone
    * hand-edited `?range=` is a worse outcome than showing the default one.
    */
  def parse(raw: Option[String]): OverviewRange =
    raw.flatMap(value => values.find(_.key == value.trim.toLowerCase)).getOrElse(Default)

/** Everything the overview renders, fetched once.
  *
  * @param freshness
  *   the newest hour the rollup holds. `None` means the view has never been refreshed against a populated fact table,
  *   which the page must say out loud — every number here would then be a confident zero.
  */
final case class Overview(
  range: OverviewRange,
  window: TimeWindow,
  totals: RollupTotals,
  volume: Vector[VolumePoint],
  types: Vector[RollupSlice],
  sources: Vector[RollupSlice],
  severities: Vector[RollupSlice],
  alerts: Vector[EventSummary],
  freshness: Option[OffsetDateTime]
)

/** The application service behind `/`.
  *
  * **Six queries, none of them awaited before all six are issued** — the same discipline as `SearchService`, for the
  * same reason: they are independent, they run on a pool of eight, and a `for` comprehension over the calls would
  * sequence them and turn one page into six serial round trips.
  *
  * **Five of the six read the rollup; exactly one reads the fact table.** That split is the whole point of the page.
  * `events.event_rollup_hourly` collapses a month of events into a few thousand rows, so the volume chart, the totals
  * and the three breakdowns cost the same for thirty days as for one. The exception is the alert feed, which needs
  * individual events and gets them through index (11) of ADR §5 — the partial index over `severity_rank >= 50`, which
  * covers well under one percent of rows and exists precisely for "the last twenty alerts".
  *
  * @param clock
  *   injected, not `OffsetDateTime.now()`, so the window is deterministic in a test. A dashboard whose window depends
  *   on the wall clock cannot have its links asserted.
  */
@Singleton
final class OverviewService @Inject() (
  rollup: OverviewRepository,
  events: EventRepository,
  clock: Clock
)(using ec: SearchExecutionContext):

  import OverviewService.*

  /** The whole page.
    *
    * Returns `Left` only when the range produced an unusable window, which cannot happen for the three values
    * [[OverviewRange]] admits — the branch exists so that a fourth added carelessly fails loudly here instead of
    * rendering an empty chart.
    */
  def load(range: OverviewRange): Future[Either[String, Overview]] =
    val until = OffsetDateTime.now(clock)
    val from = until.minusSeconds(range.span.toSeconds)
    OverviewRequest.of(from, until, range.step) match
      case Left(reason)   => Future.successful(Left(reason))
      case Right(request) =>
        val volumeF = rollup.volume(request)
        val typesF = rollup.breakdown(RollupDimension.Type, request)
        val sourcesF = rollup.breakdown(RollupDimension.Source, request)
        val severitiesF = rollup.breakdown(RollupDimension.Severity, request)
        val totalsF = rollup.totals(request)
        val freshnessF = rollup.freshness()
        val alertsF = alerts(request.from, until)

        for
          volume <- volumeF
          types <- typesF
          sources <- sourcesF
          severities <- severitiesF
          totals <- totalsF
          freshness <- freshnessF
          recent <- alertsF
        yield Right(
          Overview(
            range = range,
            // The window reported to the UI is the *aligned* one the queries actually ran, not the raw `now - span`.
            // A chart labelled with a start it did not use is a chart whose first bar is a lie about its own bounds.
            window = TimeWindow(request.from, until),
            totals = totals,
            volume = volume,
            types = types,
            sources = sources,
            severities = severities,
            alerts = recent,
            freshness = freshness
          )
        )

  /** The newest few events at error severity or worse, inside the window.
    *
    * Bounded by the window as well as by the count, so the feed cannot show an alert from last month next to a chart of
    * the last day — two panels describing different periods on one screen is how an operator misreads a page.
    *
    * A failure to build the filter degrades to an empty feed: the alert list is one panel of six, and a page that
    * refuses to render because one sub-query could not be constructed is worse than a page missing one panel.
    */
  private def alerts(from: OffsetDateTime, until: OffsetDateTime): Future[Vector[EventSummary]] =
    val filter =
      for
        occurred <- Filter.occurred(Some(from), Some(until))
        combined <- Filter.and(Vector(occurred, Filter.severityAtLeast(AlertLevel)))
      yield combined
    filter.flatMap(f => SearchRequest.first(Some(f), SortDirection.Newest, AlertCount)) match
      case Left(_)        => Future.successful(Vector.empty)
      case Right(request) => events.search(request).map(_.rows)

object OverviewService:

  /** What counts as an alert here, and it is the same 50 the partial index `cloud_event_alerts_ix` is built on. If this
    * ever drops below `error`, the feed stops being an index scan and becomes a table scan with a `LIMIT`.
    */
  val AlertLevel: Severity = Severity.Error

  /** Enough to see whether something is on fire, few enough to read at a glance. The panel links to the full search for
    * the same window, which is where "all of them" lives.
    */
  val AlertCount: Int = 8
