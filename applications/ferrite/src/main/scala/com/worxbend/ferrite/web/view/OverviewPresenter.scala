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

package com.worxbend.ferrite.web.view

import com.worxbend.ferrite.overview.Overview
import com.worxbend.ferrite.overview.OverviewRange
import com.worxbend.ferrite.search.SearchQuery
import com.worxbend.ferrite.web.Query
import com.worxbend.ferrite.web.Urls
import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.search.Severity
import com.worxbend.persistence.repository.RollupDimension
import com.worxbend.persistence.repository.RollupSlice
import com.worxbend.persistence.repository.VolumePoint
import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

/** [[Overview]] in, [[OverviewPage]] out. Pure, total, and the only place the two meet.
  *
  * **Every element on the overview is a way into `/events`.** That is the design constraint this object exists to
  * enforce: a dashboard whose panels are dead ends makes an operator retype what they just read into a search box, and
  * a number retyped is a number mistyped. So each link is built here, through [[SearchQuery]] and [[Query]] — never by
  * concatenating a query string at a call site, because the permalink grammar is versioned and the version has to be
  * re-stamped on anything the UI emits.
  *
  * Every link also carries the *window* the number was computed over. Clicking "1,204 events" and landing on a search
  * over a different period would show a different number, and the user would be right to conclude the dashboard is
  * wrong.
  */
object OverviewPresenter:

  /** Quantised bar widths, matching `Presenter.heightClass`'s twenty-one steps. Prefixed `bar-w-` so it cannot collide
    * with a Tailwind width utility.
    */
  def widthClass(percent: Int): String =
    val clamped = math.max(0, math.min(100, percent))
    val quantised = if clamped == 0 then 0 else math.max(5, (clamped + 2) / 5 * 5)
    s"bar-w-$quantised"

  def page(overview: Overview, now: OffsetDateTime): OverviewPage =
    val base = windowQuery(overview.window.from, overview.window.until)
    OverviewPage(
      ranges = ranges(overview.range),
      rangeLabel = overview.range.label,
      fromLabel = Format.absolute(overview.window.from),
      untilLabel = Format.absolute(overview.window.until),
      tiles = tiles(overview, base),
      volume = volume(overview.volume, overview.range.step.width, overview.window.from, overview.window.until, base),
      panels = Vector(
        panel(RollupDimension.Source, "Busiest sources", overview.sources, overview.totals.events, base),
        panel(RollupDimension.Type, "Event types", overview.types, overview.totals.events, base),
        panel(RollupDimension.Severity, "Severity mix", overview.severities, overview.totals.events, base)
      ),
      alerts = overview.alerts.map(Presenter.row(_, now)),
      alertsUrl = Urls.events(base.link(Query.set(_, SearchQuery.SeverityKey, s">=$severityLabel"))),
      alertsNote =
        if overview.alerts.nonEmpty then ""
        else s"Nothing at $severityLabel or above in the last ${overview.range.label.toLowerCase}.",
      freshness = freshness(overview.freshness),
      stale = overview.freshness.isEmpty,
      searchUrl = Urls.events(base.permalink)
    )

  /** The three range links, each preserving nothing but the range — there is no other state on this page. */
  def ranges(selected: OverviewRange): Vector[RangeOption] =
    OverviewRange.values.toVector.map { range =>
      RangeOption(
        key = range.key,
        label = range.label,
        url = Urls.overview(Query.render(Vector(OverviewRange.Key -> range.key))),
        selected = range == selected
      )
    }

  /** The headline numbers.
    *
    * Three, not six. Every tile is a claim an operator has to hold in their head while reading the panels below, and
    * the fourth one is where a dashboard starts being scenery.
    */
  def tiles(overview: Overview, base: SearchQuery): Vector[Tile] =
    val events = overview.totals.events
    val errors = overview.totals.errors
    val peak = overview.volume.maxByOption(_.events)
    Vector(
      Tile(
        label = "Events",
        value = Format.count(events),
        detail = s"in the last ${overview.range.label.toLowerCase}",
        url = Urls.events(base.permalink),
        tone = ""
      ),
      Tile(
        label = "Errors",
        value = Format.count(errors),
        // The share is the reading, not the count: "412" means nothing without the denominator, and an operator
        // comparing two days is comparing rates whether or not the page helps them.
        detail = s"${percent(errors, events)} of traffic at $severityLabel or above",
        url = Urls.events(base.link(Query.set(_, SearchQuery.SeverityKey, s">=$severityLabel"))),
        tone = if errors > 0 then Presenter.tone(Some(severityLabel)) else ""
      ),
      peak.fold(
        Tile("Busiest bucket", Format.Absent, "no events in this window", Urls.events(base.permalink), "")
      ) { point =>
        Tile(
          label = "Busiest bucket",
          value = Format.count(point.events),
          detail = s"beginning ${Format.bucket(point.bucket)}",
          url = Urls.events(bucketQuery(base, point.bucket, overview.range.step.width)),
          tone = ""
        )
      }
    )

  /** The volume chart, as the same [[Histogram]] model the search page's timeline uses.
    *
    * Sharing the model is what keeps the two charts legible as the same object: identical bar heights, identical
    * quantisation, identical "empty window" wording. What is *not* shared is the template — the search timeline drives
    * an htmx swap of `#results`, which does not exist on this page, and pointing `hx-target` at a missing element is a
    * click that silently does nothing.
    */
  def volume(
    points: Vector[VolumePoint],
    width: FiniteDuration,
    from: OffsetDateTime,
    until: OffsetDateTime,
    base: SearchQuery
  ): Histogram =
    val peak = points.map(_.events).maxOption.getOrElse(0L)
    val bars = points.map { point =>
      val height = Format.heightPercent(point.events, peak)
      Bar(
        start = Rfc3339.render(point.bucket),
        startLabel = Format.bucket(point.bucket),
        count = point.events,
        countLabel = Format.count(point.events),
        heightPercent = height,
        heightClass = Presenter.heightClass(height),
        url = Urls.events(bucketQuery(base, point.bucket, width))
      )
    }
    Histogram(
      bars = bars,
      widthLabel = Format.duration(width),
      fromLabel = Format.absolute(from),
      untilLabel = Format.absolute(until),
      peakLabel = Format.count(peak),
      empty = peak == 0L
    )

  /** One breakdown panel.
    *
    * Shares are computed against the window total rather than against the largest row, unlike the histogram. The
    * question a leaderboard answers is "how much of the traffic is this", and scaling to the leader would make the
    * busiest source a full bar on every system, whether it is 90 % of traffic or 9 %.
    */
  def panel(
    dimension: RollupDimension,
    heading: String,
    slices: Vector[RollupSlice],
    total: Long,
    base: SearchQuery
  ): Panel =
    Panel(
      key = dimension.filterKey,
      heading = heading,
      note = "Nothing in this window.",
      rows = slices.map { slice =>
        val share = Format.heightPercent(slice.events, total)
        MeterRow(
          label = slice.value,
          count = Format.count(slice.events),
          errorCount = if slice.errors > 0 then Format.count(slice.errors) else "",
          share = share,
          shareClass = widthClass(share),
          url = link(dimension, slice.value, base).map(Urls.events),
          tone = if dimension == RollupDimension.Severity then Presenter.tone(Some(slice.value)) else ""
        )
      }
    )

  /** The search one breakdown row stands for, or `None` when the grammar cannot express it.
    *
    * Severity is the case that forces the `Option`. The filter grammar has only `SeverityAtLeast` (ADR §6.1), so a
    * severity row means "at least this level" and a value the domain does not recognise — including the rollup's own
    * `'none'` placeholder for events that carried no severity at all — has no expressible search. Emitting `>=none`
    * would produce a link that 400s, which is worse than a row that is plainly not clickable.
    */
  def link(dimension: RollupDimension, value: String, base: SearchQuery): Option[String] = dimension match
    case RollupDimension.Severity =>
      Severity.parse(value).toOption.map(level => base.link(Query.set(_, dimension.filterKey, s">=${level.label}")))
    case _ => Option.when(value.nonEmpty)(base.toggled(dimension.filterKey, value))

  /** The window every link on the page carries.
    *
    * Built as a `SearchQuery` and not as a string: `SearchQuery.link` re-stamps the grammar version on anything
    * non-empty and refuses to carry a cursor over, and both of those are invariants no call site should be trusted to
    * remember.
    */
  def windowQuery(from: OffsetDateTime, until: OffsetDateTime): SearchQuery =
    SearchQuery.lenient(
      Query.render(
        Vector(
          SearchQuery.FromKey -> Rfc3339.render(from),
          SearchQuery.UntilKey -> Rfc3339.render(until)
        )
      )
    )

  /** One bucket's half-open window, replacing the page window. */
  private def bucketQuery(base: SearchQuery, bucket: OffsetDateTime, width: FiniteDuration): String =
    base.within(Rfc3339.render(bucket), Rfc3339.render(bucket.plusSeconds(width.toSeconds)))

  /** Whole percent, with the zero-denominator case answered rather than divided. */
  private def percent(part: Long, whole: Long): String =
    if whole <= 0L then "0%" else s"${math.round(part.toDouble / whole.toDouble * 100.0)}%"

  /** What the alert tile and the alert feed mean by "error or above", spelled once so the tile, the feed's link and the
    * query that produced the rows cannot disagree.
    */
  private val severityLabel: String = com.worxbend.ferrite.overview.OverviewService.AlertLevel.label

  /** The staleness sentence.
    *
    * Two cases, and the distinction between them is the useful part. An empty rollup is an operational fact — the
    * refresh job is not running — and can be stated as one. A rollup whose newest bucket is old is *ambiguous*: on a
    * quiet system that is simply correct. So the second case reports the time and names the metric that tells the two
    * apart, rather than guessing on the operator's behalf.
    */
  def freshness(newest: Option[OffsetDateTime]): String = newest match
    case None =>
      "The hourly rollup holds no rows, so every count here is zero regardless of what has been ingested. " +
        "Nothing refreshes a materialized view on its own: check cobalt's maintenance schedule."
    case Some(bucket) =>
      s"Counts come from the hourly rollup, current to ${Format.bucket(bucket)}. It is refreshed on a schedule, so " +
        "the newest few minutes are in search and in the live tail but not here. If that time is far behind the " +
        """clock, maintenance_job_duration_seconds{job="rollup-refresh"} says whether the job stopped or the """ +
        "system is simply quiet."
