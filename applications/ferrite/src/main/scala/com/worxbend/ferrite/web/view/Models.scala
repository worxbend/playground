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

/** The view model: flat, primitive-typed records and nothing else (ADR §8.4).
  *
  * Templates never see a domain entity, a repository row or a `Filter`. That is not ceremony — Twirl templates are the
  * one place in this build where the compiler's help is thinnest, and every piece of logic that leaks into them
  * (formatting a timestamp, deciding a colour, building a URL) becomes untestable and duplicated across the fragment
  * and the page that wraps it. Every decision is made in [[Presenter]], asserted by unit tests, and arrives here as a
  * `String` the template only has to place.
  *
  * The corollary: **no field in this package may contain markup**. Twirl escapes everything by default and nothing in
  * these templates uses `@Html` on a value derived from user input or from the database. A field that needed markup
  * would be a decision made in the wrong layer.
  */
object ViewModels

/** One row of the results table.
  *
  * `occurredAt` is the machine-readable RFC 3339 form for `<time datetime>`, `occurredAtLabel` the human one and
  * `relative` the "3 minutes ago" text. All three are rendered: the relative form is what a person scanning a feed
  * actually reads, and the absolute form is what they need the moment they are comparing against another system's log.
  *
  * @param severityTone
  *   a stable token (`debug`, `info`, …, `unknown`) the stylesheet keys colour off. A CSS class, not a colour: the
  *   template must not know what red means.
  */
final case class EventRow(
  detailUrl: String,
  occurredAt: String,
  occurredAtLabel: String,
  relative: String,
  eventUid: String,
  ceId: String,
  source: String,
  eventType: String,
  subject: String,
  device: String,
  room: String,
  person: String,
  severity: String,
  severityTone: String,
  metric: String
)

/** One bar of the timeline strip.
  *
  * `heightPercent` is computed server-side against the tallest bar so the template contains no arithmetic, and `url`
  * narrows the search to this bucket — the histogram is a control, not a decoration.
  *
  * `heightClass` exists because the height cannot be an inline `style` attribute: the Content-Security-Policy in
  * `application.conf` deliberately omits `'unsafe-inline'` from `style-src`, and a per-bar inline style is exactly what
  * that directive blocks. Quantising to twenty-one stylesheet classes costs a few percent of visual precision and buys
  * a policy with no inline-style hole in it — for a chart whose job is to show *shape*, that is the right side of the
  * trade.
  */
final case class Bar(
  start: String,
  startLabel: String,
  count: Long,
  countLabel: String,
  heightPercent: Int,
  heightClass: String,
  url: String
)

/** The timeline strip above the results.
  *
  * @param empty
  *   true when every bucket is zero. Rendered as an explicit "no events in this window" strip rather than 90 invisible
  *   bars, which read as a broken chart.
  */
final case class Histogram(
  bars: Vector[Bar],
  widthLabel: String,
  fromLabel: String,
  untilLabel: String,
  peakLabel: String,
  empty: Boolean
):

  /** The same series as data, for the canvas chart that progressively enhances the bars.
    *
    * **A JSON island rather than attributes on each bar.** The Content-Security-Policy has no `'unsafe-inline'` in
    * `script-src`, so the data cannot be a `<script>` literal; it goes in a `<script type="application/json">`, which
    * is inert data and not script, and the CSP does not govern it. Reading ninety `data-` attributes back out of the
    * DOM would work too and would make the parse cost linear in the number of bars for no benefit.
    *
    * `[timestamps, counts]` is uPlot's own column-major shape, so the client hands it straight to the constructor
    * without a transform — and a transform is where a chart quietly starts disagreeing with the table beside it.
    */
  def series: io.circe.Json =
    val seconds = bars.map(bar => io.circe.Json.fromLong(java.time.Instant.parse(bar.start).getEpochSecond))
    val counts = bars.map(bar => io.circe.Json.fromLong(bar.count))
    io.circe.Json.obj(
      "t" -> io.circe.Json.arr(seconds*),
      "v" -> io.circe.Json.arr(counts*),
      // The bucket width, so the chart can draw bars of the right span rather than guessing from the point spacing —
      // which is wrong for the last bucket of a series and for any window with a gap in it.
      "widthSeconds" -> io.circe.Json.fromLong(
        if bars.sizeIs >= 2 then
          java.time.Instant.parse(bars(1).start).getEpochSecond -
            java.time.Instant.parse(bars.head.start).getEpochSecond
        else 0L
      )
    )

/** One selectable value within a facet. `count` is pre-formatted because it may be an approximation ("1,024" or the
  * capped form), and deciding which belongs to [[Presenter]].
  */
final case class FacetEntry(value: String, label: String, count: String, selected: Boolean, url: String)

/** One facet dimension. `key` is the permalink parameter name (`type`, `device`, …) so the panel and the URL agree. */
final case class Facet(key: String, label: String, entries: Vector[FacetEntry])

/** An active filter, rendered as a dismissible chip above the results. */
final case class Chip(label: String, value: String, removeUrl: String)

/** A parameter the permalink codec rejected, positioned on the input that caused it. */
final case class Problem(parameter: String, message: String)

/** The filter bar.
  *
  * The text fields are the raw strings from the query string, *not* values re-rendered from the parsed AST: a user who
  * typed `frm=yesterday` must see what they typed next to the error, and an AST cannot hold a value that failed
  * validation.
  */
final case class FilterBar(
  action: String,
  version: String,
  text: String,
  from: String,
  until: String,
  severity: String,
  sort: String,
  limit: String,
  hidden: Vector[(String, String)],
  chips: Vector[Chip],
  problems: Vector[Problem],
  permalink: String,
  clearUrl: String
)

/** The results region: everything that is swapped when the filter changes.
  *
  * @param emptiness
  *   present exactly when there are no rows, and carries a *reason* rather than a generic apology — the empty state is
  *   the screen a user sees when they are already confused, and "no events in the last 24 hours; widen the window" is a
  *   different problem from "no events match device=kitchen-1".
  */
final case class Results(
  rows: Vector[EventRow],
  histogram: Histogram,
  facets: Vector[Facet],
  totalLabel: String,
  totalIsExact: Boolean,
  facetsCapped: Boolean,
  statusLabel: String,
  moreUrl: Option[String],
  emptiness: Option[Emptiness],
  sortUrl: String,
  sortAria: String,
  sortLabel: String,
  /** `newest` or `oldest`, rendered onto the row container as `data-order`.
    *
    * The live tail prepends arriving rows, which is only the right place for them when the list is newest-first. It
    * reads this attribute rather than being told at page load, because the sort button swaps the results region on its
    * own and a value captured outside that region would go stale the moment somebody flipped the order.
    */
  order: String
)

/** The empty state: what was searched, why nothing came back, and the two things worth trying next. */
final case class Emptiness(headline: String, explanation: String, suggestions: Vector[Suggestion])

/** One actionable way out of an empty or failed state. */
final case class Suggestion(label: String, url: String)

/** A label/value pair in the detail view's attribute tables. */
final case class Field(label: String, value: String)

/** The decoded [[com.worxbend.kernel.event.Observation]].
  *
  * `unrecognised` is not an error flag: ADR §4.2 makes `Unrecognised` the total fallback, so an event this build has
  * never seen still renders — with its raw payload and, where there is one, the reason the registry could not refine
  * it. The UI says so plainly instead of showing an empty panel.
  */
final case class ObservationView(kind: String, fields: Vector[Field], unrecognised: Boolean, reason: String)

/** The event detail page. */
final case class Detail(
  row: EventRow,
  attributes: Vector[Field],
  extensions: Vector[Field],
  observation: ObservationView,
  raw: String,
  backUrl: String
)

/** The error state.
  *
  * Carries the offending parameters and a way back, because a bare status code on a search UI leaves the user holding a
  * URL they cannot fix. `status` drives the HTTP response code too, so the page and the response can never disagree.
  */
final case class Failure(
  status: Int,
  title: String,
  message: String,
  problems: Vector[Problem],
  suggestions: Vector[Suggestion]
)

/** Everything the full list page needs. */
final case class EventsPage(bar: FilterBar, results: Results)

// -------------------------------------------------------------------------------------------------------- overview

/** One entry of the overview's range selector. A link, not a `<select>`: the range is in the URL, so it must be
  * shareable and it must work without JavaScript.
  */
final case class RangeOption(key: String, label: String, url: String, selected: Boolean)

/** A headline number.
  *
  * `url` is never empty. A dashboard number an operator cannot click is a number they then have to reproduce by hand in
  * the search bar, which is the moment they stop trusting it.
  */
final case class Tile(label: String, value: String, detail: String, url: String, tone: String)

/** One row of a breakdown panel: a dimension value, its share of the window, and the search it stands for.
  *
  * `shareClass` carries the bar width for the same reason [[Bar.heightClass]] carries the height — the
  * Content-Security-Policy has no `'unsafe-inline'` in `style-src`, so a per-row `style="width:…"` would not render.
  *
  * `url` is optional, and the case that makes it so is real: the rollup stores `coalesce(severity, 'none')`, and "no
  * severity" is not something the filter grammar can express. A row that cannot be turned into a search is rendered as
  * text rather than as a link that would quietly search for something else.
  */
final case class MeterRow(
  label: String,
  count: String,
  errorCount: String,
  share: Int,
  shareClass: String,
  url: Option[String],
  tone: String
)

/** One breakdown panel. `note` is shown in place of the rows when there are none. */
final case class Panel(key: String, heading: String, note: String, rows: Vector[MeterRow])

/** The overview.
  *
  * @param freshness
  *   a sentence about how current the rollup is, always rendered. Every count on this page comes from a materialized
  *   view refreshed on a schedule, so it is stale by up to one refresh interval — a dashboard that does not say so is a
  *   dashboard that will be quoted against a search result it disagrees with.
  * @param stale
  *   true only when the rollup has never been refreshed against a populated fact table. Deliberately not inferred from
  *   "the newest bucket is old": on a quiet system that is the correct answer, and an alarm that fires on quiet is an
  *   alarm people turn off.
  */
final case class OverviewPage(
  ranges: Vector[RangeOption],
  rangeLabel: String,
  fromLabel: String,
  untilLabel: String,
  tiles: Vector[Tile],
  volume: Histogram,
  panels: Vector[Panel],
  alerts: Vector[EventRow],
  alertsUrl: String,
  alertsNote: String,
  freshness: String,
  stale: Boolean,
  searchUrl: String
)
