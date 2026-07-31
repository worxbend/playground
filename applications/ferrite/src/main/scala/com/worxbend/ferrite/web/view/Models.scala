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
  sortLabel: String
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
