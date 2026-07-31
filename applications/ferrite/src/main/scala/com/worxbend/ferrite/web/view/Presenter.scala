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

import com.worxbend.ferrite.search.SearchOutcome
import com.worxbend.ferrite.search.SearchQuery
import com.worxbend.ferrite.search.SearchService
import com.worxbend.ferrite.web.Query
import com.worxbend.ferrite.web.Urls
import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.Observation
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.search.FilterError
import com.worxbend.kernel.search.FilterQuery
import com.worxbend.kernel.search.Severity
import com.worxbend.persistence.repository.EventDetail
import com.worxbend.persistence.repository.EventSummary
import com.worxbend.persistence.repository.FacetDimension
import com.worxbend.persistence.repository.Facets
import com.worxbend.persistence.repository.HistogramBucket
import com.worxbend.persistence.search.SortDirection
import io.circe.Json
import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

/** Domain and repository values in, [[ViewModels]] out. The only place either is allowed to meet the other.
  *
  * Pure and total by construction: no `Future`, no I/O, no clock of its own — `now` is a parameter so that every
  * relative timestamp in a rendered page is measured against the same instant and so tests are deterministic. That
  * makes the whole of the presentation logic — colour tokens, empty-state wording, facet URLs, bar heights — testable
  * without a database, a browser or a Play application, which is the point.
  */
object Presenter:

  /** Severity token -> CSS class suffix. Unknown or absent severities get their own token rather than being folded into
    * `info`: "this event did not say" and "this event said info" are different facts and an operator triaging an
    * incident needs to tell them apart.
    */
  val UnknownSeverity: String = "unknown"

  /** The permalink parameter each facet dimension edits. Keeping the mapping here — rather than deriving it from
    * `FacetDimension.label`, which happens to agree today — means a rename on either side is a compile-time change in
    * one place instead of a facet panel that silently stops filtering.
    */
  val FacetKeys: Vector[(FacetDimension, String)] = Vector(
    FacetDimension.Type -> "type",
    FacetDimension.Source -> "source",
    FacetDimension.Device -> "device",
    FacetDimension.Room -> "room",
    FacetDimension.Person -> "person",
    FacetDimension.Severity -> SearchQuery.SeverityKey
  )

  /** The permalink parameter for the tag facet, which is not a [[FacetDimension]] — see [[tagFacet]]. */
  val TagKey: String = "tag"

  private val FacetLabels: Map[FacetDimension, String] = Map(
    FacetDimension.Type -> "Event type",
    FacetDimension.Source -> "Source",
    FacetDimension.Device -> "Device",
    FacetDimension.Room -> "Room",
    FacetDimension.Person -> "Person",
    FacetDimension.Severity -> "Severity"
  )

  /** Filter parameters that get a chip, and the label the chip shows. `q`, `from` and `until` are absent on purpose:
    * they have their own inputs in the filter bar, and a chip duplicating a visible text field is noise.
    */
  private val ChipLabels: Vector[(String, String)] = Vector(
    "type" -> "type",
    "source" -> "source",
    "device" -> "device",
    "room" -> "room",
    "person" -> "person",
    "tag" -> "tag"
  )

  // ---------------------------------------------------------------------------------------------------------------
  // Rows
  // ---------------------------------------------------------------------------------------------------------------

  def row(summary: EventSummary, now: OffsetDateTime): EventRow =
    EventRow(
      detailUrl = Urls.event(summary.occurredAt, summary.eventUid),
      occurredAt = Rfc3339.render(summary.occurredAt),
      occurredAtLabel = Format.absolute(summary.occurredAt),
      relative = Format.relative(summary.occurredAt, now),
      eventUid = summary.eventUid.toString,
      ceId = summary.ceId,
      source = summary.ceSource,
      eventType = summary.ceType,
      subject = Format.orAbsent(summary.ceSubject),
      device = Format.orAbsent(summary.deviceId),
      room = Format.orAbsent(summary.roomId),
      person = Format.orAbsent(summary.personId),
      severity = summary.severity.filter(_.nonEmpty).getOrElse(UnknownSeverity),
      severityTone = tone(summary.severity),
      metric = Format.metric(summary.metricValue)
    )

  /** Maps a producer-supplied severity string onto one of the eight known tokens.
    *
    * Goes through `Severity.parse` rather than lowercasing the string, so the aliases the database's
    * `events.severity_rank()` folds (`warning`, `crit`, `emerg`, …) colour the same way they rank. Colouring `warning`
    * differently from `warn` would be a UI that disagrees with its own filter.
    */
  def tone(severity: Option[String]): String =
    severity.flatMap(raw => Severity.parse(raw).toOption).fold(UnknownSeverity)(_.label)

  // ---------------------------------------------------------------------------------------------------------------
  // Histogram
  // ---------------------------------------------------------------------------------------------------------------

  /** The timeline strip.
    *
    * Bars are scaled against the tallest bucket, not against a fixed maximum: the question this chart answers is "when
    * did the shape change", and an absolute scale flattens every quiet window into nothing. Each bar links to its own
    * half-open window, which is the only drill-down interaction in the UI that is not a facet.
    */
  def histogram(
    buckets: Vector[HistogramBucket],
    width: FiniteDuration,
    from: OffsetDateTime,
    until: OffsetDateTime,
    query: SearchQuery
  ): Histogram =
    val peak = buckets.map(_.count).maxOption.getOrElse(0L)
    val bars = buckets.map { bucket =>
      val end = bucket.bucket.plusSeconds(width.toSeconds)
      val height = Format.heightPercent(bucket.count, peak)
      Bar(
        start = Rfc3339.render(bucket.bucket),
        startLabel = Format.bucket(bucket.bucket),
        count = bucket.count,
        countLabel = Format.count(bucket.count),
        heightPercent = height,
        heightClass = heightClass(height),
        url = Urls.events(query.within(Rfc3339.render(bucket.bucket), Rfc3339.render(end)))
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

  /** The stylesheet class carrying a bar's height, quantised to 5 % steps.
    *
    * The Content-Security-Policy in `application.conf` omits `'unsafe-inline'` from `style-src`, so a per-bar
    * `style="height:…"` would simply not render. Twenty-one classes cover the range; a bar that has any events at all
    * is never quantised below 5 %, so "one event" and "no events" stay visually distinct.
    */
  def heightClass(percent: Int): String =
    val clamped = math.max(0, math.min(100, percent))
    val quantised = if clamped == 0 then 0 else math.max(5, (clamped + 2) / 5 * 5)
    // Prefixed: a bare `h-5` would collide with Tailwind's own height utility, which means 1.25rem and not 5 %.
    s"bar-h-$quantised"

  // ---------------------------------------------------------------------------------------------------------------
  // Facets
  // ---------------------------------------------------------------------------------------------------------------

  /** The facet panel.
    *
    * Every dimension is emitted even when it has no values, so the panel does not reflow as the filter narrows — a
    * control that moves under the pointer between clicks is a control that gets misclicked.
    *
    * Severity is the one dimension whose link is not a toggle: the grammar has only `SeverityAtLeast` (ADR §6.1), so
    * selecting `warn` means `severity=>=warn` and selecting it again clears it. Modelling it as a set membership would
    * produce a URL the filter codec cannot express.
    */
  def facets(source: Facets, query: SearchQuery): Vector[Facet] =
    FacetKeys.map { (dimension, key) =>
      val selected = query.raw.collect { case (k, v) if k == key => v }.toSet
      val entries = source.dimensions.getOrElse(dimension, Vector.empty).map { value =>
        val isSelected =
          if dimension == FacetDimension.Severity then selected.exists(_.stripPrefix(">=") == value.value)
          else selected.contains(value.value)
        FacetEntry(
          value = value.value,
          label = value.value,
          // Above the candidate cap every count is a lower bound, so it is rendered with a `+` and never as a
          // number — ADR §6.3 makes that honesty a product decision, not an implementation detail.
          count = if source.capped then s"${Format.count(value.count)}+" else Format.count(value.count),
          selected = isSelected,
          url = Urls.events(facetLink(query, dimension, key, value.value, isSelected))
        )
      }
      Facet(key = key, label = FacetLabels.getOrElse(dimension, key), entries = entries)
    } :+ tagFacet(source, query)

  /** Tags are a facet, but not a *dimension*: they live in a `text[]` column and the repository counts them with a
    * separate `unnest` pass (ADR §6.3), so they arrive alongside the grouping-sets result rather than inside it. They
    * are rendered last because containment (`tags @> ?`) narrows harder than any single-value dimension.
    */
  private def tagFacet(source: Facets, query: SearchQuery): Facet =
    val selected = query.raw.collect { case (TagKey, value) => value }.toSet
    Facet(
      key = TagKey,
      label = "Tag",
      entries = source.tags.map { value =>
        FacetEntry(
          value = value.value,
          label = value.value,
          count = if source.capped then s"${Format.count(value.count)}+" else Format.count(value.count),
          selected = selected.contains(value.value),
          url = Urls.events(query.toggled(TagKey, value.value))
        )
      }
    )

  private def facetLink(
    query: SearchQuery,
    dimension: FacetDimension,
    key: String,
    value: String,
    selected: Boolean
  ): String =
    if dimension != FacetDimension.Severity then query.toggled(key, value)
    else if selected then query.link(Query.remove(_, key))
    else query.link(Query.set(_, key, s">=$value"))

  // ---------------------------------------------------------------------------------------------------------------
  // Filter bar
  // ---------------------------------------------------------------------------------------------------------------

  /** The filter bar, built from the *raw* query pairs rather than from the parsed AST.
    *
    * That is what lets a broken permalink render: `?from=yesterday` has no AST, but the user still has to see
    * `yesterday` in the From box with the error attached to it. Re-rendering from the AST would blank the field and
    * leave them guessing which parameter the message referred to.
    */
  def filterBar(query: SearchQuery, errors: Vector[FilterError]): FilterBar =
    val bound = Set(
      SearchQuery.VersionKey,
      SearchQuery.TextKey,
      SearchQuery.FromKey,
      SearchQuery.UntilKey,
      SearchQuery.SeverityKey
    )
    def first(key: String): String = query.raw.collectFirst { case (k, v) if k == key => v }.getOrElse("")

    FilterBar(
      action = Urls.Events,
      version = FilterQuery.Version,
      text = first(SearchQuery.TextKey),
      from = first(SearchQuery.FromKey),
      until = first(SearchQuery.UntilKey),
      severity = first(SearchQuery.SeverityKey).stripPrefix(">="),
      sort = SearchQuery.sortLabel(query.sort),
      limit = query.limit.toString,
      // Everything the bar has no input for travels as a hidden field, or submitting the form would silently drop
      // every facet the user has clicked.
      hidden = query.raw.filterNot((key, _) => bound(key)),
      chips = chips(query),
      problems = problems(errors),
      permalink = Urls.events(query.permalink),
      clearUrl = Urls.Events
    )

  def chips(query: SearchQuery): Vector[Chip] =
    ChipLabels.flatMap { (key, label) =>
      query.raw.collect { case (k, v) if k == key => Chip(label, v, Urls.events(query.without(k, v))) }
    }

  /** Errors, positioned on the parameter that caused them.
    *
    * The parameter name is carried separately from the message so the filter bar can attach `aria-describedby` to the
    * exact input, which is the difference between a screen-reader user hearing "invalid" and hearing which box to fix.
    */
  def problems(errors: Vector[FilterError]): Vector[Problem] = errors.map { error =>
    val parameter = error match
      case FilterError.Invalid(name, _)       => name
      case FilterError.Repeated(name)         => name
      case FilterError.UnknownParameter(name) => name
      case FilterError.Malformed(fragment, _) => fragment
      case FilterError.MissingVersion         => SearchQuery.VersionKey
      case FilterError.UnsupportedVersion(_)  => SearchQuery.VersionKey
      case FilterError.NotPermalinkable(_)    => ""
    Problem(parameter, error.message)
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Results
  // ---------------------------------------------------------------------------------------------------------------

  def results(outcome: SearchOutcome, query: SearchQuery, now: OffsetDateTime): Results =
    val rows = outcome.page.rows.map(row(_, now))
    val total = Format.boundedCount(outcome.total, SearchService.TotalCap.toLong)
    Results(
      rows = rows,
      histogram = histogram(outcome.histogram, outcome.bucketWidth, outcome.window.from, outcome.window.until, query),
      facets = facets(outcome.facets, query),
      totalLabel = total,
      totalIsExact = outcome.totalIsExact,
      facetsCapped = outcome.facets.capped,
      statusLabel =
        if rows.isEmpty then "No events match this search"
        else s"Showing ${Format.count(rows.size.toLong)} of $total events",
      moreUrl = outcome.page.nextCursor.map(cursor => Urls.events(query.continuation(cursor))),
      emptiness = Option.when(rows.isEmpty)(emptiness(query, outcome)),
      sortUrl = Urls.events(flippedSort(query)),
      sortAria = if query.sort == SortDirection.Newest then "descending" else "ascending",
      sortLabel = if query.sort == SortDirection.Newest then "Sort oldest first" else "Sort newest first",
      order = SearchQuery.sortLabel(query.sort)
    )

  /** The same search with the time order reversed.
    *
    * Derived by re-parsing the permalink rather than by editing `SearchQuery.raw`, because `sort` is a *control* and
    * lives outside the filter half of the query string — `SearchQuery.link` would re-append the current sort and undo
    * the flip. Sorting is the one dimension of this UI that is not a filter, and this is where that shows.
    */
  private def flippedSort(query: SearchQuery): String =
    val flipped = query.sort match
      case SortDirection.Newest => SortDirection.Oldest
      case SortDirection.Oldest => SortDirection.Newest
    Query.render(Query.set(Query.parse(query.permalink), SearchQuery.SortKey, SearchQuery.sortLabel(flipped)))

  /** The empty state, phrased against what was actually searched.
    *
    * The three cases are genuinely different problems and deserve different words: nothing has ever been ingested; the
    * window is too narrow; the filter is too narrow. A single "no results" message makes the first case — which is an
    * operational incident — look like the third.
    */
  def emptiness(query: SearchQuery, outcome: SearchOutcome): Emptiness =
    val hasFilter = query.raw.exists((key, _) => key != SearchQuery.VersionKey)
    val everything = Suggestion("Clear all filters", Urls.Events)
    val lastWeek = Suggestion(
      "Widen to the last 7 days",
      Urls.events(
        query.within(Rfc3339.render(outcome.window.until.minusDays(7)), Rfc3339.render(outcome.window.until))
      )
    )
    if !hasFilter then
      Emptiness(
        headline = "No events in the last 24 hours",
        explanation =
          "Nothing has been ingested in the default window. If devices are publishing, check that cobalt is " +
            "consuming and that its consumer-group lag is not growing.",
        suggestions = Vector(lastWeek)
      )
    else
      Emptiness(
        headline = "No events match this search",
        explanation =
          "Every filter below is applied at once. Remove the narrowest one — usually a device or a tag — or widen " +
            "the time window, then try again.",
        suggestions = Vector(everything, lastWeek)
      )

  // ---------------------------------------------------------------------------------------------------------------
  // Detail
  // ---------------------------------------------------------------------------------------------------------------

  /** The detail view: the decoded observation *and* the raw CloudEvent, never one without the other.
    *
    * The decode is lossy by design (ADR §4.2) — `Unrecognised` keeps the payload but not the attributes — so showing
    * only the refined form would hide exactly the information someone opens this page to find. The raw JSON is the
    * authority; the decoded panel is the convenience.
    */
  def detail(detail: EventDetail, now: OffsetDateTime, backUrl: String): Detail =
    val envelope = Envelope.decoder.decodeJson(detail.raw).toOption
    Detail(
      row = row(detail.summary, now),
      attributes = attributes(detail),
      extensions = envelope.toVector.flatMap(
        _.extensions.toVector.sortBy((name, _) => name).map((name, value) => Field(name, value.toJson.noSpaces))
      ),
      observation = envelope.fold(undecodable)(e => observation(Observation.from(e))),
      raw = detail.raw.spaces2,
      backUrl = backUrl
    )

  private def attributes(detail: EventDetail): Vector[Field] =
    val summary = detail.summary
    Vector(
      Field("id", summary.ceId),
      Field("source", summary.ceSource),
      Field("type", summary.ceType),
      Field("subject", Format.orAbsent(summary.ceSubject)),
      Field("time", Format.absolute(summary.occurredAt)),
      Field("ingested", Format.absolute(summary.ingestedAt)),
      Field("dataschema", stringAttribute(detail.raw, "dataschema")),
      Field("datacontenttype", stringAttribute(detail.raw, "datacontenttype")),
      Field("event_uid", summary.eventUid.toString)
    )

  private def stringAttribute(raw: Json, name: String): String =
    Format.orAbsent(raw.hcursor.get[String](name).toOption)

  private val undecodable: ObservationView =
    ObservationView(
      kind = "Undecodable",
      fields = Vector.empty,
      unrecognised = true,
      reason = "the stored JSON is not a CloudEvent this build can parse; the raw payload below is authoritative"
    )

  def observation(value: Observation): ObservationView = value match
    case Observation.Telemetry(device, metric, reading, unit) =>
      ObservationView(
        "Telemetry",
        Vector(
          Field("device", device),
          Field("metric", metric),
          Field("value", Format.metric(Some(reading))),
          Field("unit", unit)
        ),
        unrecognised = false,
        reason = ""
      )
    case Observation.StateChanged(device, from, to) =>
      ObservationView(
        "State changed",
        Vector(Field("device", device), Field("from", from), Field("to", to)),
        unrecognised = false,
        reason = ""
      )
    case Observation.Alarm(device, severity, message) =>
      ObservationView(
        "Alarm",
        Vector(Field("device", device), Field("severity", severity.toString), Field("message", message)),
        unrecognised = false,
        reason = ""
      )
    case Observation.Unrecognised(eventType, payload, reason) =>
      ObservationView(
        "Unrecognised",
        Vector(Field("type", eventType), Field("payload", payloadKind(payload))),
        unrecognised = true,
        reason = reason.getOrElse("no decoder is registered for this event type and schema major")
      )

  private def payloadKind(payload: Payload): String = payload match
    case Payload.Structured(_)        => "JSON"
    case Payload.Opaque(_, mediaType) => mediaType
    case Payload.Empty                => "none"

  // ---------------------------------------------------------------------------------------------------------------
  // Failure
  // ---------------------------------------------------------------------------------------------------------------

  /** The permalink-rejected state: 400, every offending parameter named, and a way back to a search that works. */
  def badQuery(errors: Vector[FilterError]): Failure =
    Failure(
      status = 400,
      title = "That search link could not be read",
      message =
        "The parameters below were rejected. Nothing was searched — showing partial results for a link this " +
          "service could not fully understand would be worse than showing none.",
      problems = problems(errors),
      suggestions = Vector(Suggestion("Start a new search", Urls.Events))
    )

  /** The not-found state for a detail URL whose event has aged out of its partition or never existed. */
  def notFound(backUrl: String): Failure =
    Failure(
      status = 404,
      title = "No such event",
      message =
        "This event is not in the store. Retention detaches whole partitions (ADR §5), so an event older than the " +
          "retention window disappears in one step rather than gradually.",
      problems = Vector.empty,
      suggestions = Vector(Suggestion("Back to events", backUrl))
    )

  /** The rejected-request state: a cursor that does not belong to this filter, or a limit outside its bounds. */
  def rejected(reason: String, backUrl: String): Failure =
    Failure(
      status = 400,
      title = "That request could not be served",
      message = reason,
      problems = Vector.empty,
      suggestions = Vector(Suggestion("Start from the first page", backUrl))
    )
