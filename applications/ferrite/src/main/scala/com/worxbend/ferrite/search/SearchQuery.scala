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

package com.worxbend.ferrite.search

import com.worxbend.ferrite.web.Query
import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.FilterError
import com.worxbend.kernel.search.FilterQuery
import com.worxbend.kernel.search.Severity
import com.worxbend.persistence.repository.SearchRequest
import com.worxbend.persistence.search.SortDirection
import scala.util.Try

/** A parsed `/events` query string: the domain filter plus the three presentation controls.
  *
  * The split is the point. `FilterQuery` in `modules/kernel` owns the *filter* grammar and would report `limit`,
  * `cursor` and `sort` as unknown parameters, because from the domain's point of view they are — they say nothing about
  * which events match, only about how many of them to draw and where to resume. So the raw query is partitioned here
  * first: control parameters are peeled off, everything else is handed to the kernel codec verbatim.
  *
  * Parsing is **total** (ADR §6.3). A malformed permalink produces a `Vector[FilterError]` that the filter bar renders
  * next to the offending value; it never 500s, and — more importantly — it never silently drops a parameter and shows
  * results the URL did not ask for. A user who cannot see that their filter was ignored will trust the wrong numbers.
  *
  * @param raw
  *   the filter half of the query string, decoded into pairs. Kept so the facet panel and the chips can edit *this*
  *   query rather than re-render one from the AST — an AST cannot represent the invalid value the user needs to see in
  *   order to fix it.
  */
final case class SearchQuery(
  filter: Option[Filter],
  sort: SortDirection,
  limit: Int,
  cursor: Option[String],
  raw: Vector[(String, String)]
):

  /** **Every parameter this search is spelled as**, in the order a permalink renders them: the filter half with the
    * grammar version re-stamped, then each control whose value is not the default.
    *
    * The one list, and the reason this type exists rather than a bag of strings. [[permalink]] renders it and the
    * filter bar hides whatever it has no input for, so the URL a user copies and the fields a form submits are the same
    * set of pairs by construction. Building the hidden fields from [[raw]] instead is what silently reset `sort` and
    * `limit` on every keystroke: `raw` is the *filter* half and never held a control at all.
    */
  def fields: Vector[(String, String)] = fieldsOf(identity)

  /** The one query-string builder. Everything the UI links to is `link` with a different edit.
    *
    * Two invariants it enforces that no call site should have to remember:
    *
    *   - **the version is re-stamped** whenever the result is non-empty, so a link the UI produced always round-trips
    *     through `FilterQuery.decode` even if the URL it was derived from omitted `v`;
    *   - **the cursor is never carried over.** A cursor is a position inside a result set, not part of the query, and
    *     replaying one against an edited filter is exactly the mistake `Fingerprint` exists to catch. Editing the
    *     filter always restarts at page one.
    */
  def link(edit: Vector[(String, String)] => Vector[(String, String)]): String = Query.render(fieldsOf(edit))

  private def fieldsOf(edit: Vector[(String, String)] => Vector[(String, String)]): Vector[(String, String)] =
    val edited = edit(raw)
    val versioned = if edited.isEmpty then edited else Query.set(edited, SearchQuery.VersionKey, FilterQuery.Version)
    controls(versioned)

  /** The canonical, shareable form of "what I am looking at". */
  def permalink: String = Query.render(fields)

  /** The same query resumed from `next`. The cursor is appended after the controls, so it is always last and a
    * hand-truncated URL degrades to page one rather than to a different filter.
    */
  def continuation(next: String): String =
    val base = permalink
    val cursor = Query.render(Vector(SearchQuery.CursorKey -> next))
    if base.isEmpty then cursor else s"$base&$cursor"

  /** This query with one filter value toggled — what a facet click means. */
  def toggled(key: String, value: String): String = link(Query.toggle(_, key, value))

  /** This query with one filter value removed — the chip dismissal. */
  def without(key: String, value: String): String = link(Query.remove(_, key, value))

  /** This query narrowed to a half-open time window — what clicking a histogram bar means. */
  def within(from: String, until: String): String =
    link(pairs => Query.set(Query.set(pairs, SearchQuery.FromKey, from), SearchQuery.UntilKey, until))

  /** The severity floor this search asks for, canonicalised.
    *
    * Read through `Severity.parse` and not off the string, so `>=warning` and `>=warn` are one floor and not two. A
    * facet panel that compared the raw text reported its own link as unselected and could never be toggled off.
    *
    * `None` covers both "no severity parameter" and "a value the grammar cannot express", because nothing that reads a
    * floor can act on the difference. What reports it is the `FilterError` the codec already produced.
    */
  def severityFloor: Option[Severity] =
    raw.collectFirst { case (SearchQuery.SeverityKey, value) => value }.flatMap(SearchQuery.parseSeverity)

  /** This query with the severity floor set. The only place `>=` is spelled outside the kernel codec. */
  def withSeverity(level: Severity): String =
    link(Query.set(_, SearchQuery.SeverityKey, s"${SearchQuery.AtLeast}${level.label}"))

  /** This query with the severity floor cleared — what clicking the selected severity facet means. */
  def withoutSeverity: String = link(Query.remove(_, SearchQuery.SeverityKey))

  /** The search "at least this severity" for a severity string a **producer** chose, or `None` when it is not one.
    *
    * The guard, in the one place both presenters can reach it. `events.severity` is `lower(raw #>> '{data,severity}')`
    * and the rollup stores `coalesce(severity, 'none')`, so both the facet panel and the overview's breakdown offer
    * values the grammar has no `SeverityAtLeast` for. Emitting `>=low` produces a link that 400s, which is worse than a
    * row that is plainly not clickable — and the two presenters each having their own copy of that sentence is how one
    * of them came to be missing it.
    */
  def severityAtLeast(value: String): Option[String] = SearchQuery.parseSeverity(value).map(withSeverity)

  /** Non-default controls re-attached to an edited filter query. Defaults are omitted so a shared URL stays short. */
  private def controls(pairs: Vector[(String, String)]): Vector[(String, String)] =
    val withSort =
      if sort == SearchQuery.DefaultSort then pairs
      else Query.set(pairs, SearchQuery.SortKey, SearchQuery.sortLabel(sort))
    if limit == SearchRequest.DefaultLimit then withSort
    else Query.set(withSort, SearchQuery.LimitKey, limit.toString)

  /** The repository request. Fails only on a cursor that is malformed or was minted for a different filter — the
    * distinction the caller has to make, so it is preserved rather than flattened into `Option`.
    */
  def toRequest: Either[String, SearchRequest] = SearchRequest.of(filter, sort, limit, cursor)

object SearchQuery:

  val LimitKey: String = "limit"
  val CursorKey: String = "cursor"
  val SortKey: String = "sort"

  /** Filter-side parameter names this layer also has to name, because the filter bar binds inputs to them and the
    * histogram narrows on them. They belong to `FilterQuery`'s grammar; duplicating the literals here is the price of
    * that codec keeping its key names private, and `SearchQuerySuite` pins them against a real round trip.
    */
  val VersionKey: String = "v"
  val FromKey: String = "from"
  val UntilKey: String = "until"
  val TextKey: String = "q"
  val SeverityKey: String = "severity"

  /** The two open-ended filter families, whose parameter *names* are chosen by the user: `data.<path>=<op><number>` and
    * `ext.<name>=<value>`. Everything else in the grammar has a fixed key.
    */
  val PathPrefix: String = "data."
  val ExtensionPrefix: String = "ext."

  /** Parameters this layer consumes. Everything else belongs to the kernel's filter codec. */
  val ControlKeys: Set[String] = Set(LimitKey, CursorKey, SortKey)

  val DefaultSort: SortDirection = SortDirection.Newest

  /** How the permalink grammar spells a severity threshold. `SeverityAtLeast` is the only severity predicate there is
    * (ADR §6.1), so a bare label means the same thing — but everything this application *emits* is prefixed, or two
    * URLs would describe one filter and a facet could not recognise its own selection.
    */
  val AtLeast: String = ">="

  /** A severity as a permalink value reads it: the prefix is optional and the aliases the database folds are accepted,
    * exactly as `FilterQuery` does it. Canonical or nothing — the caller gets a `Severity` and cannot re-emit the
    * producer's spelling by accident.
    */
  def parseSeverity(value: String): Option[Severity] =
    Severity.parse(value.stripPrefix(AtLeast).trim).toOption

  private val NewestLabel: String = "newest"
  private val OldestLabel: String = "oldest"

  def sortLabel(sort: SortDirection): String = sort match
    case SortDirection.Newest => NewestLabel
    case SortDirection.Oldest => OldestLabel

  private def parseSort(raw: String): Either[FilterError, SortDirection] = raw.trim.toLowerCase match
    case NewestLabel => Right(SortDirection.Newest)
    case OldestLabel => Right(SortDirection.Oldest)
    case other       => Left(FilterError.Invalid(SortKey, s"'$other' is not a sort order; expected newest or oldest"))

  private def parseLimit(raw: String): Either[FilterError, Int] =
    Try(raw.trim.toInt).toEither.left
      .map(_ => FilterError.Invalid(LimitKey, s"'$raw' is not a whole number"))
      .flatMap { value =>
        if value < 1 then Left(FilterError.Invalid(LimitKey, "limit must be at least 1"))
        else if value > SearchRequest.MaxLimit then
          Left(FilterError.Invalid(LimitKey, s"limit must be at most ${SearchRequest.MaxLimit}"))
        else Right(value)
      }

  /** The parameters a query string actually states, split into the control half and the filter half.
    *
    * **Present-but-empty is absent.** `FilterQuery.decode` says why — a form serialises every named control and spells
    * "untouched" as the empty string — and applies the same rule to the filter half it is handed. It is applied here as
    * well because it governs the *controls*, which never reach that codec, and because [[raw]] is what the filter bar
    * echoes and the permalink renders: a pair dropped on the way in must not reappear on the way out.
    */
  private def stated(rawQueryString: String): (Vector[(String, String)], Vector[(String, String)]) =
    Query.parse(rawQueryString).filterNot((_, value) => value.isEmpty).partition((key, _) => ControlKeys(key))

  /** Parses a raw query string (with or without a leading `?`).
    *
    * An **empty filter half means no filter, not an error.** `FilterQuery` requires an explicit `v=1` precisely so a
    * future grammar change is detectable, but the landing page `/events` legitimately carries no filter at all and must
    * not be greeted with "missing 'v' parameter". The filter bar therefore renders `v=1` as a hidden input, so every
    * query the UI itself produces is versioned, and only a truly empty query takes the shortcut below.
    */
  def parse(rawQueryString: String): Either[Vector[FilterError], SearchQuery] =
    val (control, filterPairs) = stated(rawQueryString)

    def single(key: String): Either[Vector[FilterError], Option[String]] =
      control.collect { case (k, v) if k == key => v } match
        case Vector()      => Right(None)
        case Vector(value) => Right(Some(value))
        case _             => Left(Vector(FilterError.Repeated(key)))

    def control1[A](key: String, default: A)(parse: String => Either[FilterError, A]): Either[Vector[FilterError], A] =
      single(key).flatMap(_.fold[Either[Vector[FilterError], A]](Right(default))(parse(_).left.map(Vector(_))))

    val sort = control1(SortKey, DefaultSort)(parseSort)
    val limit = control1(LimitKey, SearchRequest.DefaultLimit)(parseLimit)
    val cursor = single(CursorKey)
    val filter: Either[Vector[FilterError], Option[Filter]] =
      if filterPairs.isEmpty then Right(None) else FilterQuery.decode(Query.render(filterPairs))

    // Every part is evaluated and every failure reported. Short-circuiting would show the user one broken parameter,
    // they would fix it, and the next reload would show them the next one.
    val errors = Vector(sort, limit, cursor, filter).flatMap {
      case Left(reported) => reported
      case Right(_)       => Vector.empty
    }
    if errors.nonEmpty then Left(errors)
    else
      for
        s <- sort
        l <- limit
        c <- cursor
        f <- filter
      yield SearchQuery(f, s, l, c.filter(_.nonEmpty), filterPairs)

  /** The same query string with every failure ignored: no filter, the controls it could read, the raw pairs preserved.
    *
    * Two callers, and the same reason underneath both: something has to render a search it must not run.
    *
    *   - The **error path**. ADR §6.3 requires a rejected permalink to come back *in the filter bar*, with each bad
    *     value still visible in the input that produced it, rather than as a bare error page that leaves the user
    *     holding a URL they cannot see or edit. That needs a `SearchQuery` even though there is no valid query.
    *   - The **detail page**, which has to reproduce the list its reader came from without re-running or re-validating
    *     it. A filter that 400s the list must not also 400 the event somebody clicked through to.
    *
    * `filter` is `None` because nothing here was validated, not because the user asked for everything, so this value is
    * never used to run a search — it would answer a question nobody posed. A control that *does* parse is kept: `sort`
    * and `limit` are what the bar and the back-link have to carry, and defaulting them silently would drop exactly the
    * parameter this method exists to preserve. `cursor` is deliberately not: a position inside a result set that was
    * never produced is meaningless.
    */
  def lenient(rawQueryString: String): SearchQuery =
    val (control, filterPairs) = stated(rawQueryString)
    def one[A](key: String, default: A)(parse: String => Either[FilterError, A]): A =
      control.collectFirst { case (k, v) if k == key => v }.flatMap(parse(_).toOption).getOrElse(default)
    SearchQuery(
      None,
      one(SortKey, DefaultSort)(parseSort),
      one(LimitKey, SearchRequest.DefaultLimit)(parseLimit),
      None,
      filterPairs
    )

  /** The payload and extension predicates of a rendered filter bar, as dismissible chips.
    *
    * These two families are the only active filters with neither an input of their own nor a facet to click, so without
    * a chip they are invisible: they travel through a form submit as hidden fields, and a user looking at a link
    * somebody sent them has no way to see — let alone remove — the `data.temperature=>21` that is excluding most of the
    * rows. An applied filter the user cannot see is the same defect as a dropped one, pointing the other way.
    *
    * `label` is the parameter name verbatim and `value` the raw text, rather than a prose rendering: the chip is then a
    * legend for the URL, which is the thing the user is actually expected to edit and share. It also keeps the chip
    * honest for a value the codec rejected — `data.temperature=warm` still shows what was typed.
    *
    * Derived from a rendered [[com.worxbend.ferrite.web.view.FilterBar]] rather than from a `SearchQuery`, because the
    * filter-bar fragment is handed the view model and nothing else. `hidden` already carries exactly the pairs with no
    * input, and `permalink` is this search as a URL, so removing one pair from it is the whole of the edit — the same
    * `Query.remove` [[SearchQuery.without]] performs, applied one layer later.
    */
  def predicateChips(hidden: Vector[(String, String)], permalink: String): Vector[PredicateChip] =
    val (path, queryString) = permalink.span(_ != '?')
    val pairs = Query.parse(queryString)
    hidden.collect {
      case (key, value) if key.startsWith(PathPrefix) || key.startsWith(ExtensionPrefix) =>
        val remaining = Query.render(Query.remove(pairs, key, value))
        PredicateChip(key, value, if remaining.isEmpty then path else s"$path?$remaining")
    }

/** One payload or extension predicate in the filter bar.
  *
  * Field-for-field the same shape as [[com.worxbend.ferrite.web.view.Chip]] and deliberately not that type: the chips
  * built in the presenter come from a fixed table of parameter names, and these come from parameters whose names the
  * user invented. Sharing the record would suggest the two are produced the same way.
  */
final case class PredicateChip(label: String, value: String, removeUrl: String)
