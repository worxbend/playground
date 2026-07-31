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
  def link(edit: Vector[(String, String)] => Vector[(String, String)]): String =
    val edited = edit(raw)
    val versioned = if edited.isEmpty then edited else Query.set(edited, SearchQuery.VersionKey, FilterQuery.Version)
    Query.render(controls(versioned))

  /** The canonical, shareable form of "what I am looking at". */
  def permalink: String = link(identity)

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

  /** Parameters this layer consumes. Everything else belongs to the kernel's filter codec. */
  val ControlKeys: Set[String] = Set(LimitKey, CursorKey, SortKey)

  val DefaultSort: SortDirection = SortDirection.Newest

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

  /** Parses a raw query string (with or without a leading `?`).
    *
    * An **empty filter half means no filter, not an error.** `FilterQuery` requires an explicit `v=1` precisely so a
    * future grammar change is detectable, but the landing page `/events` legitimately carries no filter at all and must
    * not be greeted with "missing 'v' parameter". The filter bar therefore renders `v=1` as a hidden input, so every
    * query the UI itself produces is versioned, and only a truly empty query takes the shortcut below.
    */
  def parse(rawQueryString: String): Either[Vector[FilterError], SearchQuery] =
    val pairs = Query.parse(rawQueryString)
    val (control, filterPairs) = pairs.partition((key, _) => ControlKeys(key))

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

  /** The same query string with every failure ignored: no filter, default controls, but the raw pairs preserved.
    *
    * Exists for exactly one caller — the error path. ADR §6.3 requires a rejected permalink to come back *in the filter
    * bar*, with each bad value still visible in the input that produced it, rather than as a bare error page that
    * leaves the user holding a URL they cannot see or edit. That needs a `SearchQuery` even though there is no valid
    * query, and this is it. It is never used to run a search: `filter` is `None` here because the parse failed, not
    * because the user asked for everything, and running it would answer a question nobody posed.
    */
  def lenient(rawQueryString: String): SearchQuery =
    val (_, filterPairs) = Query.parse(rawQueryString).partition((key, _) => ControlKeys(key))
    SearchQuery(None, DefaultSort, SearchRequest.DefaultLimit, None, filterPairs)
