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

import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.repository.EventDetail
import com.worxbend.persistence.repository.EventRef
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.FacetRequest
import com.worxbend.persistence.repository.Facets
import com.worxbend.persistence.repository.HistogramBucket
import com.worxbend.persistence.repository.HistogramRequest
import com.worxbend.persistence.repository.SearchPage
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.OffsetDateTime
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** Everything the results region needs, fetched once.
  *
  * A single value rather than four independent calls from the controller, because the four are one logical answer: the
  * histogram, the facets and the total all describe *this* page's filter, and a controller that assembled them
  * separately could render a histogram for one filter beside counts for another the moment a call site forgot one.
  *
  * @param total
  *   bounded by [[SearchService.TotalCap]]; equal to `cap + 1` when there are more. See [[totalIsExact]].
  */
final case class SearchOutcome(
  page: SearchPage,
  facets: Facets,
  histogram: Vector[HistogramBucket],
  total: Long,
  window: TimeWindow,
  bucketWidth: FiniteDuration
):

  /** False when the count hit its cap, in which case the UI must render "10,000+" and not a number (ADR §6.3). */
  def totalIsExact: Boolean = total <= SearchService.TotalCap

/** The half-open window the histogram covers. */
final case class TimeWindow(from: OffsetDateTime, until: OffsetDateTime)

/** The application service over [[EventRepository]].
  *
  * **All four queries are issued before any of them is awaited.** They are independent, they run on a pool of eight
  * connections, and the alternative — a `for` comprehension over `Future`s, which sequences them — turns one 40 ms page
  * into four serial round trips. Starting them as `val`s is not a style choice here; `for` over `Future` is sequential
  * and that is the classic way to make a fast page slow without anyone noticing.
  *
  * **Blocking JDBC never touches Play's default dispatcher.** The repository this class is given was constructed with a
  * [[SearchExecutionContext]] — a `CustomExecutionContext` whose fixed pool size equals the read pool's
  * `maximumPoolSize` — and this service composes on the same context. See that class for the full reasoning; the short
  * form is ADR §0 decision 8: a bounded executor is a place you can shed load from, and a virtual-thread executor moves
  * the queue somewhere you cannot.
  *
  * @param clock
  *   injected rather than `OffsetDateTime.now()` so the default time window is deterministic in tests. A default window
  *   that depends on wall-clock time makes every assertion about the histogram flaky.
  */
@Singleton
final class SearchService @Inject() (repository: EventRepository, metrics: SearchMetrics, clock: Clock)(using
  ec: SearchExecutionContext
):

  import SearchService.*

  /** One page plus its facets, histogram and bounded total. */
  def search(query: SearchQuery): Future[Either[String, SearchOutcome]] =
    query.toRequest match
      case Left(reason)   => Future.successful(Left(reason))
      case Right(request) =>
        val window = windowOf(query.filter)
        val facetRequest = FacetRequest.of(query.filter)
        val histogramRequest = HistogramRequest.of(query.filter, window.from, window.until)

        // The clock starts here rather than at the top of the method: everything above is pure and everything below
        // waits on the database, and a timer that included the parse would flatter the number that matters.
        val startedAt = System.nanoTime()
        val shape = SearchShape.of(query.filter)

        val pageF = repository.search(request)
        val facetsF = facetRequest.fold(_ => Future.successful(EmptyFacets), repository.facets)
        val histogramF = histogramRequest.fold(_ => Future.successful(Vector.empty), repository.histogram)
        val totalF = repository.countAtMost(query.filter, TotalCap)
        val width = histogramRequest.map(_.width).getOrElse(DefaultBucketWidth)

        val outcome =
          for
            page <- pageF
            facets <- facetsF
            histogram <- histogramF
            total <- totalF
          yield SearchOutcome(page, facets, histogram, total, window, width)

        // `andThen` and not `map`: the timer must record the failed search too. A search that times out is the slowest
        // search there is, and dropping it would make the p99 improve as the service got worse.
        outcome.andThen { case attempt =>
          metrics.searched(shape, System.nanoTime() - startedAt)
          attempt.foreach(result => if result.facets.capped then metrics.facetsCapped())
        }.map(Right.apply)

  /** A continuation page only. No facets, no histogram, no count: the filter has not changed, so neither have they, and
    * re-running them on every scroll would triple the cost of paging for markup that is thrown away.
    */
  def page(query: SearchQuery): Future[Either[String, SearchPage]] =
    query.toRequest match
      case Left(reason)   => Future.successful(Left(reason))
      case Right(request) => repository.search(request).map(Right.apply)

  /** One event with its payload. */
  def detail(ref: EventRef): Future[Option[EventDetail]] = repository.find(ref)

  /** The histogram window: the filter's own time bounds where it has them, otherwise the last [[DefaultWindow]].
    *
    * An unbounded filter has no natural window, and charting "all of history" would produce month-wide buckets in which
    * nothing is visible. Defaulting to a recent window is the same choice Kibana makes and, unlike an unbounded chart,
    * it also prunes partitions.
    *
    * The bounds are re-checked rather than trusted: `Filter.occurred` already rejects an inverted range, but a filter
    * could reach here with only one bound set and a `from` in the future, and `HistogramRequest.of` would then refuse
    * the whole chart. Falling back keeps the page rendering.
    */
  def windowOf(filter: Option[Filter]): TimeWindow =
    val now = OffsetDateTime.now(clock)
    val bounds = filter.toVector.flatMap(Filter.leaves).collectFirst { case Filter.Occurred(from, until) =>
      (from, until)
    }
    val (from, until) = bounds.getOrElse((None, None))
    val start = from.getOrElse(until.getOrElse(now).minusSeconds(DefaultWindow.toSeconds))
    val end = until.getOrElse(if start.isAfter(now) then start.plusSeconds(DefaultWindow.toSeconds) else now)
    if start.isBefore(end) then TimeWindow(start, end)
    else TimeWindow(end.minusSeconds(DefaultWindow.toSeconds), end)

object SearchService:

  import scala.concurrent.duration.DurationInt

  /** ADR §6.3: result totals are never `COUNT(*)`. At the cap the UI renders "10,000+", which is what Kibana and GitHub
    * ship and is honest — an exact total over a partitioned fact table is a full scan nobody reads the end of.
    */
  val TotalCap: Int = 10000

  /** The window a filter without time bounds gets. A day is the span an event observatory is normally opened to look
    * at, and it keeps the default landing query inside one or two monthly partitions.
    */
  val DefaultWindow: FiniteDuration = 24.hours

  /** Only used when the histogram request itself could not be built, so the value is cosmetic. */
  val DefaultBucketWidth: FiniteDuration = 15.minutes

  /** What the facet panel shows when the facet query could not be built. Empty rather than absent: the panel still
    * renders its headings, so the page does not change shape depending on whether a sub-query succeeded.
    */
  val EmptyFacets: Facets = Facets(Map.empty, Vector.empty, 0L, capped = false)
