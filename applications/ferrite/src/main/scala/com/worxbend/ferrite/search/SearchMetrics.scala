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
import com.worxbend.observability.Meters
import com.worxbend.observability.Telemetry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit

/** The bounded `shape` tag on [[Meters.SearchQueryDuration]].
  *
  * **Why not "which filter families were used".** That is the obvious reading of "shape", and it is a cardinality trap:
  * [[Filter]] has fourteen leaves, so the set of families present has 2^14 values. Nothing in the UI would produce most
  * of them, which is exactly why the mistake survives review — it is bounded in practice and unbounded in principle,
  * and the day someone builds a filter programmatically it stops being bounded in practice too.
  *
  * **What this splits on instead: the four decisions that change the query plan.** ADR §5's schema gives each a
  * different access path, so a slow bucket names the index to look at rather than a filter to guess about:
  *   - `time` — a bounded `occurred_at`, so partition pruning and the BRIN index apply. Its *absence* is usually the
  *     answer when a search is slow.
  *   - `text` — free text, which is the trigram GIN index and the most expensive single decision a query can make.
  *   - `payload` — a `jsonb` containment or numeric comparison, `jsonb_path_ops` GIN.
  *   - `attrs` — everything else: type, source, device, room, person, severity, tags, extensions. All btree or GIN
  *     equality, all cheap, and not worth telling apart from each other in a latency panel.
  *
  * Sixteen possible values, plus `none`. Closed by construction rather than by a filter in [[Telemetry]], which is the
  * stronger guarantee: a cap drops values silently once it engages, whereas this cannot mint a seventeenth.
  */
object SearchShape:

  /** The filter-free landing query. Named `Unfiltered` rather than `None` so it cannot shadow `scala.None` inside this
    * object, and a distinct tag value rather than an empty string because "no filter" is the single most-run query on
    * the service — its latency is the closest thing this system has to a baseline.
    */
  val Unfiltered: String = "none"

  val Time: String = "time"
  val Text: String = "text"
  val Payload: String = "payload"
  val Attrs: String = "attrs"

  /** Tokens in a fixed order, so `time+text` and `text+time` cannot both exist. */
  private val Order: Vector[String] = Vector(Time, Text, Payload, Attrs)

  /** Every value [[of]] can return. Enumerated so a panel author knows the whole set up front and a property test can
    * assert no input escapes it.
    */
  val all: Set[String] =
    val subsets = (0 until (1 << Order.size)).map { mask =>
      Order.zipWithIndex.collect { case (token, index) if (mask & (1 << index)) != 0 => token }.mkString("+")
    }
    subsets.toSet.map(shape => if shape.isEmpty then Unfiltered else shape)

  /** The shape of a filter.
    *
    * Total by construction: `Filter.leaves` flattens `And` only, so a nested `Or` or `Not` arrives here whole and falls
    * into `attrs`. That is the right answer rather than a gap — a disjunction's cost is dominated by whichever branch
    * is most expensive, and this tag is not fine-grained enough to say which.
    */
  def of(filter: Option[Filter]): String =
    val leaves = filter.toVector.flatMap(Filter.leaves)
    val tokens = Order.filter(token => leaves.exists(matches(token, _)))
    if tokens.isEmpty then Unfiltered else tokens.mkString("+")

  private def matches(token: String, leaf: Filter): Boolean = (token, leaf) match
    case (Time, _: Filter.Occurred)                                                                   => true
    case (Text, _: Filter.FullText)                                                                   => true
    case (Payload, _: (Filter.PayloadContains | Filter.PayloadCmp))                                   => true
    case (Attrs, _: (Filter.Occurred | Filter.FullText | Filter.PayloadContains | Filter.PayloadCmp)) => false
    case (Attrs, _)                                                                                   => true
    case _                                                                                            => false

/** ferrite's search metrics, in the shared vocabulary of `modules/observability`.
  *
  * A separate class from [[SearchService]] so that the service's four-queries-in-parallel logic and its instrumentation
  * can be read — and tested — apart.
  *
  * **The registry, not [[Telemetry]], is the primary constructor's parameter.** Instrumentation needs somewhere to
  * register meters and nothing else; taking the whole composition root would mean a unit test of the search timer had
  * to start JVM binders and a JFR stream. The `@Inject` secondary constructor is what Guice sees, so production still
  * gets the one registry the process owns and no binding for `MeterRegistry` has to exist.
  *
  * **`-Werror` trap (ADR §7.4).** `MeterRegistry#counter` and `#timer` return the meter, so every call below is part of
  * an expression that ends in a `Unit`-returning `increment`/`record`.
  */
@Singleton
final class SearchMetrics(registry: MeterRegistry):

  @Inject()
  def this(telemetry: Telemetry) = this(telemetry.registry)

  /** One completed search, tagged by [[SearchShape]]. Recorded for failures too: a search that times out is the slowest
    * search there is, and excluding it would make the p99 improve as the service got worse.
    */
  def searched(shape: String, elapsedNanos: Long): Unit =
    registry
      .timer(Meters.SearchQueryDuration, Tags.of(Meters.TagKeys.Shape, shape))
      .record(elapsedNanos, TimeUnit.NANOSECONDS)

  /** The facet counts for this search hit the candidate cap and are therefore lower bounds.
    *
    * Untagged, deliberately. This is a product signal — "the cap is now hiding information users need" — not a
    * diagnostic, and the rate is the whole reading; splitting it by shape would invite the reader to tune the cap per
    * query type, which is not a knob that exists.
    */
  def facetsCapped(): Unit =
    registry.counter(Meters.SearchFacetsCapped).increment()
