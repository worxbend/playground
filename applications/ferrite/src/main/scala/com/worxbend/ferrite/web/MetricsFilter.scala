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

package com.worxbend.ferrite.web

import com.worxbend.observability.Meters
import com.worxbend.observability.Telemetry
import io.micrometer.core.instrument.Timer
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit
import play.api.mvc.EssentialAction
import play.api.mvc.EssentialFilter
import play.api.mvc.RequestHeader
import scala.concurrent.ExecutionContext

/** Times every request into the shared Micrometer registry.
  *
  * **Exactly one component times each request, and it is always Micrometer** (ADR §7.1). That is the rule that makes
  * `http.server.requests` carry identical names and tags in ferrite, cobalt and wolfram, so one Grafana dashboard works
  * for all three. A Play-native or framework-supplied timer would fork the metric family for this service alone.
  *
  * **`uri` is a route template, never a raw path.** ferrite serves server-rendered search over millions of events; one
  * timeseries per permalink is, in the ADR's words, the single most likely way to take down Prometheus in this system.
  * [[RouteTemplate]] collapses every path to one of a fixed handful of shapes, and `Telemetry`'s `maximumAllowableTags`
  * filter is the backstop for the day someone adds a route and forgets.
  *
  * **`/metrics` and the `/health` subtree are not timed.** A scrape endpoint that times itself adds a request per
  * scrape interval to every rate panel that reads it, which is a self-fulfilling traffic graph.
  */
@Singleton
final class MetricsFilter @Inject() (telemetry: Telemetry)(using ec: ExecutionContext) extends EssentialFilter:

  def apply(next: EssentialAction): EssentialAction = EssentialAction { request =>
    if RouteTemplate.uninstrumented(request) then next(request)
    else
      val started = System.nanoTime()
      next(request).map { result =>
        Timer
          .builder(Meters.HttpServerRequests)
          .tag("method", request.method)
          .tag(Meters.TagKeys.Uri, RouteTemplate.of(request))
          .tag("status", result.header.status.toString)
          .tag(
            Meters.TagKeys.Outcome,
            if result.header.status < 400 then Meters.Outcomes.Success else Meters.Outcomes.Failure
          )
          .register(telemetry.registry)
          .record(System.nanoTime() - started, TimeUnit.NANOSECONDS)
        result
      }
  }

/** Path to bounded route template.
  *
  * There is no `conf/routes` file (ADR §8.1), so Play never attaches a `HandlerDef` and there is nothing to read a
  * matched route off. Deriving the template here keeps the tag cardinality bounded by *this list* rather than by user
  * input, which is stronger than reading a template from a router would have been: a new route that nobody adds a case
  * for degrades to `other` instead of leaking a raw path.
  */
object RouteTemplate:

  val Other: String = "other"

  /** Paths excluded from `http.server.requests` entirely (ADR §7.1). */
  def uninstrumented(request: RequestHeader): Boolean =
    Meters.UninstrumentedPaths.exists(prefix => request.path == prefix || request.path.startsWith(s"$prefix/"))

  def of(request: RequestHeader): String = request.path match
    case Urls.Root   => Urls.Root
    case Urls.Events => Urls.Events
    // The live tail gets a template of its own rather than being excluded. `MetricsFilter` records when the `Result`
    // is produced, which for a chunked response is when the headers are ready — so a tail open for an hour is one
    // short observation at connect, not an hour-long outlier, and this series is then the only place that says how
    // many tails were opened and how many were refused with a 503.
    case Urls.Live                                         => Urls.Live
    case path if path.startsWith(s"${Urls.Events}/")       => s"${Urls.Events}/{eventUid}"
    case path if path.startsWith(s"${Urls.AssetsPrefix}/") => s"${Urls.AssetsPrefix}/*file"
    case _                                                 => Other
