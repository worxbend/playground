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

import com.worxbend.kernel.Rfc3339
import java.time.OffsetDateTime
import java.util.UUID

/** Every URL this application can produce, in one place.
  *
  * There is no `conf/routes` file (ADR §8.1), so there is no reverse router either. That is a deliberate trade: SIRD
  * keeps routing as ordinary, testable Scala, and the price is that nothing generates URLs for the templates. Paying it
  * with string interpolation scattered through Twirl would be the worst of both worlds — a typo in a link is invisible
  * until someone clicks it — so the builders live here, are the *only* sanctioned way to build a path, and are checked
  * against the routers by `UrlRoutingSuite`. If a builder and its route ever disagree, that suite fails at compile-test
  * time rather than in a browser.
  *
  * Paths only. Query strings are composed by [[Query]] and appended by [[eventsQuery]], because a permalink is data the
  * user can edit (ADR §6.3) and must not be assembled by concatenation at each call site.
  */
object Urls:

  /** Landing page. Redirects to [[Events]] rather than duplicating it, so there is one canonical list URL to share. */
  val Root: String = "/"

  val Events: String = "/events"

  val Metrics: String = "/metrics"

  val HealthLive: String = "/health/live"

  val HealthReady: String = "/health/ready"

  val AssetsPrefix: String = "/assets"

  /** The query parameter carrying an event's `occurred_at`.
    *
    * The detail route is `/events/{event_uid}?at={rfc3339}` and not `/events/{at}/{uid}`, because `occurred_at` is a
    * partition key that has to be handed to the repository verbatim and RFC 3339 is full of characters (`:`, `+`) that
    * a path segment has to percent-encode and every intermediary is then free to re-normalise. As a query parameter it
    * survives proxies, `curl`, and a user editing the link.
    */
  val AtParam: String = "at"

  /** The list page, with an already-rendered query string (no leading `?`). */
  def events(query: String): String = if query.isEmpty then Events else s"$Events?$query"

  /** The detail page for one event.
    *
    * Takes the two halves of the primary key rather than a repository type: this object is presentation, and letting a
    * `EventRef` in here would put a persistence type on the path from a Twirl template to the router.
    */
  def event(occurredAt: OffsetDateTime, eventUid: UUID): String =
    s"$Events/$eventUid?${Query.render(Vector(AtParam -> Rfc3339.render(occurredAt)))}"

  /** A static asset, cache-busted by the build version.
    *
    * `?v=` rather than a content hash in the filename: ADR §8.3 rejects `sbt-digest_sbt2_3` (a milestone artifact) and
    * a version query is enough for an application whose assets change only with a deployment.
    */
  def asset(path: String): String =
    s"$AssetsPrefix/${path.stripPrefix("/")}?${Query.render(Vector("v" -> BuildVersion.value))}"

/** The version stamped onto asset URLs.
  *
  * Read from the jar manifest so it is the real artifact version in a packaged run, and falls back to a constant in a
  * `sbt run` or a test, where there is no manifest and cache-busting is meaningless anyway.
  */
object BuildVersion:

  val Development: String = "dev"

  val value: String =
    Option(getClass.getPackage)
      .flatMap(pkg => Option(pkg.getImplementationVersion))
      .orElse(sys.env.get("SERVICE_VERSION"))
      .getOrElse(Development)
