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

package com.worxbend.kernel

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import scala.util.Try

/** RFC 3339 rendering, shared by the CloudEvents `time` attribute and the search permalink's time bounds.
  *
  * **`OffsetDateTime.toString` is not RFC 3339.** It follows ISO 8601, which makes the seconds field optional, so
  * `17:31:00Z` renders as `17:31Z` — a string RFC 3339 forbids and that stricter CloudEvents implementations reject.
  * The failure is invisible in a Scala-only round-trip (parsing accepts it back) and only surfaces at the boundary with
  * another vendor's SDK, which is exactly the kind of bug a shared kernel exists to prevent.
  *
  * The fractional second is emitted only when it is non-zero and is trimmed of trailing zeros, which is the one
  * documented normalisation this codec performs: `17:31:00.000Z` comes back as `17:31:00Z`. The instant is unchanged.
  */
object Rfc3339:

  /** The years this codec accepts, and the only ones it can honestly render.
    *
    * RFC 3339's `date-fullyear` is `4DIGIT`, so a year outside 0000–9999 is not an RFC 3339 timestamp at all: the
    * formatter below would spell it with ISO 8601's expanded form (`+10000-01-01T00:00:00Z`), which is exactly the
    * "renders something no conformant parser accepts" failure this object exists to prevent.
    *
    * The same number is the storage bound, from two directions. `events.cloud_event` is partitioned into
    * `cloud_event_YYYY_MM` leaves whose name pattern is four digits wide — `MonthPartition` refuses to build a name
    * outside it — and PostgreSQL's `timestamptz` domain ends at 294276 AD, so a value beyond that is a `22008 datetime
    * field overflow` raised from inside a bind parameter. Two independent reasons, one range, and it is around eight
    * thousand years wider than any telemetry archive will need.
    */
  val MinYear: Int = 0

  val MaxYear: Int = 9999

  /** Whether a value is inside [[MinYear]]–[[MaxYear]].
    *
    * Exposed because a smart constructor has to enforce the same bound on a value that never arrived as text —
    * `com.worxbend.kernel.search.Filter.occurred` is built directly by the histogram drill-down as well as by the
    * permalink codec, and a bound that guards only one entry point is not a bound.
    *
    * The check is on the value's own local year rather than its UTC one, which is the conservative reading: an
    * eighteen-hour offset can move the instant across the boundary, and the rendered form — the thing that has to stay
    * RFC 3339 — carries the local year.
    */
  def inRange(value: OffsetDateTime): Boolean =
    value.getYear >= MinYear && value.getYear <= MaxYear

  private val formatter: DateTimeFormatter =
    DateTimeFormatterBuilder()
      .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .appendOffsetId()
      .toFormatter()

  def render(value: OffsetDateTime): String = formatter.format(value)

  /** Parsing stays lenient about **shape** — the built-in ISO parser — because rejecting an inbound event over a
    * missing `:00` would lose data to punish someone else's serialiser. Strictness about shape belongs on the way out,
    * where this build is the author.
    *
    * It is not lenient about **range**, and the distinction is the point. `OffsetDateTime.parse` accepts ISO 8601's
    * expanded year, so `+999999999-01-01T00:00:00Z` used to come back as an ordinary timestamp and travel on: into
    * `Filter.Occurred`, into `occurred_at >= ?` as a bind parameter, and into a PostgreSQL `22008 datetime field
    * overflow` on a page whose codec promises never to 500. Accepting a value nothing downstream can store is not
    * leniency, it is deferring the failure to somewhere it cannot be reported. See [[MinYear]].
    */
  def parse(raw: String): Either[String, OffsetDateTime] =
    Try(OffsetDateTime.parse(raw)).toEither.left
      .map(_ => s"'$raw' is not an RFC 3339 timestamp")
      .filterOrElse(inRange, s"'$raw' is outside the years $MinYear-$MaxYear that RFC 3339 can spell")
