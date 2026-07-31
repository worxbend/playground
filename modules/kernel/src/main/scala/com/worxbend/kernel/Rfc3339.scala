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

  private val formatter: DateTimeFormatter =
    DateTimeFormatterBuilder()
      .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .appendOffsetId()
      .toFormatter()

  def render(value: OffsetDateTime): String = formatter.format(value)

  /** Parsing stays lenient — the built-in ISO parser — because rejecting an inbound event over a missing `:00` would
    * lose data to punish someone else's serialiser. Strictness belongs on the way out, where this build is the author.
    */
  def parse(raw: String): Either[String, OffsetDateTime] =
    Try(OffsetDateTime.parse(raw)).toEither.left.map(_ => s"'$raw' is not an RFC 3339 timestamp")
