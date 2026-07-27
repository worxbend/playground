/*
 * Copyright (c) 2020 Kzonix Projects
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

package io.kzonix.ferrite.web.view

import java.time.Duration
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.concurrent.duration.FiniteDuration

/** Every string a template displays but does not own.
  *
  * Formatting lives here, not in Twirl, for the same reason the view models are flat: a template cannot be unit-tested
  * cheaply and a duplicated format string between a fragment and the page that wraps it is invisible until the two
  * disagree on screen. Everything here is pure and total.
  *
  * `Locale.ROOT` throughout. This is an operator-facing tool whose output sits next to ISO timestamps, JSON and SQL; a
  * thousands separator that changes with the server's default locale would make two replicas render different numbers
  * for the same data, and there is no i18n requirement to trade that against.
  */
object Format:

  /** Human-readable absolute time. Seconds are kept — an event observatory that hides them is useless for correlating
    * against another system's log — and the offset is shown because ADR §4.1 keeps the producer's local offset on
    * purpose.
    */
  private val absoluteFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX", Locale.ROOT)

  /** Compact form for a histogram axis, where the date is already implied by the window. */
  private val bucketFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("MMM d HH:mm", Locale.ROOT)

  def absolute(value: OffsetDateTime): String = absoluteFormatter.format(value)

  def bucket(value: OffsetDateTime): String = bucketFormatter.format(value)

  /** "3 m ago" / "in 12 s" / "just now".
    *
    * Deliberately terse and unpluralised: this text repeats on every row of a dense table, where width costs more than
    * grammar. The exact time is one column away and in the `title` attribute, so nothing is lost.
    *
    * The future case is real, not defensive — a device with a skewed clock produces events dated ahead of the server,
    * and showing "in 3 h" is how an operator discovers that. Clamping it to "just now" would hide the bug.
    */
  def relative(value: OffsetDateTime, now: OffsetDateTime): String =
    val seconds = Duration.between(value, now).getSeconds
    val magnitude = math.abs(seconds)
    if magnitude < 10 then "just now"
    else
      val quantity =
        if magnitude < 60 then s"$magnitude s"
        else if magnitude < 3600 then s"${magnitude / 60} m"
        else if magnitude < 86400 then s"${magnitude / 3600} h"
        else if magnitude < 2592000 then s"${magnitude / 86400} d"
        else s"${magnitude / 2592000} mo"
      if seconds >= 0 then s"$quantity ago" else s"in $quantity"

  /** Grouped decimal. */
  def count(value: Long): String = String.format(Locale.ROOT, "%,d", value)

  /** A count that may be a lower bound.
    *
    * ADR §6.3 makes the approximation a signed-off product decision: above the cap the answer is "10,000+" and never a
    * number, because a number that is silently approximate is a number that will be quoted in a meeting. The `+` is the
    * whole point of this function.
    */
  def boundedCount(value: Long, cap: Long): String =
    if value > cap then s"${count(cap)}+" else count(value)

  /** A bucket width, as an axis caption. */
  def duration(value: FiniteDuration): String =
    val seconds = value.toSeconds
    if seconds % 86400 == 0 then plural(seconds / 86400, "day")
    else if seconds % 3600 == 0 then plural(seconds / 3600, "hour")
    else if seconds % 60 == 0 then plural(seconds / 60, "minute")
    else plural(seconds, "second")

  private def plural(quantity: Long, unit: String): String =
    if quantity == 1 then s"1 $unit" else s"$quantity ${unit}s"

  /** Bar height as a whole percentage of the tallest bucket.
    *
    * A floor of 2 % for any non-zero bucket: a single event in a window of thousands rounds to 0 % and vanishes, and a
    * chart that renders "one" and "none" identically is worse than no chart. Zero stays zero.
    */
  def heightPercent(value: Long, peak: Long): Int =
    if value <= 0 || peak <= 0 then 0
    else math.max(2, math.round(value.toDouble / peak.toDouble * 100.0).toInt)

  /** `Option` to display text, with an explicit placeholder rather than an empty cell.
    *
    * An empty table cell is ambiguous between "no value" and "rendering bug"; an em dash is not.
    */
  val Absent: String = "—"

  def orAbsent(value: Option[String]): String = value.filter(_.nonEmpty).getOrElse(Absent)

  /** Metric readings are rendered at fixed precision so a column of them aligns; `Double.toString` produces anything
    * from `1.0` to `1.0E-5` and turns the column into noise.
    */
  def metric(value: Option[Double]): String =
    value.fold(Absent)(v => String.format(Locale.ROOT, "%.3f", v))
