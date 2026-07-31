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

package com.worxbend.wolfram

import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.event.Envelope
import java.time.Instant
import java.time.OffsetDateTime
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/** The ingest-time plausibility check on CloudEvents `time` (ADR §4.3, §12.4).
  *
  * **Why this exists at the edge and nowhere else.** `events.cloud_event` is RANGE-partitioned on `occurred_at`, which
  * is the event's own `time`. A device whose clock reads 2038 does not produce a wrong-looking row, it produces a row
  * in a partition that does not exist — so it lands in the DEFAULT partition, where it is invisible to partition
  * pruning. ADR §12.4 spells out the consequence: *once the default partition holds rows, creating an overlapping
  * partition takes `ACCESS EXCLUSIVE` and scans it*. That is a maintenance outage caused by one bad clock, and the only
  * cheap place to prevent it is the one process that sees the event before it is durable.
  *
  * **Why it rejects rather than clamps, despite the name.** Rewriting an implausible `time` to `now` would produce a
  * row that is *silently wrong* — indistinguishable from a correctly-timestamped event, and impossible to repair later
  * because the original value is gone. ADR §4.3 says it in four words: "reject; never invent defaults". The name
  * "clamp" is the ADR's and is kept so the two documents talk about the same thing.
  *
  * **Why an absent `time` is also a rejection.** `modules/kernel` models `time` as `Option` on purpose: the CloudEvents
  * spec makes it OPTIONAL and the *codec* must stay total, or a DLQ record could not be parsed back. The ingestion API
  * is allowed to be stricter than the codec, and it has to be: `occurred_at` is `NOT NULL` in the DDL, so an event with
  * no `time` has no row it could ever become. Rejecting at the edge with a sentence naming the missing attribute is
  * strictly better than accepting it and having cobalt dead-letter it minutes later.
  *
  * The window is asymmetric — hours ahead, months behind — because the two directions fail differently. A future
  * timestamp is almost always a broken clock and creating partitions ahead of real time is expensive; a past timestamp
  * is usually a gateway draining a buffer after an outage, which is legitimate and must keep working.
  */
final case class TimeClamp(maxFutureSkew: FiniteDuration, maxPastSkew: FiniteDuration):

  /** The earliest `time` accepted at instant `now`. */
  def earliest(now: Instant): Instant = now.minusMillis(maxPastSkew.toMillis)

  /** The latest `time` accepted at instant `now`. */
  def latest(now: Instant): Instant = now.plusMillis(maxFutureSkew.toMillis)

  /** Checks one envelope's `time`, returning the accepted value.
    *
    * Returns the `OffsetDateTime` rather than `Unit` so callers cannot forget which of the two representations is
    * authoritative: the producer's offset is preserved (never normalised to UTC — ADR §4.1), and the comparison is done
    * on the instant, which is the only thing an offset-bearing timestamp can be ordered by.
    */
  def check(envelope: Envelope, now: Instant): Either[Rejection, OffsetDateTime] =
    envelope.time match
      case None =>
        Left(
          Rejection.ImplausibleTime(
            "attribute 'time' is required by this API: it becomes the partitioning key of the stored event, and " +
              "inventing one would silently misfile the event"
          )
        )
      case Some(value) =>
        val instant = value.toInstant
        if instant.isAfter(latest(now)) then
          Left(
            Rejection.ImplausibleTime(
              s"time ${Rfc3339.render(value)} is more than ${describe(maxFutureSkew)} in the future " +
                s"(ingest clock reads $now); check the producer's clock"
            )
          )
        else if instant.isBefore(earliest(now)) then
          Left(
            Rejection.ImplausibleTime(
              s"time ${Rfc3339.render(value)} is more than ${describe(maxPastSkew)} in the past " +
                s"(ingest clock reads $now); backfill beyond that window needs an operator-run import"
            )
          )
        else Right(value)

  /** Durations are rendered for humans rather than with `FiniteDuration#toString`, which prints
    * `2073600000 milliseconds` for 24 days once HOCON has normalised the unit.
    */
  private def describe(duration: FiniteDuration): String =
    val hours = duration.toHours
    if hours >= 48 then s"${hours / 24} days"
    else if hours >= 1 then s"$hours hours"
    else s"${duration.toMinutes} minutes"

object TimeClamp:

  /** The window ADR §12.4 fixes: 24 h ahead, 90 d behind. Used when no configuration is supplied — notably by tests,
    * which must exercise the documented window and not one invented in a fixture.
    */
  val Default: TimeClamp = TimeClamp(24.hours, 90.days)

  /** Builds the clamp a configuration asks for. */
  def from(config: IngestConfig): TimeClamp = TimeClamp(config.maxFutureSkew, config.maxPastSkew)
