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

package io.kzonix.wolfram

import io.kzonix.kernel.event.Envelope
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import munit.FunSuite
import scala.concurrent.duration.DurationInt

/** The plausibility clamp of ADR §4.3 / §12.4.
  *
  * These assertions are about a *partitioning* decision, not about validation aesthetics: an accepted implausible
  * `time` produces a row in the DEFAULT partition of `events.cloud_event`, and ADR §12.4 records that getting rows back
  * out of it takes an `ACCESS EXCLUSIVE` lock and a full scan. The boundaries are therefore asserted exactly, on a
  * fixed clock — a wall clock would make the ±1 ms cases flaky, which is precisely where a rounding bug would hide.
  */
final class TimeClampSuite extends FunSuite:

  private val clamp = TimeClamp(24.hours, 90.days)

  private def envelopeAt(time: Option[OffsetDateTime]): Envelope =
    Envelope
      .parse(
        Fixtures.structuredBody(time = time.map(io.kzonix.kernel.Rfc3339.render))
      )
      .fold(message => fail(s"fixture should parse: $message"), identity)

  test("the ADR's documented window is the default"):
    assertEquals(TimeClamp.Default, TimeClamp(24.hours, 90.days))
    assertEquals(TimeClamp.from(Fixtures.ingest), TimeClamp(24.hours, 90.days))

  test("a recent timestamp is accepted and returned unchanged"):
    val time = Fixtures.at(Duration.ofMinutes(-5))
    assertEquals(clamp.check(envelopeAt(Some(time)), Fixtures.now), Right(time))

  test("the producer's offset survives — the value is never normalised to UTC"):
    val time = Fixtures.now.atOffset(ZoneOffset.ofHours(2))
    val accepted = clamp.check(envelopeAt(Some(time)), Fixtures.now)
    assertEquals(accepted.map(_.getOffset), Right(ZoneOffset.ofHours(2)))

  test("exactly at each boundary is still accepted"):
    val future = Fixtures.at(Duration.ofHours(24))
    val past = Fixtures.at(Duration.ofDays(-90))
    assert(clamp.check(envelopeAt(Some(future)), Fixtures.now).isRight)
    assert(clamp.check(envelopeAt(Some(past)), Fixtures.now).isRight)

  test("one millisecond beyond either boundary is rejected"):
    val future = Fixtures.at(Duration.ofHours(24).plusMillis(1))
    val past = Fixtures.at(Duration.ofDays(-90).minusMillis(1))
    assertEquals(clamp.check(envelopeAt(Some(future)), Fixtures.now).left.map(_.reason), Left("invalid-attributes"))
    assertEquals(clamp.check(envelopeAt(Some(past)), Fixtures.now).left.map(_.reason), Left("invalid-attributes"))

  test("a rejection names which direction was wrong, so the producer knows where to look"):
    val future = clamp.check(envelopeAt(Some(Fixtures.at(Duration.ofDays(2)))), Fixtures.now)
    val past = clamp.check(envelopeAt(Some(Fixtures.at(Duration.ofDays(-200)))), Fixtures.now)
    assert(future.left.exists(_.detail.contains("in the future")), future.toString)
    assert(past.left.exists(_.detail.contains("in the past")), past.toString)

  test("an absent time is rejected rather than defaulted — occurred_at is NOT NULL and must not be invented"):
    val result = clamp.check(envelopeAt(None), Fixtures.now)
    assert(result.isLeft)
    assert(result.left.exists(_.detail.contains("required")), result.toString)

  test("rejections are ImplausibleTime, which the API renders as 400"):
    val result = clamp.check(envelopeAt(None), Fixtures.now)
    result match
      case Left(rejection: Rejection.ImplausibleTime) =>
        assertEquals(ApiModel.status(ApiModel.failure(rejection)).code, 400)
      case other => fail(s"expected an ImplausibleTime rejection, got $other")
