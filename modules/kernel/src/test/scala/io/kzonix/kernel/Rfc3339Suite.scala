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

package io.kzonix.kernel

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** RFC 3339 rendering.
  *
  * The first test is the one that matters: it is the difference between ISO 8601 and RFC 3339, it is what
  * `OffsetDateTime.toString` gets wrong, and it is undetectable from inside a Scala-only round-trip.
  */
final class Rfc3339Suite extends munit.ScalaCheckSuite:

  private val genTimestamp: Gen[OffsetDateTime] =
    for
      epochSecond <- Gen.choose(0L, 4102444800L)
      nano        <- Gen.choose(0, 999999999)
      quarters    <- Gen.choose(-56, 56)
    yield OffsetDateTime.ofInstant(
      Instant.ofEpochSecond(epochSecond, nano.toLong),
      ZoneOffset.ofTotalSeconds(quarters * 900)
    )

  test("seconds are always rendered, unlike OffsetDateTime.toString"):
    val whole = OffsetDateTime.parse("2018-04-05T17:31:00Z")
    assertEquals(whole.toString, "2018-04-05T17:31Z")
    assertEquals(Rfc3339.render(whole), "2018-04-05T17:31:00Z")

  test("a zero offset renders as Z and a non-zero one as ±HH:MM"):
    assertEquals(Rfc3339.render(OffsetDateTime.parse("2026-07-26T12:00:00+00:00")), "2026-07-26T12:00:00Z")
    assertEquals(Rfc3339.render(OffsetDateTime.parse("2026-07-26T12:00:00+05:45")), "2026-07-26T12:00:00+05:45")

  test("the producer's local offset survives — it is not normalised to UTC"):
    val local = OffsetDateTime.parse("2026-07-26T12:00:00+02:00")
    assertEquals(Rfc3339.parse(Rfc3339.render(local)), Right(local))
    assertNotEquals(Rfc3339.render(local), Rfc3339.render(local.withOffsetSameInstant(ZoneOffset.UTC)))

  test("a fractional second is kept but trailing zeros are trimmed"):
    assertEquals(Rfc3339.render(OffsetDateTime.parse("2026-07-26T12:00:00.500000000Z")), "2026-07-26T12:00:00.5Z")
    assertEquals(Rfc3339.render(OffsetDateTime.parse("2026-07-26T12:00:00.000Z")), "2026-07-26T12:00:00Z")

  test("nonsense is rejected rather than thrown"):
    assert(Rfc3339.parse("yesterday").isLeft)
    assert(Rfc3339.parse("").isLeft)
    assert(Rfc3339.parse("2026-07-26").isLeft)

  property("render then parse is the identity"):
    forAll(genTimestamp): timestamp =>
      Rfc3339.parse(Rfc3339.render(timestamp)) == Right(timestamp)

  property("rendering is idempotent through a parse"):
    forAll(genTimestamp): timestamp =>
      val rendered = Rfc3339.render(timestamp)
      Rfc3339.parse(rendered).map(Rfc3339.render) == Right(rendered)
