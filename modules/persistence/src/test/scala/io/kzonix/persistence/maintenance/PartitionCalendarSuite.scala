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

package io.kzonix.persistence.maintenance

import java.time.Instant
import java.time.OffsetDateTime
import java.time.YearMonth
import java.time.ZoneOffset
import scala.concurrent.duration.DurationInt

/** The month arithmetic, which is the half of partition maintenance that can be wrong without anything failing.
  *
  * A partition created for the wrong month does not raise: it is a perfectly valid table that simply does not cover the
  * rows arriving, so those rows go to `cloud_event_default` and the failure surfaces weeks later as a slow query and a
  * non-zero `partition.default.rows`. Every case below is one of the ways that arithmetic goes wrong quietly — a year
  * boundary, a month index off by one, or a timezone leaking in from the machine.
  */
final class PartitionCalendarSuite extends munit.FunSuite:

  private def at(text: String): Instant = OffsetDateTime.parse(text).toInstant

  test("partition names are zero-padded so they sort chronologically as text"):
    // `cloud_event_2026_9` would sort after `cloud_event_2026_10`, and every catalog listing an operator reads is
    // ordered by name.
    assertEquals(MonthPartition(YearMonth.of(2026, 9)).name, "cloud_event_2026_09")
    assertEquals(MonthPartition(YearMonth.of(2026, 12)).name, "cloud_event_2026_12")

  test("bounds are the UTC month boundaries, half-open"):
    val september = MonthPartition(YearMonth.of(2026, 9))
    assertEquals(september.from, at("2026-09-01T00:00:00Z"))
    assertEquals(september.until, at("2026-10-01T00:00:00Z"))
    assert(september.contains(at("2026-09-01T00:00:00Z")), "the lower bound is inclusive")
    assert(!september.contains(at("2026-10-01T00:00:00Z")), "the upper bound is exclusive")
    assert(!september.contains(at("2026-08-31T23:59:59Z")))

  test("bound literals carry the explicit +00 offset, because a bare date is parsed in the session timezone"):
    // `V1__events.sql` writes its own bounds this way and `MigrationScriptSuite` asserts on it; a generated bound that
    // dropped the offset would shift with the server's timezone and file the first hour of each month next door.
    val september = MonthPartition(YearMonth.of(2026, 9))
    assertEquals(september.fromLiteral, "2026-09-01 00:00:00+00")
    assertEquals(september.untilLiteral, "2026-10-01 00:00:00+00")

  test("December rolls over into January of the next year"):
    val december = MonthPartition(YearMonth.of(2026, 12))
    assertEquals(december.until, at("2027-01-01T00:00:00Z"))
    assertEquals(december.untilLiteral, "2027-01-01 00:00:00+00")
    assertEquals(december.next.name, "cloud_event_2027_01")

  test("the window rolls the year over rather than producing a thirteenth month"):
    val window = PartitionCalendar.window(at("2026-11-15T12:00:00Z"), monthsAhead = 3)
    assertEquals(
      window.map(_.name),
      Vector("cloud_event_2026_11", "cloud_event_2026_12", "cloud_event_2027_01", "cloud_event_2027_02")
    )

  test("the window includes the current month, so a fresh deployment has somewhere to put the next event"):
    val window = PartitionCalendar.window(at("2026-09-15T12:00:00Z"), monthsAhead = 3)
    assertEquals(window.size, 4, "monthsAhead counts months beyond the current one")
    assertEquals(window.head.name, "cloud_event_2026_09")
    assertEquals(
      PartitionCalendar.window(at("2026-09-15T12:00:00Z"), monthsAhead = 0).map(_.name),
      Vector("cloud_event_2026_09")
    )

  test("consecutive windows are contiguous and leave no gap for a row to fall through"):
    val window = PartitionCalendar.window(at("2026-11-01T00:00:00Z"), monthsAhead = 14)
    window.sliding(2).foreach:
      case Vector(earlier, later) => assertEquals(earlier.until, later.from, s"gap between $earlier and $later")
      case _                      => ()

  test("the month of an instant is its UTC month, not its month anywhere else"):
    // 2026-09-01T00:30Z is still 2026-08-31 in any zone west of UTC and already 2026-09-01 everywhere east. The
    // partition it belongs to is decided by the database, which stores an instant, so it must be decided here the
    // same way.
    assertEquals(MonthPartition.monthOf(at("2026-09-01T00:30:00Z")), YearMonth.of(2026, 9))
    assertEquals(MonthPartition.monthOf(at("2026-08-31T23:30:00Z")), YearMonth.of(2026, 8))

  test("the calendar's zone is UTC, which is the assertion a UTC build machine cannot make any other way"):
    // Boundary tests above cannot distinguish "uses UTC" from "uses the ambient zone" on a machine that is itself UTC,
    // and mutating TimeZone.getDefault inside one test would leak into every suite sharing the JVM. Pinning the
    // constant every conversion goes through is the check that stays honest.
    assertEquals(MonthPartition.Zone, ZoneOffset.UTC)

  test("UTC has no offset transitions, so a DST changeover month is arithmetically ordinary"):
    // The European changeover falls inside March and October. In a civil zone the day would be 23 or 25 hours long and
    // `atStartOfDay` would not even be total; in UTC the month is the same exact interval as any other.
    val march = MonthPartition(YearMonth.of(2026, 3))
    val october = MonthPartition(YearMonth.of(2026, 10))
    assertEquals(march.fromLiteral, "2026-03-01 00:00:00+00")
    assertEquals(march.untilLiteral, "2026-04-01 00:00:00+00")
    assertEquals(october.untilLiteral, "2026-11-01 00:00:00+00")

  test("a leap February is 29 days and still ends exactly on the first of March"):
    val february = MonthPartition(YearMonth.of(2028, 2))
    assertEquals(february.until, at("2028-03-01T00:00:00Z"))

  test("names round-trip, and the catch-all deliberately does not parse"):
    val partition = MonthPartition(YearMonth.of(2027, 1))
    assertEquals(MonthPartition.parse(partition.name), Some(partition))
    // This is the structural reason retention can never detach the clock-skew safety net: it is not a candidate
    // because it is not a month.
    assertEquals(MonthPartition.parse("cloud_event_default"), None)
    assertEquals(MonthPartition.parse("cloud_event"), None)
    assertEquals(MonthPartition.parse("cloud_event_2026_13"), None, "month 13 is not a month")
    assertEquals(MonthPartition.parse("cloud_event_2026_9"), None, "an unpadded name is not one this job wrote")
    assertEquals(MonthPartition.parse("events.cloud_event_2026_09"), None, "the name is the leaf, unqualified")

  test("retention counts back from and including the current month"):
    val existing = (1 to 14).toVector.map(month => MonthPartition(YearMonth.of(2026, 1).plusMonths(month - 1L)))
    // July 2026 with retainMonths = 3 keeps May, June, July.
    val expired = PartitionCalendar.expired(at("2026-07-15T00:00:00Z"), retainMonths = 3, existing)
    assertEquals(expired.map(_.name), Vector(1, 2, 3, 4).map(m => f"cloud_event_2026_$m%02d"))

  test("retention across a year boundary retires the right months and not the whole previous year"):
    val existing = (0 to 17).toVector.map(step => MonthPartition(YearMonth.of(2025, 9).plusMonths(step.toLong)))
    val expired = PartitionCalendar.expired(at("2026-02-10T00:00:00Z"), retainMonths = 6, existing)
    // February 2026 keeping six months retains September 2025 through February 2026; nothing before it exists here.
    assertEquals(expired, Vector.empty[MonthPartition])
    val stricter = PartitionCalendar.expired(at("2026-02-10T00:00:00Z"), retainMonths = 3, existing)
    assertEquals(stricter.map(_.name), Vector("cloud_event_2025_09", "cloud_event_2025_10", "cloud_event_2025_11"))

  test("retention never proposes a future partition, whatever the window"):
    val future = (0 to 6).toVector.map(step => MonthPartition(YearMonth.of(2026, 7).plusMonths(step.toLong)))
    assertEquals(PartitionCalendar.expired(at("2026-07-15T00:00:00Z"), retainMonths = 1, future), Vector.empty)

  test("headroom counts consecutive months from now, so a hole in the middle is not hidden by the tail"):
    val now = at("2026-09-10T00:00:00Z")
    val september = MonthPartition(YearMonth.of(2026, 9))
    val october = MonthPartition(YearMonth.of(2026, 10))
    val december = MonthPartition(YearMonth.of(2026, 12))
    assertEquals(PartitionCalendar.headroom(now, Set(september, october, december)), 2)
    assertEquals(PartitionCalendar.headroom(now, Set(september, october)), 2)
    // The value that means events are landing in the catch-all right now.
    assertEquals(PartitionCalendar.headroom(now, Set(october, december)), 0)
    assertEquals(PartitionCalendar.headroom(now, Set.empty), 0)

  test("headroom is bounded, so a decade of pre-created partitions cannot turn a gauge into a long loop"):
    val now = at("2026-09-10T00:00:00Z")
    val many = Iterator.iterate(MonthPartition.of(now))(_.next).take(400).toSet
    assertEquals(PartitionCalendar.headroom(now, many), PartitionCalendar.MaxHeadroom)

  test("a negative window and a retention that keeps nothing are rejected rather than silently reinterpreted"):
    intercept[IllegalArgumentException](PartitionCalendar.window(at("2026-09-01T00:00:00Z"), monthsAhead = -1))
    intercept[IllegalArgumentException](PartitionCalendar.expired(at("2026-09-01T00:00:00Z"), 0, Vector.empty))
    intercept[IllegalArgumentException](PartitionPolicy(-1, None, 5.seconds))
    intercept[IllegalArgumentException](PartitionPolicy(3, Some(0), 5.seconds))
    intercept[IllegalArgumentException](PartitionPolicy(3, None, 0.seconds))
