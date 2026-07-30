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
import java.time.YearMonth
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.util.Try

/** One monthly partition of `events.cloud_event`: a name and a half-open UTC bound pair.
  *
  * **Everything here is arithmetic on `YearMonth`, and the zone is [[MonthPartition.Zone]] rather than the ambient
  * one.** That is the whole reason this type exists as a value instead of the job computing bounds inline.
  * `V1__events.sql` says it in the DDL and ADR §12.4 lists it as a standing trap: a partition bound literal is parsed
  * in the *session* timezone, so a bare date on a server running `Europe/Warsaw` shifts every partition by an hour and
  * files the first hour of each month into the neighbouring partition. The symptom appears only at month ends, only for
  * a handful of rows, and looks exactly like a producer with a wrong clock.
  *
  * The same trap has a JVM-side half, which is what [[MonthPartition.Zone]] closes: `YearMonth.now()` and
  * `instant.atZone(ZoneId.systemDefault)` both consult the machine's timezone, so a container deployed without `TZ=UTC`
  * would decide it is still September while the database has already moved to October. Every conversion below goes
  * through `Zone`, and a unit test pins `Zone` to UTC — which is the only assertion that stays honest on a CI machine
  * that is itself UTC.
  *
  * DST never enters into it. UTC has no offset transitions, so "the first instant of month M" is a total function of
  * `(year, month)` and month arithmetic is exact addition on a 12-month cycle. That is a property of choosing UTC, not
  * an accident: `atStartOfDay(someCivilZone)` is *not* total, because a civil zone can skip midnight entirely.
  *
  * @param month
  *   the UTC calendar month this partition holds.
  */
final case class MonthPartition(month: YearMonth):

  /** The leaf table name — `cloud_event_2026_09`.
    *
    * Checked rather than trusted. This string is the one value in the maintenance path that becomes SQL *text* instead
    * of a bind parameter, because PostgreSQL takes no parameters in DDL at all: `CREATE TABLE ?` is not a statement.
    * `modules/persistence` otherwise makes that impossible by construction (`Sql.lit` rejects any argument that is not
    * a compile-time constant), so where the guarantee has to be re-established by hand it is re-established loudly. No
    * caller-supplied string reaches here — the value is derived from a clock and an `Int` — and the `require` is what
    * keeps that true after the next edit.
    */
  val name: String =
    val text = f"${MonthPartition.TablePrefix}${month.getYear}%04d_${month.getMonthValue}%02d"
    require(
      MonthPartition.NamePattern.matches(text),
      s"'$text' is not a partition name this module will ever put into DDL"
    )
    text

  /** Schema-qualified, for use in statements. */
  def qualifiedName: String = s"${MonthPartition.Schema}.$name"

  /** First instant of the month, inclusive. */
  def from: Instant = month.atDay(1).atStartOfDay(MonthPartition.Zone).toInstant

  /** First instant of the *next* month, exclusive — the half-open upper bound `FOR VALUES ... TO` wants. */
  def until: Instant = month.plusMonths(1).atDay(1).atStartOfDay(MonthPartition.Zone).toInstant

  /** The lower bound as PostgreSQL literal text, with the explicit `+00` offset that stops the session timezone from
    * moving it.
    */
  def fromLiteral: String = MonthPartition.literal(from)

  /** The upper bound as PostgreSQL literal text. */
  def untilLiteral: String = MonthPartition.literal(until)

  /** Whether an event with this timestamp belongs in this partition. Half-open, matching `FOR VALUES FROM ... TO`. */
  def contains(instant: Instant): Boolean = !instant.isBefore(from) && instant.isBefore(until)

  def next: MonthPartition = MonthPartition(month.plusMonths(1))

object MonthPartition:

  /** The schema `V1__events.sql` creates. */
  val Schema: String = "events"

  /** The partitioned parent. */
  val Parent: String = "cloud_event"

  /** Qualified parent, as it appears in every statement this package emits. */
  val QualifiedParent: String = s"$Schema.$Parent"

  /** The catch-all partition. Named here because two different jobs need to talk about it and a re-typed string is how
    * the detach path would one day detach the safety net.
    */
  val DefaultPartition: String = s"${Parent}_default"

  val QualifiedDefaultPartition: String = s"$Schema.$DefaultPartition"

  val TablePrefix: String = s"${Parent}_"

  /** **The only timezone partition arithmetic is ever done in.** Exposed so a test can assert on it: on a CI machine
    * that is itself UTC, no amount of boundary testing can distinguish "uses UTC" from "uses the ambient zone", and
    * pinning the constant is the assertion that stays true anyway.
    */
  val Zone: ZoneOffset = ZoneOffset.UTC

  /** Matches `V1__events.sql`'s hand-written bounds — `2026-08-01 00:00:00+00`. Lower-case `x` renders a zero offset as
    * `+00`; upper-case `X` would render it as `Z`, which PostgreSQL also accepts but which would make the generated DDL
    * stop matching the migration it extends.
    */
  private val Bound: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssx").withZone(Zone)

  private val NamePattern = """cloud_event_\d{4}_\d{2}""".r

  private val NameRegex = """^cloud_event_(\d{4})_(\d{2})$""".r

  private def literal(instant: Instant): String = Bound.format(instant)

  /** The partition an instant belongs to. */
  def of(instant: Instant): MonthPartition = MonthPartition(monthOf(instant))

  /** The UTC calendar month an instant falls in. */
  def monthOf(instant: Instant): YearMonth = YearMonth.from(instant.atOffset(Zone))

  /** Reads a leaf table name back into a month.
    *
    * `None` for anything that is not a monthly partition — most importantly for `cloud_event_default`, which is how the
    * retention path is structurally incapable of detaching the clock-skew safety net: the catch-all never parses, so it
    * is never a candidate.
    */
  def parse(name: String): Option[MonthPartition] =
    name match
      case NameRegex(year, month) => Try(YearMonth.of(year.toInt, month.toInt)).toOption.map(MonthPartition.apply)
      case _                      => None

/** Which months should exist, and which have outlived the retention window.
  *
  * Pure, and separated from the job that executes the DDL for the usual reason: month arithmetic across a year boundary
  * is the part that is easy to get subtly wrong and cheap to test exhaustively, while `CREATE TABLE` needs a database
  * and a Docker daemon.
  */
object PartitionCalendar:

  /** The months that must have a partition: the current one plus `monthsAhead` following it.
    *
    * Inclusive of the current month, so `monthsAhead = 3` yields four partitions. That is the reading ADR §5's "N+3
    * months ahead" needs in order to be safe: a job that created only the *future* months would leave a freshly
    * deployed service with nowhere to put the event that arrives one second later.
    *
    * Year rollover is `YearMonth.plusMonths`, not `month + n` with a manual carry — December 2026 plus one is January
    * 2027 and not month 13 of 2026, and the arithmetic is the JDK's rather than this module's.
    */
  def window(now: Instant, monthsAhead: Int): Vector[MonthPartition] =
    require(monthsAhead >= 0, s"monthsAhead must not be negative, was $monthsAhead")
    val current = MonthPartition.monthOf(now)
    (0 to monthsAhead).toVector.map(step => MonthPartition(current.plusMonths(step.toLong)))

  /** The partitions that fall outside the retention window, oldest first.
    *
    * `retainMonths` counts back from **and includes** the current month: with `retainMonths = 12` in July 2026, the
    * retained range is August 2025 through July 2026 and anything ending at or before 2025-08-01 is a candidate. The
    * inclusive reading is the conservative one — the other reading silently retires one more month than an operator
    * asked for, and the data is only recoverable because this job detaches rather than drops.
    *
    * Future months are never candidates regardless of the arithmetic, because `cutoff` is always at or before the
    * current month.
    */
  def expired(now: Instant, retainMonths: Int, existing: Vector[MonthPartition]): Vector[MonthPartition] =
    require(retainMonths >= 1, s"retainMonths must keep at least the current month, was $retainMonths")
    val cutoff = MonthPartition.monthOf(now).minusMonths((retainMonths - 1).toLong)
    existing.filter(_.month.isBefore(cutoff)).sortBy(_.month)

  /** How many consecutive months, starting with the current one, already have a partition.
    *
    * This is ADR §5's "months of headroom" and it is deliberately *consecutive from now* rather than "the newest
    * partition minus today": a gap in the middle of the window is as fatal as a missing tail — the row for the missing
    * month lands in `cloud_event_default` — and a max-based measure would hide it behind the months on either side.
    *
    * `0` means the current month itself has no partition, i.e. every event being ingested right now is landing in the
    * catch-all. That is the value to alert on.
    */
  def headroom(now: Instant, existing: Set[MonthPartition]): Int =
    Iterator
      .iterate(MonthPartition.of(now))(_.next)
      .takeWhile(existing.contains)
      .take(PartitionCalendar.MaxHeadroom)
      .size

  /** Bound on [[headroom]]'s scan so a database with a decade of pre-created partitions cannot turn a gauge into a long
    * loop. Well above any sane `monthsAhead`.
    */
  val MaxHeadroom: Int = 240
