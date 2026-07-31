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

package com.worxbend.persistence.maintenance

import com.typesafe.scalalogging.StrictLogging
import java.sql.Connection
import java.sql.SQLException
import java.time.Instant
import java.time.YearMonth
import javax.sql.DataSource
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.Using
import scala.util.control.NonFatal

/** What one `CREATE TABLE ... PARTITION OF` attempt achieved. Three cases and not a `Boolean`, because "it was already
  * there" and "the DEFAULT partition is holding rows for that month" are the same non-event to a `Boolean` and opposite
  * events to an operator.
  */
private enum CreateOutcome:
  case Created, AlreadyPresent, Blocked

/** How far ahead to create partitions and how far back to keep them.
  *
  * @param monthsAhead
  *   months *beyond* the current one that must have a partition. `3` — ADR §5's figure — means four partitions exist at
  *   any time and the job may miss three consecutive monthly runs before an event has nowhere to go.
  * @param retainMonths
  *   how many months, counting back from and including the current one, stay attached. `None` disables retention
  *   entirely, which is the default: detaching is the only operation here that makes data disappear from queries, so it
  *   happens when somebody configures it and not because a default said so.
  * @param detachLockTimeout
  *   bound on how long a detach may wait for `ACCESS EXCLUSIVE` on the parent. See [[PartitionDdl.detach]] for why
  *   waiting is the dangerous part.
  */
final case class PartitionPolicy(
  monthsAhead: Int,
  retainMonths: Option[Int],
  detachLockTimeout: FiniteDuration
):
  require(monthsAhead >= 0, s"monthsAhead must not be negative, was $monthsAhead")
  require(retainMonths.forall(_ >= 1), s"retainMonths must keep at least the current month, was $retainMonths")
  require(detachLockTimeout.toMillis > 0L, s"detachLockTimeout must be positive, was $detachLockTimeout")

/** A month whose partition could not be created because rows for it are already in the `DEFAULT` partition.
  *
  * Carries the remedy rather than a pointer to a runbook, because the remedy depends on the month: the operator gets
  * the actual statements, with the actual bounds, for the actual table.
  */
final case class BlockedMonth(partition: MonthPartition, rows: Long):

  /** The paste-able transaction that moves the rows and attaches the month. See [[PartitionDdl.adoptDefaultRows]] for
    * what each step is for and why no job runs it automatically.
    */
  def remedy: String = PartitionDdl.adoptDefaultRows(partition)

/** What one partition-maintenance pass did.
  *
  * Returned as a value rather than only logged, for the same reason [[com.worxbend.persistence.MigrationReport]] is:
  * the caller meters it, the integration tests assert on it, and "the job ran" and "the job achieved anything" are
  * different questions that a log line answers equally badly.
  *
  * @param defaultRows
  *   total rows in `cloud_event_default` at the time of the pass. Feeds `partition.default.rows`, which ADR §7.1 says
  *   must stay `0`.
  * @param headroomMonths
  *   consecutive months from the current one that now have a partition — see [[PartitionCalendar.headroom]]. `0` means
  *   events are landing in the catch-all right now.
  */
final case class PartitionReport(
  created: Vector[MonthPartition],
  present: Vector[MonthPartition],
  blocked: Vector[BlockedMonth],
  detached: Vector[MonthPartition],
  defaultRows: Long,
  headroomMonths: Int
):

  /** Nothing to create, nothing blocked — the steady state, and the thing a second run must report. */
  def isNoOp: Boolean = created.isEmpty && detached.isEmpty && blocked.isEmpty

/** Rolling monthly partitions for `events.cloud_event`: create ahead, and optionally retire behind.
  *
  * **Why this cannot be a migration.** Flyway migrations are versioned and immutable — that immutability is what
  * `validateOnMigrate` enforces and what makes a schema reproducible. Partitions are the opposite kind of object: the
  * set that must exist is a function of *today's date*, so a `V1` that creates July and August 2026 is correct when it
  * is written and wrong from September onwards. A deployment of this build in 2027 would apply a migration that creates
  * nothing usable, and every event would land in `cloud_event_default`, where it is invisible to partition pruning and
  * expensive to get back out of. ADR §5 makes the same call in one line; this class is that line.
  *
  * **Create and detach are deliberately asymmetric, and the asymmetry is the design.** Creation is automatic,
  * unattended and scheduled, because the failure it prevents — `no partition of relation "cloud_event" found for row` —
  * is a total ingest outage that arrives on a calendar boundary at whatever hour that boundary falls. Retirement only
  * ever *detaches*: the table keeps existing, keeps its rows and keeps its indexes, and it simply stops being part of
  * the parent. `DROP TABLE` is never issued by anything in this codebase. The reasoning is not symmetry but
  * reversibility — a detach taken in error is undone with one `ATTACH PARTITION`, a drop taken in error is undone from
  * a backup if there is one — and the question "may this event data be destroyed" usually has a legal answer that a
  * scheduled job is not entitled to give. What the operator then does with the detached table (archive it, export it,
  * drop it) is a decision made by a person, with the data still there while they make it.
  *
  * **The `DEFAULT` partition is the trap this class is mostly about.** See [[PartitionDdl.adoptDefaultRows]]: once a
  * row for month M sits in `cloud_event_default`, PostgreSQL refuses to create a partition covering M, and refuses it
  * expensively — the attempt takes `ACCESS EXCLUSIVE` on the parent and scans the default partition before failing.
  * Detecting that up front, skipping the month, and reporting it with the remedy is strictly better than discovering it
  * as a stack trace: the job keeps making progress on every *other* month, the ingest path is never locked, and the
  * operator gets a statement to run rather than an error to interpret.
  */
final class PartitionMaintenance(dataSource: DataSource, policy: PartitionPolicy) extends StrictLogging:

  /** The lease. Public so a caller can reason about (and a test can contend for) the exact lock this job takes. */
  val lock: AdvisoryLock = AdvisoryLock(dataSource, AdvisoryLock.Partitions)

  /** One pass, under the advisory lock.
    *
    * `None` means another replica is already doing it — not a failure, and specifically not something to retry: the
    * work is being done, and the next tick is soon enough.
    */
  def run(now: Instant): Option[PartitionReport] = lock.tryRun(connection => runOn(connection, now))

  /** The pass itself, on a caller-supplied connection and without the lock.
    *
    * Package-visible so integration tests can drive it deterministically — in particular so a test can hold the lock on
    * one connection and prove that [[run]] on another declines, which is the property that would otherwise need two
    * threads and a sleep to observe.
    */
  private[persistence] def runOn(connection: Connection, now: Instant): PartitionReport =
    val attached = attachedPartitions(connection)
    val defaultRows = defaultRowsByMonth(connection)
    val wanted = PartitionCalendar.window(now, policy.monthsAhead)

    // Split before touching the database: a month whose rows are already in DEFAULT must not even be attempted, or
    // the attempt itself takes ACCESS EXCLUSIVE on the parent and blocks ingest while it scans its way to failing.
    val (occupied, creatable) =
      wanted.filterNot(attached.contains).partition(month => defaultRows.contains(month.month))

    val attempted = creatable.map(month => month -> create(connection, month))
    val created = attempted.collect { case (month, CreateOutcome.Created) => month }
    // A month that became blocked between the survey and the CREATE is reported exactly like one the survey caught,
    // so the two paths cannot tell the operator different stories about the same situation.
    val lateBlocked = attempted.collect { case (month, CreateOutcome.Blocked) => BlockedMonth(month, 0L) }
    val allBlocked = (occupied.map(month => BlockedMonth(month, defaultRows.getOrElse(month.month, 0L))) ++ lateBlocked)
      .sortBy(_.partition.month)

    allBlocked.foreach: month =>
      logger.error(
        s"partition ${month.partition.name} cannot be created: ${MonthPartition.DefaultPartition} already holds " +
          s"${month.rows} row(s) for that month, and PostgreSQL refuses an overlapping partition while they are " +
          s"there. Ingest for that month will keep landing in the catch-all until an operator runs:\n${month.remedy}"
      )

    val detached = policy.retainMonths.toVector.flatMap(retain => detachExpired(connection, now, retain, attached))
    val live = (attached ++ created).toSet -- detached
    val report = PartitionReport(
      created = created,
      present = attached.sortBy(_.month),
      blocked = allBlocked,
      detached = detached,
      defaultRows = defaultRows.values.sum,
      headroomMonths = PartitionCalendar.headroom(now, live)
    )
    if report.created.nonEmpty || report.detached.nonEmpty then
      logger.info(
        s"partition maintenance: created ${names(report.created)}, detached ${names(report.detached)}, " +
          s"${report.headroomMonths} month(s) of headroom"
      )
    report

  /** Creates one partition and re-applies the leaf-only storage options.
    *
    * Two SQLSTATEs are outcomes rather than defects, and they are distinguished because they mean opposite things:
    *
    *   - `42P07 duplicate_table` — somebody else created it between the catalog survey and here, so the job got what it
    *     wanted and there is nothing to report. The advisory lock rules that out between replicas of this job but not
    *     against an operator with a `psql` session open.
    *   - `23514 check_violation` — the `DEFAULT` partition acquired a row for this month inside the same window. That
    *     is the blocking case the survey exists to catch, arrived a few milliseconds later, and it must reach the
    *     operator with the same remedy.
    *
    * Anything else propagates. A permission error or a full disk is not something to paper over as "no partitions
    * needed creating".
    */
  private def create(connection: Connection, partition: MonthPartition): CreateOutcome =
    try
      execute(connection, PartitionDdl.create(partition))
      execute(connection, PartitionDdl.applyStorageOptions(partition))
      CreateOutcome.Created
    catch
      case error: SQLException if error.getSQLState == PartitionMaintenance.DuplicateTable =>
        logger.debug(s"partition ${partition.name} already existed")
        CreateOutcome.AlreadyPresent
      case error: SQLException if error.getSQLState == PartitionMaintenance.CheckViolation =>
        logger.warn(s"partition ${partition.name} is blocked by rows in ${MonthPartition.DefaultPartition}")
        CreateOutcome.Blocked

  /** Detaches every attached partition older than the retention window, oldest first.
    *
    * Each detach is its own transaction with its own `SET LOCAL lock_timeout`, and a failure is logged and skipped
    * rather than propagated: a contended lock on the oldest month must not stop the rest of the pass, and every one of
    * these is retried on the next tick anyway. Retention is a background tidy — there is no month in which it is
    * urgent.
    */
  private def detachExpired(
    connection: Connection,
    now: Instant,
    retainMonths: Int,
    attached: Vector[MonthPartition]
  ): Vector[MonthPartition] =
    PartitionCalendar
      .expired(now, retainMonths, attached)
      .flatMap: partition =>
        try
          detach(connection, partition)
          logger.info(
            s"detached ${partition.qualifiedName}; it still exists and still holds its rows. Dropping it is an " +
              "operator decision, deliberately not this job's"
          )
          Some(partition)
        catch
          case NonFatal(error) =>
            logger.warn(s"detaching ${partition.qualifiedName} failed; it stays attached until the next pass", error)
            None

  /** `SET LOCAL` needs a transaction to be local to, so autocommit is turned off for exactly these two statements and
    * restored afterwards — the connection goes back to a pool that expects it the way it was lent out.
    */
  private def detach(connection: Connection, partition: MonthPartition): Unit =
    val autoCommit = connection.getAutoCommit
    connection.setAutoCommit(false)
    try
      execute(connection, PartitionDdl.lockTimeout(policy.detachLockTimeout))
      execute(connection, PartitionDdl.detach(partition))
      connection.commit()
    catch
      case error: Throwable =>
        try connection.rollback()
        catch case NonFatal(suppressed) => error.addSuppressed(suppressed)
        throw error
    finally connection.setAutoCommit(autoCommit)

  private def attachedPartitions(connection: Connection): Vector[MonthPartition] =
    Using.resource(connection.createStatement()): statement =>
      Using.resource(statement.executeQuery(PartitionDdl.attachedPartitions)): results =>
        Iterator
          .continually(results)
          .takeWhile(_.next())
          .flatMap(row => MonthPartition.parse(row.getString(1)))
          .toVector

  /** Rows in the catch-all, keyed by the UTC month they belong to. Empty in the healthy case, which is why it is one
    * grouped scan rather than a probe per candidate month.
    */
  private def defaultRowsByMonth(connection: Connection): Map[YearMonth, Long] =
    Using.resource(connection.createStatement()): statement =>
      Using.resource(statement.executeQuery(PartitionDdl.defaultRowsByMonth)): results =>
        Iterator
          .continually(results)
          .takeWhile(_.next())
          .flatMap: row =>
            // `YYYY-MM` is `YearMonth`'s own ISO form, which is why the query renders it that way rather than handing
            // the driver a timestamp and letting its timezone handling back into an answer that is about timezones.
            val month = Try(YearMonth.parse(row.getString(1))).toOption
            month.map(_ -> row.getLong(2))
          .toMap

  private def execute(connection: Connection, sql: String): Unit =
    Using.resource(connection.createStatement()): statement =>
      val _ = statement.execute(sql)

  private def names(partitions: Vector[MonthPartition]): String =
    if partitions.isEmpty then "none" else partitions.map(_.name).mkString(", ")

object PartitionMaintenance:

  /** `duplicate_table` — the partition is already there. */
  val DuplicateTable: String = "42P07"

  /** `check_violation` — PostgreSQL's answer when the `DEFAULT` partition holds rows the new bound would claim. */
  val CheckViolation: String = "23514"
