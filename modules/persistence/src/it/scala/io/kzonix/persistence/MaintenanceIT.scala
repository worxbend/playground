package io.kzonix.persistence

import io.kzonix.persistence.maintenance.AdvisoryLock
import io.kzonix.persistence.maintenance.MonthPartition
import io.kzonix.persistence.maintenance.PartitionCalendar
import io.kzonix.persistence.maintenance.PartitionDdl
import io.kzonix.persistence.maintenance.PartitionMaintenance
import io.kzonix.persistence.maintenance.PartitionPolicy
import io.kzonix.persistence.maintenance.PartitionReport
import io.kzonix.persistence.maintenance.RefreshReport
import io.kzonix.persistence.maintenance.RollupRefresh
import java.time.Instant
import java.time.YearMonth
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt
import scala.util.Using

/** The maintenance jobs against a real PostgreSQL 18.
  *
  * None of what follows can be proved anywhere else. `PartitionCalendarSuite` proves the arithmetic and
  * `PartitionDdlSuite` proves the text, but every interesting property of these jobs is a property of the *database's*
  * reaction to that text: that `CREATE TABLE ... PARTITION OF` really is idempotent when the duplicate SQLSTATE is
  * caught, that PostgreSQL really does refuse an overlapping partition while the `DEFAULT` partition holds matching
  * rows, that `REFRESH ... CONCURRENTLY` really is legal against the unique index `V1__events.sql` creates, and that
  * two sessions really do contend on `pg_try_advisory_lock`. Each of those is a claim about PostgreSQL that a unit test
  * can only restate.
  *
  * **The clock is the real one.** These tests take `Instant.now()` rather than a fixed instant, because the specific
  * failure being guarded against is a deployment running months after `V1__events.sql` was written — and a test with a
  * frozen 2026-07 clock is a test that keeps passing forever while the production job silently has nothing to do. Every
  * assertion is therefore relative to whatever month it is when the suite runs.
  *
  * Each test cleans up what it left, in particular anything it put in `cloud_event_default` and anything it detached:
  * `MigrationIT` asserts that the default partition is empty and that every hand-written index carries a comment, and a
  * detached partition's indexes stop being partitions of the parent's and would start failing that check.
  */
final class MaintenanceIT extends PostgresSuite:

  private val now: Instant = Instant.now()

  private val policy: PartitionPolicy =
    PartitionPolicy(monthsAhead = 3, retainMonths = None, detachLockTimeout = 5.seconds)

  private def partitionJob(overrides: PartitionPolicy = policy): PartitionMaintenance =
    PartitionMaintenance(database.write.get(), overrides)

  private def rollup: RollupRefresh = RollupRefresh(database.write.get())

  private def ran(result: Option[PartitionReport]): PartitionReport =
    result.getOrElse(fail("the advisory lock was already held; nothing else should be running against this database"))

  /** The month `offset` months after the current one. Relative, because the suite runs on the real calendar. */
  private def monthAfter(offset: Int): MonthPartition = MonthPartition(MonthPartition.monthOf(now).plusMonths(offset))

  test("a pass creates every month in the window, including months the migration never knew about"):
    val report = ran(partitionJob().run(now))
    val wanted = PartitionCalendar.window(now, policy.monthsAhead)
    val attached = attachedPartitions()
    wanted.foreach: partition =>
      assert(attached.contains(partition.name), s"${partition.name} was not created; have: ${attached.mkString(", ")}")
    // `V1__events.sql` hard-codes 2026-07 and 2026-08, so on any run after August 2026 this pass is the only reason
    // ingest has anywhere to write at all.
    assert(
      report.headroomMonths > policy.monthsAhead,
      s"expected more than ${policy.monthsAhead} months of headroom, got ${report.headroomMonths}"
    )
    assertEquals(report.blocked, Vector.empty)

  test("a second pass creates nothing, which is what makes it safe on every replica on every tick"):
    val second = ran(partitionJob().run(now))
    assertEquals(second.created, Vector.empty, "the second pass created a partition that already existed")
    assert(second.isNoOp, s"a repeat pass was not a no-op: $second")
    // Still reports the state, not merely "nothing happened" — a job that can only say "no-op" cannot feed a gauge.
    assert(second.headroomMonths > policy.monthsAhead)
    assertEquals(second.present.map(_.name).distinct.size, second.present.size, "a partition was listed twice")

  test("a created partition carries the insert-driven autovacuum settings, which are not inherited"):
    // The newest month is the only one being written to, so a month that misses these is the one month whose
    // visibility map goes stale and whose index-only scans quietly stop being index-only.
    val fresh = monthAfter(policy.monthsAhead)
    val options = query(s"SELECT unnest(reloptions) FROM pg_class WHERE relname = '${fresh.name}'")
    assert(options.contains("autovacuum_vacuum_insert_scale_factor=0.0"), options.mkString(", "))
    assert(options.contains("autovacuum_vacuum_insert_threshold=50000"), options.mkString(", "))

  test("rows already in the default partition block their month, and the job reports it instead of failing"):
    // The trap in full: PostgreSQL will not create a partition whose range overlaps rows held by DEFAULT, and the
    // attempt is expensive — ACCESS EXCLUSIVE on the parent plus a scan — before it fails. The job must detect this
    // up front, keep making progress on every other month, and hand the operator a remedy.
    val blockedMonth = monthAfter(6)
    assert(!attachedPartitions().contains(blockedMonth.name), "the fixture month already has a partition")
    insertEvent(blockedMonth.from.plus(3, ChronoUnit.DAYS), "blocked-fixture")
    try
      assertEquals(countRows("events.cloud_event_default"), 1L, "the fixture row did not land in the catch-all")

      val report = ran(partitionJob(policy.copy(monthsAhead = 6)).run(now))

      assertEquals(report.blocked.map(_.partition), Vector(blockedMonth))
      assertEquals(report.blocked.head.rows, 1L)
      assertEquals(report.defaultRows, 1L, "the total feeds partition.default.rows, which must stay 0")
      assert(!attachedPartitions().contains(blockedMonth.name), "the blocked month must not have been created")
      // Progress on every *other* month is the point of detecting rather than failing.
      assert(report.created.map(_.name).contains(monthAfter(5).name), s"the job stopped early: $report")
      // And the report carries statements, not a pointer to a runbook.
      val remedy = report.blocked.head.remedy
      assert(remedy.contains(s"ATTACH PARTITION ${blockedMonth.qualifiedName}"), remedy)
    finally truncateEvents()

  test("the remedy the job reports is real: running it moves the rows and attaches the month"):
    // A documented remedy nobody has executed is a guess. This runs it exactly as an operator would paste it, and
    // asserts the end state PostgreSQL refused to reach on its own.
    val month = monthAfter(8)
    val occurredAt = month.from.plus(2, ChronoUnit.DAYS)
    insertEvent(occurredAt, "remedy-fixture")
    assertEquals(countRows("events.cloud_event_default"), 1L)

    val report = ran(partitionJob(policy.copy(monthsAhead = 8)).run(now))
    val blocked = report.blocked.find(_.partition == month).getOrElse(fail(s"$month was not reported blocked"))

    runScript(blocked.remedy)

    assert(attachedPartitions().contains(month.name), "the remedy did not attach the month")
    assertEquals(countRows("events.cloud_event_default"), 0L, "the rows were not moved out of the catch-all")
    assertEquals(countRows(month.qualifiedName), 1L, "the rows did not arrive in the new partition")
    // The generated columns are recomputed by the database on the way in, so the moved row is a first-class row and
    // not a partial copy — which is the whole reason the remedy names only the non-generated columns.
    assertEquals(query(s"SELECT ce_id FROM ${month.qualifiedName}"), Vector("remedy-fixture"))
    assertEquals(query(s"SELECT ce_type FROM ${month.qualifiedName}"), Vector("io.kzonix.it.maintenance"))
    truncateEvents()

  test("the rollup refresh actually moves the numbers the migration froze at zero"):
    // V1 refreshes the view once, against the table it has just created — i.e. against nothing. Without a refresh job
    // every facet count and every dashboard aggregate reads that empty snapshot, successfully, forever.
    truncateEvents()
    val baseline = refreshed(rollup.run())
    assertEquals(baseline.rows, 0L)
    assert(baseline.concurrent, "the refresh fell back to the blocking form; the unique index may be missing")

    insertEvent(now, "rollup-one")
    insertEvent(now, "rollup-two")
    assertEquals(countRows("events.event_rollup_hourly"), 0L, "a materialized view is a snapshot; nothing updates it")

    val second = refreshed(rollup.run())
    assertEquals(second.rows, 1L, "both events share an hour, a type, a source and a severity: one bucket")
    assertEquals(
      query("SELECT event_count FROM events.event_rollup_hourly").mkString(","),
      "2",
      "the bucket did not pick up both events"
    )
    truncateEvents()

  test("two concurrent runs do not both execute: the loser skips and changes nothing"):
    // Deterministic rather than threaded. The outer `tryRun` holds the partition lock on one pooled connection; the
    // inner `run` takes a different connection, so it is a different session and genuinely contends.
    val ahead = 10
    val contendedMonth = monthAfter(ahead)
    assert(!attachedPartitions().contains(contendedMonth.name), "the fixture month already has a partition")

    val job = partitionJob(policy.copy(monthsAhead = ahead))
    val outcome = job.lock.tryRun(_ => job.run(now))

    assertEquals(outcome, Some(None), "the inner run should have found the lock held and declined")
    assert(
      !attachedPartitions().contains(contendedMonth.name),
      "the replica that lost the lock did the work anyway, which makes the lock decorative"
    )

    // And the same job succeeds the moment the lock is free, so the skip was contention and not a broken job.
    val free = ran(job.run(now))
    assert(free.created.map(_.name).contains(contendedMonth.name), s"the uncontended run did nothing: $free")

  test("the rollup lock is separate, so a long refresh cannot lock out partition creation"):
    // A missing partition is a hard ingest failure; a stale rollup is a slightly old dashboard. Holding one must not
    // block the other.
    val partitions = partitionJob()
    val held = AdvisoryLock(database.write.get(), AdvisoryLock.Rollup).tryRun(_ => partitions.run(now))
    assert(held.exists(_.isDefined), "holding the rollup lock blocked the partition job")

  test("retention detaches an expired partition without destroying it, and never touches the catch-all"):
    // The asymmetry that matters: creation is automatic, retirement is reversible. A detached partition is still a
    // table, still holds its rows, and dropping it stays a decision made by a person.
    val ancient = MonthPartition(YearMonth.of(2020, 1))
    execute(PartitionDdl.create(ancient))
    execute(PartitionDdl.applyStorageOptions(ancient))
    insertEvent(ancient.from.plus(1, ChronoUnit.DAYS), "ancient-fixture")
    try
      assertEquals(countRows(ancient.qualifiedName), 1L)

      // Twelve months of retention: comfortably keeps the current month and its neighbours whatever month it is when
      // this runs, and comfortably expires January 2020.
      val report = ran(partitionJob(policy.copy(retainMonths = Some(12))).run(now))

      assertEquals(report.detached.map(_.name), Vector(ancient.name))
      assert(!attachedPartitions().contains(ancient.name), "the partition is still attached")
      // Detached, not dropped — the table and every row in it survive.
      assertEquals(countRows(ancient.qualifiedName), 1L, "retention destroyed data it was only supposed to detach")
      assertEquals(countRows("events.cloud_event"), 0L, "the detached rows are no longer visible through the parent")
      // The clock-skew safety net is not a month, so it can never be a retention candidate.
      assert(attachedPartitions().contains("cloud_event_default"))
    finally
      // Test cleanup, not the job's business: a detached partition's indexes are no longer partitions of the parent's,
      // which would make `MigrationIT`'s "every index carries a comment" check fail on thirteen of them.
      execute(s"DROP TABLE IF EXISTS ${ancient.qualifiedName}")

  // --- fixture plumbing --------------------------------------------------------------------------------------------

  private def refreshed(result: Option[RefreshReport]): RefreshReport =
    result.getOrElse(fail("the rollup advisory lock was already held"))

  /** The leaves currently attached to the fact table, by the same catalog query the job uses. */
  private def attachedPartitions(): Vector[String] = query(PartitionDdl.attachedPartitions)

  private def countRows(table: String): Long = query(s"SELECT count(*) FROM $table").head.toLong

  /** A minimal but genuinely valid CloudEvent: the fact table's CHECK constraints reject anything less, and the
    * generated columns are computed from `raw` by the database rather than supplied here.
    */
  private def insertEvent(occurredAt: Instant, id: String): Unit =
    val raw =
      s"""{"specversion":"1.0","id":"$id","source":"urn:kzonix:it:maintenance",""" +
        """"type":"io.kzonix.it.maintenance","data":{"deviceId":"device-it","severity":"info"}}"""
    withConnection: connection =>
      Using.resource(
        connection.prepareStatement(
          "INSERT INTO events.cloud_event (occurred_at, raw, payload_sha256) " +
            "VALUES (?::timestamptz, ?::jsonb, sha256(convert_to(?, 'UTF8')))"
        )
      ): statement =>
        statement.setString(1, occurredAt.toString)
        statement.setString(2, raw)
        statement.setString(3, raw)
        val _ = statement.executeUpdate()

  /** Runs a multi-statement script the way an operator pastes one: comments stripped, statements in order.
    *
    * Split here rather than handed to the driver whole, so that a failure names the statement that failed instead of
    * the whole transaction.
    */
  private def runScript(script: String): Unit =
    val statements = script.linesIterator
      .filterNot(_.trim.startsWith("--"))
      .mkString("\n")
      .split(";")
      .map(_.trim)
      .filter(_.nonEmpty)
    // One connection for the whole script, because the script opens a transaction: BEGIN on one pooled connection and
    // CREATE TABLE on another would be two unrelated sessions and would prove nothing about the remedy.
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        statements.foreach: sql =>
          val _ = statement.execute(sql)

  private def query(sql: String): Vector[String] =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        Using.resource(statement.executeQuery(sql)): results =>
          Iterator.continually(results).takeWhile(_.next()).map(_.getString(1)).toVector

  private def execute(sql: String): Unit =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute(sql)
