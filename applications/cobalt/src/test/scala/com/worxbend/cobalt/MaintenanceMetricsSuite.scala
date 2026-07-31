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

package com.worxbend.cobalt

import com.worxbend.observability.Meters
import com.worxbend.persistence.maintenance.BlockedMonth
import com.worxbend.persistence.maintenance.MonthPartition
import com.worxbend.persistence.maintenance.PartitionReport
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.YearMonth

/** The telemetry contract of the maintenance jobs.
  *
  * Worth testing precisely because these meters are the *only* way either job's failure becomes visible: nothing about
  * a partition job that has stopped creating months produces an error, a failed probe or a Kafka symptom until a month
  * boundary turns it into an ingest outage. A meter that is registered under the wrong name, or only after the first
  * successful run, is indistinguishable from a healthy system right up to the moment it is not.
  */
final class MaintenanceMetricsSuite extends munit.FunSuite:

  private def registry(): MeterRegistry = SimpleMeterRegistry()

  private def report(
    created: Vector[MonthPartition] = Vector.empty,
    detached: Vector[MonthPartition] = Vector.empty,
    blocked: Vector[BlockedMonth] = Vector.empty,
    defaultRows: Long = 0L,
    headroom: Int
  ): PartitionReport =
    PartitionReport(created, Vector.empty, blocked, detached, defaultRows, headroom)

  private def month(year: Int, value: Int): MonthPartition = MonthPartition(YearMonth.of(year, value))

  test("the gauges exist from construction, before any job has run"):
    // The deployment where the job never ran once is exactly the deployment the alert is for, and `absent()` and
    // `== 0` are different alert rules — only one of which anybody writes.
    val meters = registry()
    val _ = MaintenanceMetrics(meters)
    Vector(
      Meters.PartitionDefaultRows,
      Meters.MaintenancePartitionHeadroom,
      Meters.MaintenancePartitionsBlocked
    ).foreach: name =>
      assert(meters.find(name).gauge() != null, s"$name is not in the exposition until the first run")
      assertEquals(meters.find(name).gauge().value(), 0.0d, s"$name did not start at zero")

  test("a run is timed and tagged with its job and its outcome"):
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    metrics.observe(Meters.Jobs.Partitions, Meters.Outcomes.Success, 1_000_000L)
    val timer = meters
      .find(Meters.MaintenanceDuration)
      .tags(Tags.of(Meters.TagKeys.Job, Meters.Jobs.Partitions, Meters.TagKeys.Outcome, Meters.Outcomes.Success))
      .timer()
    assert(timer != null, "the run was not timed under the shared meter name")
    assertEquals(timer.count(), 1L)

  test("the two jobs and the four outcomes are separate series, so one cannot hide the other"):
    // A rollup refresh runs every five minutes and the partition job every six hours; folded into one series the
    // partition job's failures would be a rounding error on the refresh's volume.
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    metrics.observe(Meters.Jobs.Partitions, Meters.Outcomes.Failure, 1L)
    metrics.observe(Meters.Jobs.Rollup, Meters.Outcomes.Success, 1L)
    metrics.observe(Meters.Jobs.Rollup, Meters.Outcomes.Skipped, 1L)
    assertEquals(meters.find(Meters.MaintenanceDuration).timers().size, 3)

  test("a pass publishes what it created, what it detached, and the two leading indicators"):
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    metrics.partitions(
      report(
        created = Vector(month(2026, 10), month(2026, 11)),
        detached = Vector(month(2025, 1)),
        blocked = Vector(BlockedMonth(month(2026, 12), 7L)),
        defaultRows = 7L,
        headroom = 2
      )
    )
    assertEquals(meters.find(Meters.MaintenancePartitionsCreated).counter().count(), 2.0d)
    assertEquals(meters.find(Meters.MaintenancePartitionsDetached).counter().count(), 1.0d)
    assertEquals(meters.find(Meters.PartitionDefaultRows).gauge().value(), 7.0d)
    assertEquals(meters.find(Meters.MaintenancePartitionHeadroom).gauge().value(), 2.0d)
    assertEquals(meters.find(Meters.MaintenancePartitionsBlocked).gauge().value(), 1.0d)

  test("a steady-state pass leaves the counters alone but keeps refreshing the gauges"):
    // Creation is a step function of roughly one per month; a counter nudged on every no-op pass would make "the job
    // is running" and "the job is achieving something" indistinguishable.
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    metrics.partitions(report(headroom = 4))
    assertEquals(meters.find(Meters.MaintenancePartitionsCreated).counter(), null)
    assertEquals(meters.find(Meters.MaintenancePartitionHeadroom).gauge().value(), 4.0d)

  test("gauges are replaced, not accumulated, so a recovered month lowers the blocked count again"):
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    metrics.partitions(report(blocked = Vector(BlockedMonth(month(2026, 12), 7L)), defaultRows = 7L, headroom = 1))
    metrics.partitions(report(headroom = 4))
    assertEquals(meters.find(Meters.MaintenancePartitionsBlocked).gauge().value(), 0.0d)
    assertEquals(meters.find(Meters.PartitionDefaultRows).gauge().value(), 0.0d)

  test("a job that throws is recorded as a failure rather than escaping and cancelling the schedule"):
    // An exception out of a scheduled task cancels every subsequent run of it, so the job would die on one transient
    // connection error and nothing anywhere would say so.
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    MaintenanceJobs.runQuietly(Meters.Jobs.Partitions, metrics)(throw IllegalStateException("the database is gone"))
    val failures = meters
      .find(Meters.MaintenanceDuration)
      .tags(Tags.of(Meters.TagKeys.Job, Meters.Jobs.Partitions, Meters.TagKeys.Outcome, Meters.Outcomes.Failure))
      .timer()
    assert(failures != null, "a failed run left no trace in the exposition")
    assertEquals(failures.count(), 1L)

  test("a skipped run is timed too, which is what proves a losing replica's schedule is still ticking"):
    val meters = registry()
    val metrics = MaintenanceMetrics(meters)
    MaintenanceJobs.runQuietly(Meters.Jobs.Rollup, metrics)(Meters.Outcomes.Skipped)
    val skipped = meters
      .find(Meters.MaintenanceDuration)
      .tags(Tags.of(Meters.TagKeys.Job, Meters.Jobs.Rollup, Meters.TagKeys.Outcome, Meters.Outcomes.Skipped))
      .timer()
    assertEquals(skipped.count(), 1L)
