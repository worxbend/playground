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
import com.worxbend.persistence.maintenance.PartitionReport
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** The maintenance jobs' telemetry, in the shared vocabulary of `modules/observability`.
  *
  * **Why this exists at all.** Both jobs fail silently by construction. The partition job stops creating months and
  * absolutely nothing happens — until a month boundary, at which point every insert fails with `no partition of
  * relation "cloud_event" found for row` and ingest stops completely. The rollup refresh stops running and the
  * dashboards keep answering, correctly formatted and increasingly wrong. Neither produces a request error, a failed
  * health probe, or a Kafka symptom. A log line exists in both cases and is read by nobody, because nothing prompts
  * anyone to look. Metrics are the only channel through which either failure reaches an operator before its consequence
  * does.
  *
  * **Why the gauges are separate `AtomicLong`s rather than a callback into the job.** Micrometer polls a gauge at
  * scrape time on the scrape thread; a gauge whose supplier ran a query would put a database round trip — with the
  * pool's timeout, and no `statement_timeout` of its own — on the path of every Prometheus scrape, and a slow database
  * would then also break the metrics that were supposed to tell you the database is slow. Publishing last-known values
  * into an `AtomicLong` decouples the two: the scrape is a field read, and the value is as fresh as the last job run. A
  * stale gauge is exactly the right reading when the job has stopped, and `maintenance.job.duration`'s count is what
  * says so.
  *
  * **`-Werror` trap (ADR §7.4).** Micrometer's builders return `this` and its registration methods return the meter, so
  * every registration below is bound with `val _ =` or is part of an expression.
  */
final class MaintenanceMetrics(registry: MeterRegistry):

  private val defaultRows: AtomicLong = AtomicLong(0L)
  private val headroom: AtomicLong = AtomicLong(0L)
  private val blocked: AtomicLong = AtomicLong(0L)

  // Registered eagerly, in the constructor, so all three exist in the exposition from the first scrape. A gauge that
  // only appears after the first successful run is absent for exactly the deployment where the job never ran, which
  // is the deployment the alert is for: `absent()` and `== 0` need very different alert rules, and only one of them
  // is the rule anyone writes.
  gauge(Meters.PartitionDefaultRows, defaultRows)
  gauge(Meters.MaintenancePartitionHeadroom, headroom)
  gauge(Meters.MaintenancePartitionsBlocked, blocked)

  /** One run of one job: how long it took and how it ended.
    *
    * `outcome` comes from [[Meters.Outcomes]] and never from an exception's class name — the tag has to stay a closed
    * set, and the diagnostic belongs on the log line where cardinality is free.
    */
  def observe(job: String, outcome: String, elapsedNanos: Long): Unit =
    registry
      .timer(Meters.MaintenanceDuration, Tags.of(Meters.TagKeys.Job, job, Meters.TagKeys.Outcome, outcome))
      .record(elapsedNanos, TimeUnit.NANOSECONDS)

  /** Publishes what a partition pass found. Counters advance, gauges are replaced. */
  def partitions(report: PartitionReport): Unit =
    if report.created.nonEmpty then
      registry.counter(Meters.MaintenancePartitionsCreated).increment(report.created.size.toDouble)
    if report.detached.nonEmpty then
      registry.counter(Meters.MaintenancePartitionsDetached).increment(report.detached.size.toDouble)
    defaultRows.set(report.defaultRows)
    headroom.set(report.headroomMonths.toLong)
    blocked.set(report.blocked.size.toLong)

  private def gauge(name: String, state: AtomicLong): Unit =
    val _ = Gauge.builder(name, state, (value: AtomicLong) => value.get().toDouble).register(registry)
