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

package io.kzonix.cobalt

import com.typesafe.scalalogging.StrictLogging
import io.kzonix.observability.Meters
import io.kzonix.persistence.maintenance.PartitionMaintenance
import io.kzonix.persistence.maintenance.RollupRefresh
import java.time.Instant
import org.apache.pekko.actor.Cancellable
import org.apache.pekko.actor.Scheduler
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/** The two database maintenance jobs on cobalt's schedule.
  *
  * **Why cobalt and not ferrite.** ADR §5 assigned these to ferrite, and this build puts them here for the reason that
  * outranks it: ferrite is the read side. cobalt already owns the Flyway migrations (see `CobaltApp.start`), so it is
  * the one service that writes schema, and a partition is schema. Splitting them would mean two services able to
  * change the shape of `events.cloud_event`, and the deploy-ordering question that follows — *may ferrite start before
  * cobalt has migrated?* — has no good answer. The narrower argument is just as decisive: partitions exist so that
  * inserts have somewhere to go, and ferrite never inserts. A deployment that scaled ferrite to zero overnight to save
  * money would stop creating partitions for a write path that never stopped writing. Recorded as a deliberate
  * deviation from ADR §5, not as an oversight.
  *
  * **Why the first partition pass is synchronous.** A scheduled first run, even at zero delay, is a race with
  * `EventConsumer.start`, and the loser is ingest: a fresh deployment of this build months after `V1__events.sql` was
  * written has partitions for July and August 2026 and nothing else, so the first batch through the consumer fails
  * with `no partition of relation "cloud_event" found for row` — or, worse, succeeds into `cloud_event_default`, where
  * it is invisible to partition pruning and where its presence then *blocks* the partition the job is about to try to
  * create. Running it to completion before the consumer exists removes the race entirely, and it costs one pass over
  * a handful of `CREATE TABLE`s. This mirrors `Probes.start`, which probes once synchronously for the same class of
  * reason.
  *
  * **A failure never stops the boot and never stops the schedule.** `runQuietly` catches, meters and logs, because the
  * two alternatives are both worse: an exception escaping a scheduled task cancels every subsequent run of it — the
  * job would die silently on one transient connection error and nothing would say so — and an exception escaping the
  * synchronous first pass would take down a consumer that is otherwise perfectly able to run, over a database
  * hiccup, in a `CrashLoopBackOff`. The failure is not swallowed: it lands on `maintenance.job.duration` with
  * `outcome=failure`, which is the channel an operator can actually be paged from.
  */
object MaintenanceJobs extends StrictLogging:

  /** Runs the partition pass once, then puts both jobs on the scheduler.
    *
    * `scheduleWithFixedDelay` and not `scheduleAtFixedRate`: these jobs block on JDBC and their duration is a function
    * of how much work there is to do. A fixed *rate* would queue a second run behind a first that overran — against a
    * database that is by hypothesis already struggling — and the advisory lock would not help, because the pile-up is
    * inside this process and never reaches the lock.
    *
    * Returns the handle that cancels both. Cancelling is what stops a job from running *during* shutdown, after the
    * pools it uses have been closed.
    */
  def start(
    partitions: PartitionMaintenance,
    rollup: RollupRefresh,
    metrics: MaintenanceMetrics,
    config: MaintenanceConfig,
    scheduler: Scheduler,
    clock: () => Instant = () => Instant.now()
  )(using ExecutionContext): AutoCloseable =
    if !config.enabled then
      // The escape hatch for a deployment that maintains its partitions elsewhere (a DBA's cron, a platform-managed
      // pg_partman). Logged at warn, because the far likelier cause of this being off is that somebody turned it off
      // to debug something in 2026 and nobody turned it back on.
      logger.warn("database maintenance is disabled; nothing will create partitions or refresh the hourly rollup")
      () => ()
    else
      runPartitions(partitions, metrics, clock)
      val partitionTick = scheduler.scheduleWithFixedDelay(config.partitionInterval, config.partitionInterval): () =>
        runPartitions(partitions, metrics, clock)
      val rollupTick = scheduler.scheduleWithFixedDelay(config.refreshInterval, config.refreshInterval): () =>
        runRollup(rollup, metrics)
      logger.info(
        s"database maintenance scheduled: partitions every ${config.partitionInterval} " +
          s"(${config.monthsAhead} month(s) ahead), rollup refresh every ${config.refreshInterval}"
      )
      cancelBoth(partitionTick, rollupTick)

  private def runPartitions(
    partitions: PartitionMaintenance,
    metrics: MaintenanceMetrics,
    clock: () => Instant
  ): Unit =
    runQuietly(Meters.Jobs.Partitions, metrics):
      partitions.run(clock()) match
        case None =>
          Meters.Outcomes.Skipped
        case Some(report) =>
          metrics.partitions(report)
          if report.blocked.nonEmpty then
            // Already logged with the full remedy by the job itself; repeated here as one line so the count is
            // visible next to the other maintenance logging rather than only inside a multi-line SQL block.
            logger.error(
              s"${report.blocked.size} month(s) cannot be partitioned until rows are moved out of the default " +
                "partition; see the remedy logged above"
            )
          Meters.Outcomes.Success

  private def runRollup(rollup: RollupRefresh, metrics: MaintenanceMetrics): Unit =
    runQuietly(Meters.Jobs.Rollup, metrics):
      rollup.run() match
        case None    => Meters.Outcomes.Skipped
        case Some(_) => Meters.Outcomes.Success

  /** Times the body, records its outcome, and never lets it escape.
    *
    * A `skipped` run is timed too, and deliberately: it is the cheap measurement that proves the schedule is still
    * ticking on a replica that never wins the lock. Without it, a replica whose job thread had died would be
    * indistinguishable from one that is merely losing every race.
    *
    * Package-visible so a unit test can assert the outcome mapping — especially that a throwing job still records a
    * timing tagged `failure` — without a database. That mapping is the entire mechanism by which a broken maintenance
    * job reaches an operator, and it would otherwise only be exercised on the slow tier.
    */
  private[cobalt] def runQuietly(job: String, metrics: MaintenanceMetrics)(body: => String): Unit =
    val started = System.nanoTime()
    val outcome =
      try body
      catch
        case NonFatal(error) =>
          logger.error(s"the $job job failed; it will be retried on the next tick", error)
          Meters.Outcomes.Failure
    metrics.observe(job, outcome, System.nanoTime() - started)

  private def cancelBoth(first: Cancellable, second: Cancellable): AutoCloseable =
    () =>
      val _ = first.cancel()
      val _ = second.cancel()
