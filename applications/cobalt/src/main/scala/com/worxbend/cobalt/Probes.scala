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

import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** The background poller behind both the lag gauge and the readiness probe.
  *
  * One timer thread doing both, on purpose: the two questions have the same period, the same failure modes, and the
  * same consequence when the answer is "no". Splitting them would give two schedules to keep in sync and a window in
  * which the gauge says the broker is fine while readiness says it is not.
  *
  * **A failure here is recorded, never thrown.** An exception escaping a `ScheduledExecutorService` task silently
  * cancels all subsequent runs — the gauge would freeze at its last value and readiness would freeze at its last
  * answer, which is the exact opposite of what a health poller is for.
  */
final class Probes(
  offsets: AdminOffsets,
  gauge: ConsumerLagGauge,
  groupId: String,
  dataSource: DataSource,
  health: HealthChecks,
  validationTimeout: FiniteDuration
) extends StrictLogging:

  /** One round: broker reachability, lag, then database reachability. Public so a test — and the composition root's
    * first synchronous probe — can run it without a scheduler.
    */
  def probe(): Unit =
    probeBroker()
    probeDatabase()

  private def probeBroker(): Unit =
    try
      val nodes = offsets.clusterNodes()
      if nodes <= 0 then health.broker.down("the cluster reports no brokers")
      else
        health.broker.up()
        val committed = offsets.committed(groupId)
        gauge.update(ConsumerLag.lags(committed, offsets.logEnds(committed.keySet)))
    catch
      case NonFatal(error) =>
        val cause = RecordDecoder.describe(error)
        health.broker.down(cause)
        logger.warn(s"the Kafka broker probe failed: $cause")

  /** `Connection.isValid` and not `SELECT 1`: it is the JDBC contract's own liveness check, it is bounded by a timeout
    * the driver honours, and it does not consume a statement-cache slot on every poll.
    */
  private def probeDatabase(): Unit =
    try
      val connection = dataSource.getConnection
      try
        if connection.isValid(validationTimeout.toSeconds.toInt.max(1)) then health.database.up()
        else health.database.down("the connection did not validate")
      finally connection.close()
    catch
      case NonFatal(error) =>
        val cause = RecordDecoder.describe(error)
        health.database.down(cause)
        logger.warn(s"the PostgreSQL probe failed: $cause")

object Probes:

  /** Probes once synchronously, then on a fixed delay.
    *
    * The first probe is synchronous so the very first readiness answer is evidence rather than the pessimistic default
    * — otherwise every deploy reports one interval of false negatives and orchestrators learn to ignore the probe.
    *
    * Fixed *delay* and not fixed *rate*: when the broker is unreachable each probe takes the full admin timeout, and a
    * fixed rate would queue overlapping runs against a dependency that is already struggling.
    */
  def start(probes: Probes, interval: FiniteDuration): AutoCloseable =
    val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(daemon("cobalt-probe"))
    probes.probe()
    val _ = scheduler.scheduleWithFixedDelay(
      () => probes.probe(),
      interval.toMillis,
      interval.toMillis,
      TimeUnit.MILLISECONDS
    )
    new AutoCloseable:
      def close(): Unit =
        val _ = scheduler.shutdownNow()

  private def daemon(name: String): ThreadFactory =
    runnable =>
      val thread = Thread(runnable, name)
      thread.setDaemon(true)
      thread
