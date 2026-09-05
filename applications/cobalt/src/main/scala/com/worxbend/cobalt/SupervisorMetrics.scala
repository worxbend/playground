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
import com.worxbend.observability.Meters
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** The consumer supervisor's own state, as metrics.
  *
  * **The gauge that matters most is `consume.running`, and it exists to make a deliberate pause distinguishable from an
  * outage.** Consumer lag rising with the consumer *running* is a consumer that cannot keep up; lag rising with it
  * *paused* is somebody's maintenance window. Alerting on lag alone cannot tell those apart, which is exactly how a
  * planned pause pages the on-call at 2am — and, worse, how everyone learns to acknowledge that page without reading
  * it.
  *
  * **Gauges, not counters, and fed from a poller rather than from the transition.** A gauge set only when the state
  * changes reports the truth right up until the process restarts, at which point it reports whatever the constructor
  * left there. Micrometer's gauge holds a weak reference to the state object and reads it at scrape time, so a value
  * updated by the same poller that already computes lag is both cheaper and more honest: it converges on the real value
  * within one interval no matter what happened before.
  *
  * **An unread gauge reports `NaN`, never its last value and never zero.** This is the other half of the same idea and
  * it is a correction: these gauges used to hold their reading indefinitely, so a poller whose `Await` was timing out
  * left `consume.running` frozen at 1 for the whole incident — the exact number an operator uses to rule the consumer
  * *out* as the cause. Prometheus renders `NaN` and every aggregation over it drops out, so "the probe has stopped
  * reporting" looks like absence rather than like a healthy zero. The cost is that the first scrape after a boot shows
  * gaps until the first probe lands, which is the truth.
  *
  * @param freshFor
  *   how long a reading stays believable. Comfortably more than the probe interval, because one slow tick is not an
  *   incident; [[SupervisorProbe]]'s caller sizes it from that interval.
  */
final class SupervisorMetrics(
  registry: MeterRegistry,
  freshFor: FiniteDuration = SupervisorMetrics.DefaultFreshFor,
  nanoTime: () => Long = () => System.nanoTime()
):

  private val running = SupervisorMetrics.Reading(freshFor, nanoTime)
  private val divergence = SupervisorMetrics.Reading(freshFor, nanoTime)
  private val dlqDepth = SupervisorMetrics.Reading(freshFor, nanoTime)

  gauge(Meters.ConsumeRunning, running)
  gauge(Meters.ConsumeCheckpointDivergence, divergence)
  gauge(Meters.DlqDepth, dlqDepth)

  /** Publishes a status reading. Called on the poller's tick, not on the transition — see the class comment. */
  def observe(status: ConsumerStatus): Unit =
    running.set(if status.consuming then 1L else 0L)
    divergence.set(SupervisorMetrics.divergenceOf(status))

  /** Publishes the DLQ backlog. Separate from [[observe]] because it comes from a different admin call and one of the
    * two failing must not take the other's reading down with it.
    */
  def observeDlq(outstanding: Long): Unit = dlqDepth.set(math.max(0L, outstanding))

  /** Counts a lifecycle command.
    *
    * The audit trail for an API that can stop ingestion and move offsets. `http.server.requests` records that a POST
    * happened; this records *which verb* and whether it changed anything — and an unexpected non-zero rate here is the
    * first sign that something automated is driving the pipeline.
    *
    * `outcome=skipped` for a no-op is the same convention the maintenance jobs use for a lost advisory-lock race:
    * neither succeeded nor failed, and folding it into either would misreport a healthy steady state.
    */
  def command(name: String, changed: Boolean): Unit =
    val outcome = if changed then Meters.Outcomes.Success else Meters.Outcomes.Skipped
    registry
      .counter(Meters.ConsumeLifecycle, Tags.of(Meters.TagKeys.Command, name, Meters.TagKeys.Outcome, outcome))
      .increment()

  /** Counts a lifecycle command that failed outright — a refused seek, a drain that threw. */
  def commandFailed(name: String): Unit =
    registry
      .counter(
        Meters.ConsumeLifecycle,
        Tags.of(Meters.TagKeys.Command, name, Meters.TagKeys.Outcome, Meters.Outcomes.Failure)
      )
      .increment()

  private def gauge(name: String, state: SupervisorMetrics.Reading): Unit =
    val _ = Gauge.builder(name, state, (reading: SupervisorMetrics.Reading) => reading.value).register(registry)

/** The supervisor's readings, taken together on the probe's tick.
  *
  * **This type exists so that a registered gauge cannot go unwritten.** `dlq.depth` shipped registered and with no
  * writer at all: `/metrics` reported a flat `0` while the DLQ filled, the dashboard panel stayed at zero, and
  * `docs/operations.md` told the operator that gauge was how you check whether a fix worked. Nothing in the code said
  * the wiring was missing, because a gauge with no writer looks exactly like a gauge reporting good news. Both readings
  * now live behind one method with one caller, and `SupervisorMetricsSuite` asserts that a single [[tick]] leaves no
  * registered gauge unread — which is the assertion whose absence let that ship.
  *
  * **Each reading is taken independently and neither can take the other down.** They come from different systems and
  * the moment one of them fails is precisely the moment somebody is reading the other.
  *
  * @param budget
  *   the bound on [[ConsumerSupervisor.status]], and **it has to be able to contain the calls it wraps**. `status` is a
  *   checkpoint read followed by three sequential admin round trips, each already bounded by the admin request timeout;
  *   a budget of one request timeout expires before the call it wraps can possibly return, so under a slow broker every
  *   tick threw and both gauges froze — the failure the poller was introduced to prevent, reintroduced one layer up.
  */
final class SupervisorProbe(
  metrics: SupervisorMetrics,
  status: () => Future[ConsumerStatus],
  dlqDepth: () => Long,
  budget: FiniteDuration
) extends StrictLogging:

  def tick(): Unit =
    reading("the consumer status")(metrics.observe(Await.result(status(), budget)))
    reading("the dead-letter depth")(metrics.observeDlq(dlqDepth()))

  /** Logged and dropped, but not hidden: the gauge this reading feeds goes stale and starts reporting `NaN`, so a
    * reading that has stopped arriving is distinguishable from one that keeps arriving as zero.
    */
  private def reading(what: String)(take: => Unit): Unit =
    try take
    catch
      case NonFatal(error) => logger.warn(s"$what could not be read: ${RecordDecoder.describe(error)}")

object SupervisorMetrics:

  /** The staleness window when nobody chooses one. Three minutes is nine of the default 20-second probe intervals —
    * long enough that a slow tick is not a gap, short enough that an operator watching an incident is not reading a
    * number from before it started.
    */
  val DefaultFreshFor: FiniteDuration = 3.minutes

  /** One gauge's value together with when it was taken.
    *
    * Mutable state read at scrape time, which is Micrometer's model; the [[AtomicReference]] holds value and timestamp
    * together so a scrape cannot observe a fresh timestamp beside a stale value.
    */
  final private[cobalt] class Reading(freshFor: FiniteDuration, nanoTime: () => Long):

    private val taken = AtomicReference[Option[(Long, Long)]](None)

    def set(value: Long): Unit = taken.set(Some((value, nanoTime())))

    /** The reading, or `NaN` if it was never taken or is older than `freshFor`. */
    def value: Double = taken.get() match
      case Some((reading, at)) if nanoTime() - at <= freshFor.toNanos => reading.toDouble
      case _                                                          => Double.NaN

  /** The largest gap between Kafka's committed offset and the externalised checkpoint, across partitions.
    *
    * **Zero is the only healthy value, and this is the number `events.consumer_checkpoint` exists to let you watch.**
    * The offset and the rows it accounts for are written in one transaction, so a persistent gap means one of the two
    * commits is failing: the checkpoint ahead of Kafka is events that will be replayed on the next start, Kafka ahead
    * of the checkpoint is events whose durability nobody can prove. Both are silent everywhere else in the system.
    *
    * A partition where either side is unknown contributes nothing rather than contributing its full offset. A group
    * that has committed but never checkpointed — which is every group for the first few seconds after a deploy — would
    * otherwise report a divergence the size of the whole log.
    */
  def divergenceOf(status: ConsumerStatus): Long =
    status.positions
      .flatMap: position =>
        for
          committed <- position.committed
          stored <- position.stored
        yield math.abs(committed - stored)
      .maxOption
      .getOrElse(0L)
