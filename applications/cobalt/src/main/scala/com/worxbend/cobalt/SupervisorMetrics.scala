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
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.atomic.AtomicLong

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
  * left there. Micrometer's gauge holds a weak reference to the state object and reads it at scrape time, so an
  * `AtomicLong` updated by the same poller that already computes lag is both cheaper and more honest: it converges on
  * the real value within one interval no matter what happened before.
  */
final class SupervisorMetrics(registry: MeterRegistry):

  private val running = AtomicLong(0L)
  private val divergence = AtomicLong(0L)
  private val dlqDepth = AtomicLong(0L)

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

  private def gauge(name: String, state: AtomicLong): Unit =
    val _ = Gauge.builder(name, state, (value: AtomicLong) => value.get().toDouble).register(registry)

object SupervisorMetrics:

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
