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

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.concurrent.atomic.AtomicLong
import munit.FunSuite
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** The supervisor's gauges, and the tick that has to write all of them.
  *
  * Two defects are pinned here, and both were invisible from `/metrics`: a gauge registered with no writer anywhere in
  * main sources, and a composite reading whose timeout was smaller than the calls it wrapped, so every tick threw and
  * the gauges reported their last value forever. Neither shows up as a failure — one reads as zero, the other as a flat
  * line — which is why the assertions are about *which gauges were written* and *what an unwritten one says*.
  */
final class SupervisorMetricsSuite extends FunSuite:

  private val clock = AtomicLong(0L)

  private def metrics(registry: SimpleMeterRegistry, freshFor: scala.concurrent.duration.FiniteDuration = 1.minute) =
    SupervisorMetrics(registry, freshFor, () => clock.get())

  private def gauges(registry: SimpleMeterRegistry): Map[String, Double] =
    registry.getMeters.asScala.toVector.collect { case gauge: Gauge => gauge.getId.getName -> gauge.value() }.toMap

  private def status(consuming: Boolean = true, divergence: Long = 0L): ConsumerStatus =
    ConsumerStatus(
      state = if consuming then RunState.Running else RunState.Paused,
      since = java.time.Instant.EPOCH,
      generation = 1,
      groupId = "g",
      topic = "t",
      consuming = consuming,
      lastError = None,
      restarts = 0,
      positions = List(PartitionPosition("t", 0, Some(100L), Some(100L - divergence), Some(100L), Some(0L))),
      totalLag = Some(0L)
    )

  test("one probe tick leaves no registered gauge unwritten"):
    // The regression that matters: `dlq.depth` was registered in the constructor and written by nothing, so it
    // reported 0.0 forever while the DLQ filled. A gauge with no writer is indistinguishable from a gauge reporting
    // good news, so the only defence is asserting that a tick reaches every one of them.
    val registry = SimpleMeterRegistry()
    val supervisor = metrics(registry)
    SupervisorProbe(supervisor, () => Future.successful(status()), () => 40_000L, 5.seconds).tick()
    val unwritten = gauges(registry).filter((_, value) => value.isNaN).keys.toVector.sorted
    assertEquals(unwritten, Vector.empty[String], "every gauge this class registers must be written by one tick")
    assertEquals(gauges(registry)(com.worxbend.observability.Meters.DlqDepth), 40_000.0)

  test("a gauge nobody has written yet reads NaN, not zero"):
    // Before the first probe lands there is no reading. Reporting 0 would say "the consumer is stopped and the DLQ is
    // empty", which is a specific and reassuring claim about a process that has not been asked yet.
    val registry = SimpleMeterRegistry()
    val _ = metrics(registry)
    assert(gauges(registry).values.forall(_.isNaN), gauges(registry).toString)

  test("a reading older than its freshness window stops being reported"):
    // A poller that has stopped answering must not leave `consume.running` pinned at 1 for the whole incident: that
    // is the number an operator reads to rule the consumer *out* as the cause.
    val registry = SimpleMeterRegistry()
    val supervisor = metrics(registry, freshFor = 30.seconds)
    clock.set(0L)
    supervisor.observe(status(consuming = true))
    assertEquals(gauges(registry)(com.worxbend.observability.Meters.ConsumeRunning), 1.0)
    clock.set(31.seconds.toNanos)
    assert(gauges(registry)(com.worxbend.observability.Meters.ConsumeRunning).isNaN)

  test("a status reading that blows its budget does not take the DLQ reading with it"):
    // The two come from different systems, and the moment one of them is failing is exactly the moment somebody is
    // reading the other. `Probes` wraps the whole tick in one `catch`, so without the split the first failure of the
    // tick would silently skip everything after it.
    val registry = SimpleMeterRegistry()
    val supervisor = metrics(registry)
    val never = Promise[ConsumerStatus]()
    SupervisorProbe(supervisor, () => never.future, () => 7L, 20.millis).tick()
    val written = gauges(registry)
    assert(written(com.worxbend.observability.Meters.ConsumeRunning).isNaN, "a timed-out status must not be reported")
    assertEquals(written(com.worxbend.observability.Meters.DlqDepth), 7.0)

  test("a DLQ read that throws does not take the status reading with it"):
    val registry = SimpleMeterRegistry()
    val supervisor = metrics(registry)
    SupervisorProbe(
      supervisor,
      () => Future.successful(status(consuming = false, divergence = 13L)),
      () => throw IllegalStateException("the broker is unreachable"),
      5.seconds
    ).tick()
    val written = gauges(registry)
    assertEquals(written(com.worxbend.observability.Meters.ConsumeRunning), 0.0)
    assertEquals(written(com.worxbend.observability.Meters.ConsumeCheckpointDivergence), 13.0)
    assert(written(com.worxbend.observability.Meters.DlqDepth).isNaN)
