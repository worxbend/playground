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

package io.kzonix.wolfram

import java.util.concurrent.TimeUnit

import io.kzonix.eventing.ContentMode
import io.kzonix.kernel.event.Envelope
import io.kzonix.observability.Meters
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags

/** wolfram's domain metrics, expressed only in the shared vocabulary of `modules/observability`.
  *
  * **No meter name or tag value is invented here.** ADR §7.1 makes that the enforceable rule: three services, one
  * Grafana dashboard, so the names live in [[Meters]] and this class is a typed façade over them. The façade earns its
  * keep by making the *tags* impossible to get wrong — `ingest.events.received` is tagged `(type, mode)` in exactly one
  * place, so a second call site cannot register the same meter with a different tag set and split the timeseries in
  * two (which Micrometer permits and Prometheus renders as two unrelated series).
  *
  * **Broker errors are not a separate meter.** They are the `outcome=failure` count of
  * [[Meters.KafkaProduceLatency]], which is a `Timer` and therefore already publishes a count per tag combination.
  * Adding a `kafka.produce.errors` counter would mean the same event incremented two meters that must then be kept
  * consistent, and would put a name in wolfram's exposition that cobalt's dashboard does not know.
  *
  * **`-Werror` trap (ADR §7.4).** `Counter#increment` and `Timer#record` return `Unit`, but `MeterRegistry#counter`
  * returns the meter, so a bare `registry.counter(...)` line would trip `-Wnonunit-statement`. Every builder call below
  * is therefore part of an expression.
  */
final class IngestMetrics(registry: MeterRegistry):

  /** An event passed validation and was published. Incremented *after* the broker acknowledged, so this counter counts
    * durable events and not attempts — `http.server.requests` already counts attempts.
    */
  def accepted(envelope: Envelope, mode: ContentMode): Unit =
    registry
      .counter(
        Meters.IngestReceived,
        Tags.of(Meters.TagKeys.EventType, envelope.eventType, Meters.TagKeys.Mode, IngestMetrics.modeTag(mode))
      )
      .increment()

  /** An event was refused. Tagged with the rejection's `reason`, which [[Rejection]] guarantees comes from
    * [[Meters.Reasons]] — the tag is closed-set by construction rather than by review.
    */
  def rejected(rejection: Rejection): Unit =
    registry.counter(Meters.IngestRejected, Tags.of(Meters.TagKeys.Reason, rejection.reason)).increment()

  /** Broker acknowledgement latency for one produce. The p99 is the signal this exists for (see [[Meters]]): a rising
    * median is broker pressure, a rising p99 over a flat median is one slow partition leader.
    */
  def published(topic: String, outcome: String, elapsedNanos: Long): Unit =
    registry
      .timer(Meters.KafkaProduceLatency, Tags.of(Meters.TagKeys.Topic, topic, Meters.TagKeys.Outcome, outcome))
      .record(elapsedNanos, TimeUnit.NANOSECONDS)

object IngestMetrics:

  /** The closed tag value for a content mode. A `match` and not `toString`, so renaming the enum cannot silently
    * rename a Prometheus label.
    */
  def modeTag(mode: ContentMode): String = mode match
    case ContentMode.Binary     => Meters.Modes.Binary
    case ContentMode.Structured => Meters.Modes.Structured
