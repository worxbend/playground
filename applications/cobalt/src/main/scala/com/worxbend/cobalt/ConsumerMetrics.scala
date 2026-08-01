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
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit

/** cobalt's domain metrics, expressed in the shared vocabulary of `modules/observability`.
  *
  * **No meter name or tag key is invented here.** ADR §7.1 makes a single taxonomy across three services the
  * enforceable rule, because one Grafana dashboard has to work everywhere. This class is a typed façade over [[Meters]]
  * whose value is that each meter's tag set is fixed in exactly one place — Micrometer happily registers the same name
  * twice with different tags, and Prometheus then renders two unrelated series.
  *
  * **`persisted` and `duplicate` deliberately do not partition the batch.** The idempotent insert reports one number,
  * "rows the database actually wrote", and there is no way to attribute the shortfall to a particular `type` from a
  * single `ON CONFLICT DO NOTHING` batch. So [[persisted]] counts records the database took responsibility for — which
  * is every record in a successful batch, new or already present — tagged by type, and [[duplicates]] separately counts
  * the shortfall untagged. Inventing a per-type split would produce a number that looks precise and is not.
  *
  * **`-Werror` trap (ADR §7.4).** Every Micrometer builder returns `this` and `MeterRegistry#counter` returns the
  * meter, so a bare registration line trips `-Wnonunit-statement`. Every call below is part of an expression.
  */
final class ConsumerMetrics(registry: MeterRegistry):

  /** Records durably stored, tagged by CloudEvents `type` — see the class Scaladoc for what "stored" means here. */
  def persisted(eventType: String, count: Long): Unit =
    if count > 0 then
      registry
        .counter(Meters.ConsumePersisted, Tags.of(Meters.TagKeys.EventType, eventType))
        .increment(count.toDouble)

  /** Records the idempotent insert recognised as already present. Non-zero is normal for an at-least-once pipeline. */
  def duplicates(count: Long): Unit =
    if count > 0 then registry.counter(Meters.ConsumeDuplicate).increment(count.toDouble)

  /** A record was routed to the DLQ. `reason` comes from [[Meters.Reasons]] or from a `DecodeFailure`'s own bounded
    * reason tag, never from an exception message — the latter is unbounded and would blow up the label cardinality.
    */
  def poison(reason: String): Unit =
    registry.counter(Meters.ConsumePoison, Tags.of(Meters.TagKeys.Reason, reason)).increment()

  /** How many records one `groupedWithin` batch carried. Small batches under sustained load mean the consumer is
    * starved rather than saturated, a distinction throughput alone cannot make.
    */
  def batchSize(size: Int): Unit =
    registry.summary(Meters.ConsumeBatchSize).record(size.toDouble)

  /** How long decoding one record from Kafka bytes to a domain envelope took.
    *
    * Separate from [[batchWrite]] so a slow consumer can be *attributed*. Decode is CPU on the stream's own thread; the
    * batch write is a blocking round trip on a pool. When throughput drops, exactly one of the two moved, and without
    * both numbers the answer is a guess.
    */
  def decoded(elapsedNanos: Long): Unit =
    registry.timer(Meters.ConsumeDecodeDuration).record(elapsedNanos, TimeUnit.NANOSECONDS)

  /** Wall-clock latency of one batch insert, tagged by outcome.
    *
    * A timer and not a counter for the same reason `kafka.produce.latency` is one: a rising median is database
    * pressure, a rising p99 over a flat median is one partition or one oversized payload.
    */
  def batchWrite(outcome: String, elapsedNanos: Long): Unit =
    registry
      .timer(Meters.ConsumeBatchLatency, Tags.of(Meters.TagKeys.Outcome, outcome))
      .record(elapsedNanos, TimeUnit.NANOSECONDS)
