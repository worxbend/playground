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

import io.kzonix.observability.Meters
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit

/** cobalt's domain metrics, expressed in the shared vocabulary of `modules/observability`.
  *
  * **No meter name or tag key is invented here** except the one noted below: ADR §7.1 makes a single taxonomy across
  * three services the enforceable rule, because one Grafana dashboard has to work everywhere. This class is a typed
  * façade over [[Meters]] whose value is that each meter's tag set is fixed in exactly one place — Micrometer happily
  * registers the same name twice with different tags, and Prometheus then renders two unrelated series.
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

  /** Wall-clock latency of one batch insert, tagged by outcome.
    *
    * A timer and not a counter for the same reason `kafka.produce.latency` is one: a rising median is database
    * pressure, a rising p99 over a flat median is one partition or one oversized payload.
    */
  def batchWrite(outcome: String, elapsedNanos: Long): Unit =
    registry
      .timer(ConsumerMetrics.BatchWriteLatency, Tags.of(Meters.TagKeys.Outcome, outcome))
      .record(elapsedNanos, TimeUnit.NANOSECONDS)

object ConsumerMetrics:

  /** Batch-insert latency.
    *
    * **The one name in this service that is not already in [[Meters]].** ADR §7.1's minimum set names
    * `consume.batch.size` but no companion timer, and `modules/observability` is a finished module this build does not
    * edit. It is spelled here as a single constant, in the same `consume.` family and tagged only with
    * [[Meters.TagKeys.Outcome]], so promoting it into `Meters` later is a move rather than a rename. Flagged as a
    * deviation rather than left as a surprise.
    */
  val BatchWriteLatency: String = "consume.batch.latency"
