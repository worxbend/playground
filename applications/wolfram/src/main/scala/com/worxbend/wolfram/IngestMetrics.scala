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

package com.worxbend.wolfram

import com.worxbend.eventing.ContentMode
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.Observation
import com.worxbend.observability.Meters
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit

/** wolfram's domain metrics, expressed only in the shared vocabulary of `modules/observability`.
  *
  * **No meter name or tag value is invented here.** ADR §7.1 makes that the enforceable rule: three services, one
  * Grafana dashboard, so the names live in [[Meters]] and this class is a typed façade over them. The façade earns its
  * keep by making the *tags* impossible to get wrong — `ingest.events.received` is tagged `(type, mode)` in exactly one
  * place, so a second call site cannot register the same meter with a different tag set and split the timeseries in two
  * (which Micrometer permits and Prometheus renders as two unrelated series).
  *
  * **Broker errors are not a separate meter.** They are the `outcome=failure` count of [[Meters.KafkaProduceLatency]],
  * which is a `Timer` and therefore already publishes a count per tag combination. Adding a `kafka.produce.errors`
  * counter would mean the same event incremented two meters that must then be kept consistent, and would put a name in
  * wolfram's exposition that cobalt's dashboard does not know.
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

  /** Counts an accepted event whose `type` this deployment has no decoder for, or whose payload did not fit the decoder
    * its `type` selected.
    *
    * **Why the front door and not the consumer.** ADR §4.2 makes refinement a separate, total step from envelope
    * decoding precisely so an unheard-of event still reaches Postgres — which means nothing downstream *fails*, and the
    * only trace a producer-ahead-of-its-consumer deploy leaves is this counter. Counting it here rather than in cobalt
    * puts the signal at the earliest point the event exists and attributes it to the producer's request, minutes before
    * the same event would have shown up on the consume side.
    *
    * **Why it does not reject.** A non-zero rate during a rolling deploy is expected and benign; a *sustained* one
    * means a producer shipped before its consumer. Both are operational readings, not client errors, so this is a
    * counter and not a [[Rejection]] — refusing the event would lose data that ADR §4.2 exists to keep.
    *
    * The `reason` distinguishes the two cases that need different responses: [[Meters.Reasons.UnknownType]] is "deploy
    * the consumer", [[Meters.Reasons.InvalidPayload]] is "the schema changed under a decoder that is still registered",
    * which is the more alarming of the two.
    */
  def observed(envelope: Envelope): Unit =
    Observation.from(envelope) match
      case Observation.Unrecognised(eventType, _, reason) =>
        val why = reason.fold(Meters.Reasons.UnknownType)(_ => Meters.Reasons.InvalidPayload)
        registry
          .counter(Meters.EventUnrecognised, Tags.of(Meters.TagKeys.EventType, eventType, Meters.TagKeys.Reason, why))
          .increment()
      case _ => ()

  /** The shape of what a producer actually sent: body size, and how far its clock is from ours.
    *
    * Recorded on the **accepted** path only. A rejected request's size is already visible as
    * `ingest.events.rejected{reason=too-large}`, and mixing refused bodies into the size distribution would make the
    * p99 a statement about attackers rather than about producers.
    *
    * The skew's sign is a tag rather than a negative value: Micrometer's summaries reject negatives, and folding the
    * sign away would make a fleet uniformly ten seconds fast look identical to one where half the devices are fast and
    * half are slow — which need different responses.
    */
  def shape(mode: ContentMode, bodyBytes: Int, occurredAt: java.time.OffsetDateTime, now: java.time.Instant): Unit =
    registry
      .summary(Meters.IngestPayloadBytes, Tags.of(Meters.TagKeys.Mode, IngestMetrics.modeTag(mode)))
      .record(bodyBytes.toDouble)
    val skewSeconds = java.time.Duration.between(now, occurredAt.toInstant).getSeconds
    val direction = if skewSeconds >= 0 then Meters.Skews.Future else Meters.Skews.Past
    registry
      .summary(Meters.IngestTimeSkew, Tags.of(Meters.TagKeys.Direction, direction))
      .record(math.abs(skewSeconds).toDouble)

  /** How many elements one `events:batchCreate` document carried.
    *
    * Distinct from the byte size because a batch of one large event and a batch of two hundred small ones are the same
    * bytes and completely different work — the second is two hundred sequential produces.
    */
  def batch(elements: Int): Unit =
    registry.summary(Meters.IngestBatchEvents).record(elements.toDouble)

  /** Broker acknowledgement latency for one produce. The p99 is the signal this exists for (see [[Meters]]): a rising
    * median is broker pressure, a rising p99 over a flat median is one slow partition leader.
    */
  def published(topic: String, outcome: String, elapsedNanos: Long): Unit =
    registry
      .timer(Meters.KafkaProduceLatency, Tags.of(Meters.TagKeys.Topic, topic, Meters.TagKeys.Outcome, outcome))
      .record(elapsedNanos, TimeUnit.NANOSECONDS)

object IngestMetrics:

  /** The closed tag value for a content mode. A `match` and not `toString`, so renaming the enum cannot silently rename
    * a Prometheus label.
    */
  def modeTag(mode: ContentMode): String = mode match
    case ContentMode.Binary     => Meters.Modes.Binary
    case ContentMode.Structured => Meters.Modes.Structured
