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

import io.kzonix.observability.Meters

/** Why an event was not accepted — a **value**, never an exception.
  *
  * The reason this is an ADT and not a thrown `IllegalArgumentException` is that every rejection has to be three things
  * at once and all three have to agree: an HTTP status, a metric tag, and a sentence a client can act on. An exception
  * carries only the sentence, so the status and the tag get re-derived at the catch site — and drift the moment a
  * fourth rejection kind is added. Here the three live on one case and the compiler checks the match.
  *
  * **`reason` is drawn from [[Meters.Reasons]] and nothing else.** That closed set is the shared vocabulary of ADR §7.1
  * and it deliberately has fewer values than this enum has cases: `Malformed` and `ImplausibleTime` are different
  * sentences for a client but the same actionable category for an operator, and minting a per-case tag value would
  * fork the `ingest.events.rejected` dashboard away from cobalt's `consume.records.poison`.
  *
  * `BrokerUnavailable` is tagged [[Meters.Reasons.Unpersistable]] for the same reason: it is the closest member of the
  * closed set (the event could not be made durable), and adding a `broker-unavailable` value here would mean adding it
  * to `modules/observability`, i.e. changing the vocabulary of all three services to describe a condition only one of
  * them can be in. Its rate is separately visible as the `outcome=failure` count of `kafka.produce.latency`.
  */
enum Rejection(val reason: String, val detail: String):

  /** The bytes were not a CloudEvent at all: not JSON, not an object, no usable content mode. */
  case Malformed(cause: String) extends Rejection(Meters.Reasons.Malformed, cause)

  /** Structurally a CloudEvent, but a required context attribute was missing, ill-typed or invalid. */
  case InvalidAttributes(cause: String) extends Rejection(Meters.Reasons.InvalidAttributes, cause)

  /** `time` was absent, or outside the plausibility window — see [[TimeClamp]] for why that is fatal rather than
    * repairable.
    */
  case ImplausibleTime(cause: String) extends Rejection(Meters.Reasons.InvalidAttributes, cause)

  /** The body exceeded `wolfram.ingest.max-event-bytes`, or the batch exceeded `max-batch-events`. */
  case TooLarge(limit: Long, actual: Long, unit: String)
      extends Rejection(Meters.Reasons.TooLarge, s"$actual $unit exceeds the limit of $limit")

  /** The broker did not acknowledge the record, or ingestion shed load to avoid queueing in front of it. */
  case BrokerUnavailable(cause: String) extends Rejection(Meters.Reasons.Unpersistable, cause)

  /** One human-readable line, in the shape `modules/eventing`'s `DecodeFailure.message` uses, so a wolfram log line and
    * a cobalt DLQ record read the same way.
    */
  def message: String = s"$reason: $detail"
