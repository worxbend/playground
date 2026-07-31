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

/** The two replay meters, in the shared vocabulary of `modules/observability`.
  *
  * Same rules as [[ConsumerMetrics]] and for the same reason: no name and no tag key is invented here, every tag value
  * comes from [[Meters.Outcomes]], and nothing derived from a request — a ref, a topic, a skip reason — is ever a tag.
  * The DLQ is where the *unbounded* content in this system ends up, so it is the last place to relax that rule.
  *
  * **A counter per record and a counter per operation, not one or the other.** One operation replaying two hundred
  * records and two hundred operations replaying one each produce the same record count and describe very different
  * situations: the first is a recovery, the second is somebody in a loop. Only the operation counter distinguishes
  * them, and only the record counter says how much went back on the topic.
  *
  * **`-Werror` trap (ADR §7.4).** `MeterRegistry#counter` returns the meter, so every call below is part of an
  * expression rather than a bare statement.
  */
final class ReplayMetrics(registry: MeterRegistry):

  /** One replay request reached a decision. `outcome` is [[Meters.Outcomes.Skipped]] for a dry run — see
    * [[Meters.DlqReplayOperations]] for why "an operator is looking" must not share a series with "an operator acted".
    */
  def operation(outcome: String): Unit =
    registry.counter(Meters.DlqReplayOperations, Tags.of(Meters.TagKeys.Outcome, outcome)).increment()

  /** `count` dead letters reached `outcome`: published, refused by the broker, or declined by the plan. */
  def records(outcome: String, count: Int): Unit =
    if count > 0 then
      registry.counter(Meters.DlqReplayRecords, Tags.of(Meters.TagKeys.Outcome, outcome)).increment(count.toDouble)
