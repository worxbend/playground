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

package io.kzonix.kernel.event

/** Kafka topic names and the key functions that go with them (ADR §4.3).
  *
  * These live in the kernel rather than in each service's configuration because a typo in a topic name produces a
  * consumer that starts cleanly and receives nothing — the least debuggable failure in the system. Three services
  * reading one constant cannot disagree.
  */
object Topics:

  /** Primary stream. Binary content mode, so brokers, SMTs and `kcat` can route on `ce_type`/`ce_source` headers
    * without deserialising the value, and an unknown payload is never re-encoded.
    */
  val CloudEvents: String = "events.cloudevents.v1"

  /** Poison queue. Structured content mode on purpose: a DLQ record must be one self-contained blob that a human can
    * read with `kcat` without reconstructing the headers.
    */
  val CloudEventsDlq: String = "events.cloudevents.v1.dlq"

  /** Chosen generously up front because expansion is breaking: adding partitions rehashes every key and interleaves a
    * device's timeline across the transition. Documented as a one-way door in ADR §4.3.
    */
  val CloudEventsPartitions: Int = 12

  val CloudEventsDlqPartitions: Int = 3

  /** DLQ key. Keying on the origin coordinates means a replayed poison record *overwrites* its predecessor under log
    * compaction instead of accumulating a new copy on every retry.
    */
  def dlqKey(topic: String, partition: Int, offset: Long): String = s"$topic/$partition/$offset"
