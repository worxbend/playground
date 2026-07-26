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

package io.kzonix.eventing

/** Why a Kafka record could not be turned into an [[io.kzonix.kernel.event.Envelope]].
  *
  * **This is a value, never an exception.** ADR §4.3 explains the failure it exists to prevent: a throwing
  * deserializer throws inside `KafkaConsumer.poll`, *before* the stream connector ever sees the record. The stream
  * dies, the offset is never committed, and the restart replays the same poison record forever — an outage that looks
  * like a crash loop and whose cause is one malformed byte. Decoding therefore returns `Either`, the bad record
  * becomes a [[DeadLetter]], and the offset is still committed.
  *
  * **`reason` is separate from `detail` because one is a metric tag and the other is not.** ADR §7 specifies
  * `consume.records.poison{reason}`; tagging that counter with a parser message would mint a Prometheus timeseries per
  * distinct malformed record, which is the unbounded-cardinality failure §7.1 calls the most likely way to take
  * Prometheus down. `reason` is drawn from this closed set of four; `detail` goes to the log line and the DLQ payload,
  * where cardinality costs nothing.
  */
enum DecodeFailure(val reason: String, val detail: String):

  /** Neither a structured-mode `content-type` nor a `ce_specversion` header: the record is not a CloudEvent at all.
    * Most often a producer writing to the wrong topic, which is worth distinguishing from a corrupt event.
    */
  case UnknownEncoding(cause: String) extends DecodeFailure("unknown-encoding", cause)

  /** Structured mode: the value was not JSON, or was JSON that is not a CloudEvent. */
  case MalformedStructured(cause: String) extends DecodeFailure("malformed-structured", cause)

  /** Binary mode: the headers do not form a readable CloudEvent. */
  case MalformedBinary(cause: String) extends DecodeFailure("malformed-binary", cause)

  /** A well-formed CloudEvent this build's domain model rejects — an unsupported `specversion`, an `id` that is blank,
    * a `source` that is not a URI-reference. Distinct from the "malformed" cases because it means the *producer* and
    * this build disagree about validity, not that the bytes are damaged.
    */
  case Unconvertible(cause: String) extends DecodeFailure("unconvertible", cause)

  /** One line suitable for a log statement. */
  def message: String = s"$reason: $detail"
