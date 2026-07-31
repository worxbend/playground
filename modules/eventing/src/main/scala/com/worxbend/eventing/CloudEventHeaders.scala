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

package com.worxbend.eventing

import java.nio.charset.StandardCharsets.UTF_8
import org.apache.kafka.common.header.Headers
import scala.jdk.CollectionConverters.*

/** The Kafka header vocabulary of the CloudEvents protocol binding, and the three primitive operations over
  * `org.apache.kafka.common.header.Headers` that everything else in this module is built from.
  *
  * **Why these names are re-declared here rather than imported from the SDK.** `io.cloudevents.kafka.impl.KafkaHeaders`
  * holds the same constants, but it lives in an `impl` package and `CE_PREFIX` is `protected`, so half of the
  * vocabulary is unreachable from Scala anyway. Re-declaring them is only safe if the two definitions cannot drift
  * apart silently — so `CloudEventHeadersSuite` asserts equality against the SDK's own public constants. That test is
  * the contract; deleting it makes these strings a guess.
  *
  * **Values are UTF-8 text, not arbitrary bytes.** The binding specifies every `ce_*` header and `content-type` as a
  * string, so decoding a header value with UTF-8 is lossless for anything this system — or any conformant producer —
  * emits. A third-party header carrying raw binary will still decode without throwing (UTF-8 substitutes replacement
  * characters) and is recorded for diagnosis only; the record's *value* bytes, which is what a DLQ replay actually
  * needs, are always kept verbatim.
  */
object CloudEventHeaders:

  /** The prefix the binding puts on every context attribute except `datacontenttype`. */
  val Prefix: String = "ce_"

  /** `datacontenttype` is the one attribute that does **not** get the `ce_` prefix: it maps onto the transport's own
    * content-type header so that tooling which knows nothing about CloudEvents still sees the right media type.
    */
  val ContentType: String = "content-type"

  /** Presence of this header is the definition of "this record is in binary content mode". */
  val SpecVersion: String = Prefix + "specversion"

  /** The header name carrying context attribute `name`. */
  def attribute(name: String): String = Prefix + name

  /** The attribute name behind a header name, when the header is a CloudEvents context attribute at all. */
  def attributeOf(header: String): Option[String] =
    if header.startsWith(Prefix) then Some(header.drop(Prefix.length)) else None

  /** The **last** value for `key`, decoded as UTF-8.
    *
    * Last rather than first: Kafka permits repeated header keys, and a proxy or interceptor that appends a fresh
    * `traceparent` intends its value to win. Taking the first would silently pin the trace to a stale parent.
    */
  def get(carrier: Headers, key: String): Option[String] =
    Option(carrier.lastHeader(key)).flatMap(header => Option(header.value)).map(bytes => String(bytes, UTF_8))

  /** Every distinct key present. */
  def keys(carrier: Headers): Iterable[String] =
    carrier.iterator.asScala.map(_.key).toVector.distinct

  /** Sets `key` to `value`, *replacing* any existing occurrences, and returns the same (mutable) `carrier`.
    *
    * Replacing rather than appending because the alternative accumulates: a record retried through a producer that
    * re-injects trace context would grow one `traceparent` header per attempt, and while readers take the last value,
    * the record still bloats and `kcat` output becomes unreadable.
    *
    * Throws `IllegalStateException` on the read-only `Headers` of a `ConsumerRecord` — deliberately, because mutating a
    * consumed record's headers and expecting the change to be visible anywhere is always a bug.
    */
  def put(carrier: Headers, key: String, value: String): Headers =
    val _ = carrier.remove(key)
    carrier.add(key, value.getBytes(UTF_8))

  /** A snapshot of the headers as a map, last value winning per key. Used to record the context of a poison record in
    * the dead-letter payload; see [[DeadLetter]] for why lossy-but-readable is the right trade there.
    */
  def toMap(carrier: Headers): Map[String, String] =
    carrier.iterator.asScala
      .map(header => header.key -> Option(header.value).map(bytes => String(bytes, UTF_8)).getOrElse(""))
      .toMap
