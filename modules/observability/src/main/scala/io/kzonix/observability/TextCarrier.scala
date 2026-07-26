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

package io.kzonix.observability

/** A minimal, transport-agnostic view of a string-keyed header set, used to carry W3C trace context across a process
  * boundary.
  *
  * This exists so that trace propagation lives in one place while the *transports* stay in the modules that own them.
  * `modules/eventing` binds this to `org.apache.kafka.common.header.Headers`, a Play filter binds it to request
  * headers, and neither obligation leaks a Kafka or Play dependency into this module (ADR §2 — `observability` has no
  * Kafka on its classpath and must not acquire one).
  *
  * **Why `put` returns `C` rather than being `Unit`.** The two carriers that matter have opposite mutability: a Scala
  * `Map` is persistent and an update produces a new value, while Kafka's `Headers` is mutable and `add` returns
  * `this`. A returning signature is the only one both can implement honestly — a `Unit` signature would force the Map
  * instance to be secretly mutable, and a persistent-only signature would force needless copying of Kafka headers.
  * Implementations are free to return the same instance.
  *
  * Only the three operations OpenTelemetry's `TextMapPropagator` actually needs are here. Resist growing it: every
  * method added is a method each transport binding must implement correctly for propagation to work at all.
  */
trait TextCarrier[C]:

  /** Every key present. Duplicates may be collapsed — trace context fields are single-valued. */
  def keys(carrier: C): Iterable[String]

  /** The value for `key`, or `None`.
    *
    * Where a transport permits repeated keys (Kafka does), return the **last** one: that is what a proxy or interceptor
    * appending a fresh `traceparent` intends.
    */
  def get(carrier: C, key: String): Option[String]

  /** `carrier` with `key` set to `value`, which may be `carrier` itself if the type is mutable. */
  def put(carrier: C, key: String, value: String): C

object TextCarrier:

  /** Summons the instance for `C`. */
  def apply[C](using carrier: TextCarrier[C]): TextCarrier[C] = carrier

  /** The reference binding, and the one every other binding is tested against.
    *
    * A `Map[String, String]` is also the right carrier at boundaries that have no header type of their own — a job
    * payload, a retry record, a test — so this is not merely a test double.
    */
  given stringMap: TextCarrier[Map[String, String]] with
    def keys(carrier: Map[String, String]): Iterable[String] = carrier.keys
    def get(carrier: Map[String, String], key: String): Option[String] = carrier.get(key)
    def put(carrier: Map[String, String], key: String, value: String): Map[String, String] =
      carrier.updated(key, value)
