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

import com.worxbend.kernel.event.Envelope
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** What the broker said when it accepted a record.
  *
  * Returned to the client because the triple `(topic, partition, offset)` is the only receipt that lets an operator
  * find the event again with `kcat` when someone asks "did my event arrive". `key` is included because it is the
  * partition key kernel computed, and a producer that expected its events on one partition can see immediately that
  * they are not.
  */
final case class PublishAck(topic: String, partition: Int, offset: Long, key: String)

/** The seam between ingestion and Kafka.
  *
  * A trait with one production implementation exists here for exactly one reason, and it is not "mockability in
  * principle": every endpoint test in this service needs to drive the *rejection* paths — 503 on an unreachable broker,
  * shed load under backpressure — and there is no way to make a real `KafkaProducer` fail on demand without a broker.
  * The alternative is testing the HTTP contract against a container, which moves the fast tier into the slow one.
  *
  * Failures are `Either` in the success channel of the `Future`, not a failed `Future`. A failed `Future` would make
  * "the broker is down" indistinguishable at the type level from "the code threw", and the two want different HTTP
  * statuses, different metrics and different log levels.
  */
trait EventPublisher extends AutoCloseable:

  /** Publishes one envelope, completing when the broker has acknowledged it (or refused to).
    *
    * The returned future completes with `Left` for every *expected* failure. It fails only for a bug.
    */
  def publish(envelope: Envelope): Future[Either[Rejection, PublishAck]]

  /** Whether the broker is currently believed reachable — the input to `/health/ready`. Cheap and non-blocking by
    * contract: a readiness probe that talks to Kafka on the request thread is a readiness probe that times out exactly
    * when the broker is slow, which is when a truthful answer matters most.
    */
  def brokerReachable: Boolean

/** The service's belief about broker reachability, and the only state `/health/ready` reads.
  *
  * **Why belief and not a probe per request.** Readiness is checked by a load balancer several times a second. Asking
  * Kafka each time would put a blocking metadata call on the probe path, so a broker that is *slow* would make
  * readiness *time out* — the probe would fail for a reason unrelated to the answer it was asking for. Instead the
  * evidence is collected where it already exists: every produce either succeeds or fails, and a periodic probe covers
  * the idle case. The probe reads a single volatile reference.
  *
  * **Why the initial state is not ready.** A process that has never reached the broker must not receive traffic, and
  * "no evidence" is not "good news". [[Unknown]] therefore fails readiness, and the composition root probes once
  * synchronously at boot so the first answer is real rather than pessimistic.
  *
  * Liveness deliberately does not consult this (ADR §7 / Kubernetes practice): a broker outage must not restart every
  * ingestion pod, which would turn a recoverable dependency failure into a crash loop that outlives it.
  */
final class BrokerHealth private (private val state: AtomicReference[BrokerHealth.Evidence]):

  /** Records the outcome of a real interaction with the broker. */
  def observe(reachable: Boolean, at: Instant, cause: Option[String] = None): Unit =
    state.set(if reachable then BrokerHealth.Evidence.Reachable(at) else BrokerHealth.Evidence.Unreachable(at, cause))

  /** The most recent evidence. Exposed so the readiness body can say *why*, which is the difference between a probe
    * that fails and a probe that is diagnosable.
    */
  def evidence: BrokerHealth.Evidence = state.get()

  /** Ready only on positive evidence. */
  def reachable: Boolean = evidence match
    case BrokerHealth.Evidence.Reachable(_)      => true
    case BrokerHealth.Evidence.Unknown           => false
    case BrokerHealth.Evidence.Unreachable(_, _) => false

object BrokerHealth:

  /** What is known about the broker, and when it became known. */
  enum Evidence:
    case Unknown
    case Reachable(at: Instant)
    case Unreachable(at: Instant, cause: Option[String])

  def apply(): BrokerHealth = new BrokerHealth(AtomicReference(Evidence.Unknown))
