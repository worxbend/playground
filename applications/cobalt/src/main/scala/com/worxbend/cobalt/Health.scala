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

import java.util.concurrent.atomic.AtomicReference

/** The last thing observed about one dependency, as a value readiness can read without blocking.
  *
  * **Readiness must never probe inline.** A `/health/ready` handler that opens a JDBC connection or asks a broker for
  * metadata turns the readiness endpoint into a load generator against the very dependency that is struggling, and its
  * own latency into the thing that fails the probe. Instead a background poller writes here and the handler reads a
  * field — so the answer is at most one poll interval stale, and stale-but-instant beats fresh-but-hanging every time.
  *
  * Starts in the *down* state deliberately: a process that has not yet proved it can reach its dependencies is not
  * ready, and optimistic initialisation is how a replica joins the load balancer seconds before it starts erroring.
  */
final class DependencyHealth(val name: String):

  private val failure: AtomicReference[Option[String]] = AtomicReference(Some("not probed yet"))

  def reachable: Boolean = failure.get().isEmpty

  /** The reason it is unreachable, or `None` when it is fine. Surfaced in the readiness body so an operator gets the
    * cause from the probe rather than from correlating logs.
    */
  def reason: Option[String] = failure.get()

  def up(): Unit = failure.set(None)

  def down(cause: String): Unit = failure.set(Some(cause))

/** Everything readiness consults.
  *
  * Exactly two dependencies, because cobalt has exactly two: without Kafka there is nothing to consume, without
  * PostgreSQL there is nowhere to put it. Anything else — the DLQ producer, the admin client — shares a fate with one
  * of these two and adding it would only make the probe flap for reasons that do not change the answer.
  */
final case class HealthChecks(broker: DependencyHealth, database: DependencyHealth):

  def ready: Boolean = broker.reachable && database.reachable

  def dependencies: Vector[DependencyHealth] = Vector(broker, database)

object HealthChecks:

  def create(): HealthChecks = HealthChecks(DependencyHealth("kafka"), DependencyHealth("postgresql"))
