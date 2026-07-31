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

package com.worxbend.ferrite.search

import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.pekko.actor.ActorSystem
import play.api.libs.concurrent.CustomExecutionContext

/** The dispatcher every blocking JDBC call in ferrite runs on.
  *
  * **Why not the default dispatcher.** Play's default dispatcher is a fork-join pool sized to the number of cores and
  * shared with request parsing, the Pekko HTTP server's own work and every non-blocking `map` in the application. A
  * blocking `getConnection`/`executeQuery` parked on it does not just occupy a thread, it removes a thread from the
  * pool that is supposed to be *accepting* requests — so a slow database stops the server answering `/health`, and the
  * symptom is a failing liveness probe rather than a slow search. Isolating the blocking work means a database stall
  * degrades exactly one feature.
  *
  * **Why not a virtual-thread executor.** ADR §0, decision 8 settles this: an unbounded executor over virtual threads
  * does not remove the queue, it relocates it from HikariCP — where it has a visible depth, a `connectionTimeout` and
  * therefore a load-shedding point — into an invisible one with neither. JEP 491 makes virtual threads *safe* for
  * blocking JDBC on JDK 25; it does not make them *preferable* where the bound is the resource you are protecting.
  *
  * **Why the size must equal `maximumPoolSize`.** More threads than connections means the surplus threads block inside
  * `getConnection` and the queue silently moves from the dispatcher (where it is configured and observable) to the pool
  * (where waiting shows up as a `connectionTimeout` exception, i.e. an error rather than backpressure). Fewer threads
  * than connections means connections that can never be used. `database.search-dispatcher.thread-pool-executor
  * .fixed-pool-size` in `application.conf` is therefore pinned to `database.read.maximum-pool-size`, and both are 8;
  * `DatabaseConfigSuite` in the persistence module owns the pool half of that pair.
  *
  * ferrite reads only (ADR §1) and never runs a migration, so there is exactly one of these — a write dispatcher would
  * be a pool of threads for work this service does not do.
  */
@Singleton
final class SearchExecutionContext @Inject() (system: ActorSystem)
    extends CustomExecutionContext(system, SearchExecutionContext.Name)

object SearchExecutionContext:

  /** The Pekko dispatcher id. Must match the block in `application.conf`; a typo here falls back to the default
    * dispatcher *silently*, which is precisely the failure this class exists to prevent, so it is spelled once.
    */
  val Name: String = "database.search-dispatcher"
