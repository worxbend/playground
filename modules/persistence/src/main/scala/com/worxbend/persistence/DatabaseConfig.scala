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

package com.worxbend.persistence

import pureconfig.ConfigReader
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scala.concurrent.duration.FiniteDuration

/** One connection pool's worth of settings.
  *
  * ADR §5 mandates **two** pools per service — a read-only search pool and an ingest write pool — so that a runaway
  * search cannot starve persistence and a burst of ingest cannot starve the UI. Two pools rather than one large one is
  * a load-shedding decision: a bounded pool is a queue with a visible depth and a timeout, and the whole point of ADR
  * §0's rejection of a virtual-thread executor is that unbounded concurrency moves the queue somewhere you cannot see
  * it or shed from it.
  *
  * @param maximumPoolSize
  *   also the size of the matching `CustomExecutionContext`. They must be equal: more threads than connections means
  *   threads blocked in `getConnection` (a timeout you did not choose), fewer means idle connections.
  * @param connectionInitSql
  *   run once per physical connection. This is where the search pool sets `statement_timeout`, which is the only
  *   mechanism that reliably kills a runaway query — a client-side `queryTimeout` leaves the backend running.
  * @param readOnly
  *   sets the session default. Cheap defence in depth: a repository accidentally wired to the search pool fails on the
  *   write rather than performing it.
  */
final case class PoolConfig(
  poolName: String,
  maximumPoolSize: Int,
  minimumIdle: Int,
  connectionTimeout: FiniteDuration,
  idleTimeout: FiniteDuration,
  maxLifetime: FiniteDuration,
  validationTimeout: FiniteDuration,
  readOnly: Boolean,
  connectionInitSql: Option[String]
)

object PoolConfig:

  /** Written out with `forProduct9` instead of `derives ConfigReader`.
    *
    * Two reasons, both practical. Derivation needs a wildcard given-import that `-Wunused:all` + `-Werror` is known to
    * flag spuriously (ADR §12.4), and it derives HOCON keys from field names by a convention nobody reading the
    * `reference.conf` can see. Here the key names are literally in the source next to the fields they fill.
    */
  given reader: ConfigReader[PoolConfig] =
    ConfigReader.forProduct9(
      "pool-name",
      "maximum-pool-size",
      "minimum-idle",
      "connection-timeout",
      "idle-timeout",
      "max-lifetime",
      "validation-timeout",
      "read-only",
      "connection-init-sql"
    )(PoolConfig.apply)

/** Everything needed to reach PostgreSQL, plus the two pools of ADR §5.
  *
  * The credentials live here rather than being baked into the JDBC URL so that a deployment can supply them from a
  * secret store without string-splicing a URL, and so that a password never appears in the pool name, the metric tags
  * or a connection-error message that quotes the URL.
  */
final case class DatabaseConfig(
  jdbcUrl: String,
  username: String,
  password: String,
  read: PoolConfig,
  write: PoolConfig
):

  /** Redacts the password.
    *
    * The class comment above promises the password "never appears in ... a connection-error message". A derived
    * `toString` broke that promise for every other route out of the process: one `s"…$config…"` in a log line, one
    * munit assertion message in CI output, one exception built from the value, and the database password is in a log
    * aggregator. Nothing rendered it today — every call site was checked — which is exactly why it was worth fixing
    * before something did.
    *
    * The username stays visible. It is not a secret, and an error that says which *user* could not connect is the
    * difference between a five-minute fix and an hour.
    */
  override def toString: String =
    s"DatabaseConfig($jdbcUrl, $username, <redacted>, $read, $write)"

object DatabaseConfig:

  /** The config namespace. Every service reads the same one, so a pool tuned in ferrite is tuned the same way in
    * cobalt, and a `reference.conf` in this module supplies defaults for both.
    */
  val Namespace: String = "database"

  given reader: ConfigReader[DatabaseConfig] =
    ConfigReader.forProduct5("jdbc-url", "username", "password", "read", "write")(DatabaseConfig.apply)

  /** Loads from the ambient config (`application.conf` over this module's `reference.conf`, overridable by `DATABASE_*`
    * environment variables).
    *
    * Returns the failures rather than throwing: a service decides for itself whether an unreadable database config is a
    * fatal boot error (it is, for ferrite and cobalt) — this module does not get to call `System.exit` on its caller's
    * behalf.
    */
  def load(source: ConfigSource = ConfigSource.default): Either[ConfigReaderFailures, DatabaseConfig] =
    source.at(Namespace).load[DatabaseConfig]
