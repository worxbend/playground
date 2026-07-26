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

package io.kzonix.persistence

import com.augustnagro.magnum.Transactor
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource

/** A source of a `DataSource`.
  *
  * Structurally identical to `jakarta.inject.Provider[DataSource]` — one method, `get()`, same erasure — so ferrite
  * binds it in a Guice module with a one-line adapter and nothing about this module leaks into the injection container.
  * It is declared here rather than *being* `jakarta.inject.Provider` because `jakarta.inject-api` is not on this
  * module's classpath, and adding a DI annotation library to an infrastructure library to express "a thing you can ask
  * for a DataSource" would invert the dependency this trait exists to keep pointing inward: cobalt has no Guice at all
  * and must be able to use the same pool.
  */
trait DataSourceProvider:

  def get(): DataSource

/** One bounded HikariCP pool, with an explicit shutdown.
  *
  * `AutoCloseable` and not a finaliser or a shutdown hook: a pool holds server-side sessions, and leaking them is how a
  * restart loop exhausts `max_connections` and takes down the *other* services too. The owner — a Guice `@Singleton`'s
  * `stop` hook in ferrite, `CoordinatedShutdown` in cobalt — is responsible for calling [[close]], and making that
  * explicit is the point.
  */
final class HikariPool private (private val pool: HikariDataSource) extends DataSourceProvider, AutoCloseable:

  def get(): DataSource = pool

  /** Magnum's entry point. Constructed once and shared: a `Transactor` is a value, not a connection. */
  val transactor: Transactor = Transactor(pool)

  /** Idempotent — Hikari's own `close()` is. */
  def close(): Unit = pool.close()

object HikariPool:

  /** Opens the pool eagerly.
    *
    * Hikari's default `initializationFailTimeout` is kept, so an unreachable or misconfigured database fails *here*,
    * during construction, rather than on the first user request. For a service whose entire job is reading and writing
    * this database, a boot that succeeds without a database is a service that reports itself healthy and serves errors.
    */
  def open(database: DatabaseConfig, pool: PoolConfig): HikariPool =
    val config = HikariConfig()
    config.setJdbcUrl(database.jdbcUrl)
    config.setUsername(database.username)
    config.setPassword(database.password)
    config.setPoolName(pool.poolName)
    config.setMaximumPoolSize(pool.maximumPoolSize)
    config.setMinimumIdle(pool.minimumIdle)
    config.setConnectionTimeout(pool.connectionTimeout.toMillis)
    config.setIdleTimeout(pool.idleTimeout.toMillis)
    config.setMaxLifetime(pool.maxLifetime.toMillis)
    config.setValidationTimeout(pool.validationTimeout.toMillis)
    config.setReadOnly(pool.readOnly)
    pool.connectionInitSql.foreach(config.setConnectionInitSql)
    // Magnum's `transact` manages transactions itself and `connect` wants each statement to stand alone.
    config.setAutoCommit(true)
    HikariPool(HikariDataSource(config))

/** The two pools of ADR §5, opened and closed together.
  *
  * They are a pair rather than two independent objects because their failure modes are shared: if the write pool cannot
  * be opened, a service holding only a working read pool is not degraded, it is wrong. Closing is likewise
  * all-or-nothing, and [[close]] closes the second pool even when the first throws — a half-closed pair leaks exactly
  * the sessions the explicit shutdown path exists to reclaim.
  */
final class Database private (val read: HikariPool, val write: HikariPool) extends AutoCloseable:

  def close(): Unit =
    try read.close()
    finally write.close()

object Database:

  def open(config: DatabaseConfig): Database =
    val read = HikariPool.open(config, config.read)
    val write =
      try HikariPool.open(config, config.write)
      catch
        case error: Throwable =>
          read.close()
          throw error
    Database(read, write)
