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

import pureconfig.ConfigSource
import scala.concurrent.duration.*

/** The module's `reference.conf` and its reader.
  *
  * Configuration defaults are code: the two-pool isolation of ADR §5 only exists if the defaults actually describe two
  * differently-shaped pools, and the search pool's server-side statement timeout only exists if it is actually in the
  * init SQL. Both are the kind of thing that is silently deleted during a merge, so both are asserted.
  */
final class DatabaseConfigSuite extends munit.FunSuite:

  private def loaded: DatabaseConfig =
    DatabaseConfig.load().fold(failures => fail(failures.toList.mkString("; ")), identity)

  test("the shipped defaults load"):
    val config = loaded
    assert(config.jdbcUrl.startsWith("jdbc:postgresql://"), config.jdbcUrl)
    assert(config.username.nonEmpty)

  test("the search pool is read-only and carries a server-side statement timeout"):
    // A client-side query timeout would abandon the JDBC call and leave the backend running, which is the failure
    // this setting exists to prevent. If the init SQL disappears, the pool silently stops bounding anything.
    val read = loaded.read
    assert(read.readOnly, "the search pool must default to read-only")
    assert(
      read.connectionInitSql.exists(_.contains("statement_timeout")),
      s"the search pool has no statement_timeout: ${read.connectionInitSql}"
    )

  test("the ingest pool is writable and is a separate pool"):
    val config = loaded
    assert(!config.write.readOnly)
    assertNotEquals(config.read.poolName, config.write.poolName)

  test("both pools are bounded, and idle connections are recycled before a proxy would"):
    Vector(loaded.read, loaded.write).foreach: pool =>
      assert(pool.maximumPoolSize > 0 && pool.maximumPoolSize <= 32, s"${pool.poolName}: ${pool.maximumPoolSize}")
      assert(pool.minimumIdle >= 0 && pool.minimumIdle <= pool.maximumPoolSize, pool.poolName)
      assert(pool.connectionTimeout > Duration.Zero, pool.poolName)
      // 30 minutes is the idle timeout of most proxies and of a typical `idle_session_timeout`; recycling first
      // means connections are closed by us rather than reset under us mid-statement.
      assert(pool.maxLifetime < 30.minutes, s"${pool.poolName}: maxLifetime ${pool.maxLifetime}")

  test("a missing namespace is reported, not defaulted"):
    // Silently defaulting would give a service that starts, connects to localhost, and reports itself healthy while
    // reading an empty database.
    assert(DatabaseConfig.load(ConfigSource.empty).isLeft)
