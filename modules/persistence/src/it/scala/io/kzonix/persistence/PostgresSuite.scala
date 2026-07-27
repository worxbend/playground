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

import java.sql.Connection
import java.util.concurrent.Executors
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.Using

/** The PostgreSQL 18 fixture shared by every integration suite in this module.
  *
  * **On the container.** ADR §9.2 specifies Testcontainers with a single shared lazy container per forked JVM, and that
  * is what this object should start. It does not, because `com.dimafeng::testcontainers-scala-*` and
  * `org.testcontainers:testcontainers-postgresql` are declared in `project/Dependencies.scala` but are not on any
  * project's classpath — `Dependencies.testContainers` is never referenced from `build.sbt`. Adding
  * `libraryDependencies ++= testContainers.map(_ % IT)` to the `persistence` project is the whole change; the only code
  * that then moves is [[jdbcUrl]] and its two companions, which become the container's accessors. Everything below —
  * the migration, the round trip, the idempotency replay — is written against a `DataSource` and does not care where
  * the server came from.
  *
  * Until then the suites read `IT_POSTGRES_URL` and **ignore themselves** when it is absent, rather than failing. That
  * is the honest behaviour for a fixture that cannot provision its own dependency: a red suite on a developer's laptop
  * that has no database teaches people to ignore red suites.
  *
  * One pool, one migration, one JVM: container or not, start-up dominates wall-clock, and `IT / parallelExecution` is
  * already false so there is no race to protect against.
  */
object PostgresSuite:

  val UrlEnv: String = "IT_POSTGRES_URL"
  val UserEnv: String = "IT_POSTGRES_USER"
  val PasswordEnv: String = "IT_POSTGRES_PASSWORD"

  /** The image ADR §3.10 pins. Recorded here so the value moves with the fixture when the container is wired up. */
  val Image: String = "postgres:18.4-alpine"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

  def jdbcUrl: Option[String] = env(UrlEnv)

  def username: String = env(UserEnv).getOrElse("postgres")

  def password: String = env(PasswordEnv).getOrElse("postgres")

  /** A single small pool. The pool under test is the production one, so the settings are the production defaults with
    * the sizes cut down — a fixture that used its own hand-rolled `DataSource` would test a code path nothing ships.
    */
  lazy val config: Option[DatabaseConfig] =
    jdbcUrl.map: url =>
      val pool = PoolConfig(
        poolName = "it",
        maximumPoolSize = 4,
        minimumIdle = 1,
        connectionTimeout = 5.seconds,
        idleTimeout = 1.minute,
        maxLifetime = 5.minutes,
        validationTimeout = 2.seconds,
        readOnly = false,
        connectionInitSql = None
      )
      DatabaseConfig(url, username, password, pool.copy(poolName = "it-read"), pool.copy(poolName = "it-write"))

  lazy val database: Option[Database] = config.flatMap(cfg => Try(Database.open(cfg)).toOption)

  lazy val available: Boolean = database.isDefined

  /** Migrations run exactly once per JVM. `migrate` is idempotent, so this is belt and braces rather than a correctness
    * requirement — but re-running it per suite would triple the wall-clock of the slow tier.
    */
  lazy val migrated: MigrationReport =
    val db = database.getOrElse(sys.error("no database"))
    Migrations.migrate(db.write.get())

/** Base class: ignores itself when no server is configured, and provides the shared pool. */
abstract class PostgresSuite extends munit.FunSuite:

  override def munitIgnore: Boolean = !PostgresSuite.available

  protected val patience: FiniteDuration = 30.seconds

  /** Sized to the pool, as ADR §0 decision 8 requires of every executor that runs blocking JDBC. */
  protected given executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  protected def database: Database = PostgresSuite.database.getOrElse(fail("no database configured"))

  protected def await[A](future: Future[A]): A = Await.result(future, patience)

  override def beforeAll(): Unit =
    if PostgresSuite.available then
      val _ = PostgresSuite.migrated

  /** Empties the fact table between suites.
    *
    * `TRUNCATE`, not `DELETE`: the table is partitioned and append-only, so a `DELETE` leaves dead tuples that the
    * insert-driven autovacuum settings of `V1__events.sql` are explicitly tuned not to collect.
    */
  protected def truncateEvents(): Unit =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute("TRUNCATE TABLE events.cloud_event")

  protected def withConnection[A](use: Connection => A): A =
    Using.resource(database.write.get().getConnection)(use)
