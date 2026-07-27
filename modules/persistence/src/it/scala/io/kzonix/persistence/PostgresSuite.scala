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

import com.dimafeng.testcontainers.PostgreSQLContainer
import java.sql.Connection
import java.util.concurrent.Executors
import org.testcontainers.utility.DockerImageName
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Using

/** The PostgreSQL 18 fixture shared by every integration suite in this module.
  *
  * **On the container.** ADR §9.2 specifies Testcontainers with a single shared lazy container per forked JVM, and that
  * is what this object starts: `IT / fork := true` gives one JVM per module, `IT / parallelExecution := false` means
  * nothing races for it, and the `lazy val` means a suite that never touches the database never pays for a container.
  * Ryuk reaps it when the JVM exits, so there is no teardown hook to forget.
  *
  * `IT_POSTGRES_URL` still wins when it is set. That is not a fallback for a missing Docker — it is how a CI job points
  * the suite at a server it already provisioned, and how the app-module suites (which have no Testcontainers on their
  * IT classpath) share one server with this one.
  *
  * One pool, one migration, one JVM: start-up dominates wall-clock either way.
  */
object PostgresSuite:

  val UrlEnv: String = "IT_POSTGRES_URL"
  val UserEnv: String = "IT_POSTGRES_USER"
  val PasswordEnv: String = "IT_POSTGRES_PASSWORD"

  /** The image ADR §3.10 pins. */
  val Image: String = "postgres:18.4-alpine"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

  /** The container, started once and only when no external server was supplied. */
  private lazy val container: Option[PostgreSQLContainer] =
    Option.when(env(UrlEnv).isEmpty):
      val started = PostgreSQLContainer(dockerImageNameOverride = DockerImageName.parse(Image))
      started.start()
      started

  def jdbcUrl: String = env(UrlEnv).getOrElse(container.map(_.jdbcUrl).getOrElse(sys.error("no container")))

  def username: String = env(UserEnv).orElse(container.map(_.username)).getOrElse("postgres")

  def password: String = env(PasswordEnv).orElse(container.map(_.password)).getOrElse("postgres")

  /** A single small pool. The pool under test is the production one, so the settings are the production defaults with
    * the sizes cut down — a fixture that used its own hand-rolled `DataSource` would test a code path nothing ships.
    */
  lazy val config: DatabaseConfig =
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
    DatabaseConfig(jdbcUrl, username, password, pool.copy(poolName = "it-read"), pool.copy(poolName = "it-write"))

  /** Not wrapped in a `Try`: a pool that will not open is the defect this tier exists to find, and swallowing it into a
    * skipped suite is how ten suites came to have never run at all.
    */
  lazy val database: Database = Database.open(config)

  /** Migrations run exactly once per JVM. `migrate` is idempotent, so this is belt and braces rather than a correctness
    * requirement — but re-running it per suite would triple the wall-clock of the slow tier.
    */
  lazy val migrated: MigrationReport = Migrations.migrate(database.write.get())

/** Base class: owns the shared pool and applies the schema once. */
abstract class PostgresSuite extends munit.FunSuite:

  protected val patience: FiniteDuration = 30.seconds

  /** Sized to the pool, as ADR §0 decision 8 requires of every executor that runs blocking JDBC. */
  protected given executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  protected def database: Database = PostgresSuite.database

  protected def await[A](future: Future[A]): A = Await.result(future, patience)

  override def beforeAll(): Unit =
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
