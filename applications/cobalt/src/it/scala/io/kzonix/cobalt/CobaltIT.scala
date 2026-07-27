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

package io.kzonix.cobalt

import java.sql.Connection
import java.util.concurrent.Executors

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.Using

import io.kzonix.persistence.Database
import io.kzonix.persistence.DatabaseConfig
import io.kzonix.persistence.MigrationReport
import io.kzonix.persistence.Migrations
import io.kzonix.persistence.PoolConfig
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

/** The shared slow-tier fixture: one broker, one database, one JVM.
  *
  * **Why environment variables and not Testcontainers.** ADR §9.2 asks for a shared lazy Testcontainers singleton per
  * forked JVM, and that is the intended end state — but `testcontainers-scala` is wired to `eventing` and `persistence`
  * only, and `applications/cobalt` has no `testContainers.map(_ % IT)` line in `build.sbt`. Adding one is a build
  * change this work is not permitted to make, so it is reported rather than smuggled in. Every test below already takes
  * its addresses from this object, so the switch is a change to [[bootstrapServers]] and [[jdbcUrl]] and nothing else.
  *
  * **Absent dependencies ignore the suite rather than failing it**, matching `modules/persistence`'s `PostgresSuite`: a
  * red suite on a laptop with no Docker teaches people to ignore red suites, which is a worse outcome than a skipped
  * one.
  */
object CobaltIT:

  val BootstrapEnv: String = "KAFKA_BOOTSTRAP_SERVERS"
  val UrlEnv: String = "IT_POSTGRES_URL"
  val UserEnv: String = "IT_POSTGRES_USER"
  val PasswordEnv: String = "IT_POSTGRES_PASSWORD"

  /** The images ADR §3.10 pins. Recorded here so the values move with the fixture when containers are wired up. */
  val KafkaImage: String = "apache/kafka:4.3.1"
  val PostgresImage: String = "postgres:18.4-alpine"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.trim.nonEmpty)

  def bootstrapServers: Option[String] = env(BootstrapEnv)

  def jdbcUrl: Option[String] = env(UrlEnv)

  def username: String = env(UserEnv).getOrElse("postgres")

  def password: String = env(PasswordEnv).getOrElse("postgres")

  /** The production pool with its sizes cut down. A fixture with a hand-rolled `DataSource` would exercise a code path
    * nothing ships.
    */
  lazy val databaseConfig: Option[DatabaseConfig] =
    jdbcUrl.map: url =>
      val pool = PoolConfig(
        poolName = "cobalt-it",
        maximumPoolSize = 4,
        minimumIdle = 1,
        connectionTimeout = 5.seconds,
        idleTimeout = 1.minute,
        maxLifetime = 5.minutes,
        validationTimeout = 2.seconds,
        readOnly = false,
        connectionInitSql = None
      )
      val read = pool.copy(poolName = "cobalt-it-read")
      DatabaseConfig(url, username, password, read, pool.copy(poolName = "cobalt-it-write"))

  lazy val database: Option[Database] = databaseConfig.flatMap(config => Try(Database.open(config)).toOption)

  lazy val available: Boolean = database.isDefined && bootstrapServers.isDefined

  /** Migrations run once per JVM. cobalt runs them at boot too — `migrate` is idempotent, which is the property that
    * makes both true at once.
    */
  lazy val migrated: MigrationReport =
    Migrations.migrate(database.getOrElse(sys.error("no database")).write.get())

/** Base class: ignores itself when either dependency is missing, and owns the shared handles. */
abstract class CobaltIT extends munit.FunSuite:

  override def munitIgnore: Boolean = !CobaltIT.available

  protected val patience: FiniteDuration = 60.seconds

  /** Sized to the pool, as ADR §0 decision 8 requires of every executor running blocking JDBC. */
  protected given executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  protected def database: Database = CobaltIT.database.getOrElse(fail("no database configured"))

  protected def servers: String = CobaltIT.bootstrapServers.getOrElse(fail("no broker configured"))

  protected def await[A](future: Future[A]): A = Await.result(future, patience)

  override def beforeAll(): Unit =
    if CobaltIT.available then
      val _ = CobaltIT.migrated

  protected def withConnection[A](use: Connection => A): A =
    Using.resource(database.write.get().getConnection)(use)

  /** `TRUNCATE`, not `DELETE`: the fact table is partitioned and append-only, and `V1__events.sql` tunes autovacuum
    * for insert-driven collection, so a `DELETE` leaves dead tuples nothing comes back for.
    */
  protected def truncateEvents(): Unit =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute("TRUNCATE TABLE events.cloud_event")

  /** How many rows carry this CloudEvents `id`. The idempotence assertion is that this never exceeds one. */
  protected def countById(ceId: String): Long =
    withConnection: connection =>
      Using.resource(connection.prepareStatement("SELECT count(*) FROM events.cloud_event WHERE ce_id = ?")):
        statement =>
          statement.setString(1, ceId)
          Using.resource(statement.executeQuery()): results =>
            if results.next() then results.getLong(1) else 0L

  /** Creates a uniquely-named topic so suites never inherit another suite's offsets. */
  protected def newTopic(prefix: String, partitions: Int = 1): String =
    val name = s"$prefix-${java.util.UUID.randomUUID()}"
    Using.resource(Admin.create(Map[String, AnyRef]("bootstrap.servers" -> servers).asJava)): admin =>
      val _ = admin.createTopics(List(NewTopic(name, partitions, 1.toShort)).asJava).all().get()
    name

  /** Publishes with the production producer settings, so `acks=all` and idempotence are the ones under test. */
  protected def publish(records: Vector[ProducerRecord[String, Array[Byte]]]): Unit =
    val settings = Map[String, String]("bootstrap.servers" -> servers)
    Using.resource(
      KafkaProducer[String, Array[Byte]](
        io.kzonix.eventing.KafkaCodecs.producerConfig(settings),
        StringSerializer(),
        ByteArraySerializer()
      )
    ): producer =>
      records.foreach: record =>
        val _ = producer.send(record)
      producer.flush()

  /** Polls `condition` until it holds or `patience` runs out.
    *
    * A consumer is asynchronous by construction, so there is no completion to await — only a state to observe. A fixed
    * `Thread.sleep` would either be flaky on a loaded CI box or slow on every green run; polling is neither.
    */
  protected def eventually(what: String)(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + patience.toNanos
    while !condition && System.nanoTime() < deadline do Thread.sleep(100L)
    assert(condition, s"timed out waiting for $what")
