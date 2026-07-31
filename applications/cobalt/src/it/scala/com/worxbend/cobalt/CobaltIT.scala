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

import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.worxbend.persistence.Database
import com.worxbend.persistence.DatabaseConfig
import com.worxbend.persistence.MigrationReport
import com.worxbend.persistence.Migrations
import com.worxbend.persistence.PoolConfig
import java.sql.Connection
import java.util.concurrent.Executors
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.utility.DockerImageName
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.Using

/** The shared slow-tier fixture: one broker, one database, one JVM.
  *
  * **Containers, started once and only when nobody supplied a server.** ADR §9.2's shape: `IT / fork := true` gives
  * this module its own JVM, `IT / parallelExecution := false` means nothing races for the handles, and the `lazy val`s
  * mean a run that never reaches these suites never pays for a container. Ryuk reaps both when the JVM exits, so there
  * is no teardown hook to forget.
  *
  * **The environment variables still win.** `KAFKA_BOOTSTRAP_SERVERS` and `IT_POSTGRES_URL` are not a fallback for a
  * missing Docker — they are how a CI job points every module's slow tier at infrastructure it provisioned once, and
  * how this suite shares a Postgres with `modules/persistence` instead of starting a second one.
  *
  * **What is left of the skip.** [[available]] is now false only when Docker itself is unreachable, and the suite
  * ignores itself in that case rather than failing — a red suite on a laptop with no Docker teaches people to ignore
  * red suites. It is emphatically not the old behaviour, where the absence of an environment variable nobody set made
  * the whole tier silently decorative.
  */
object CobaltIT:

  val BootstrapEnv: String = "KAFKA_BOOTSTRAP_SERVERS"
  val UrlEnv: String = "IT_POSTGRES_URL"
  val UserEnv: String = "IT_POSTGRES_USER"
  val PasswordEnv: String = "IT_POSTGRES_PASSWORD"

  /** The images ADR §3.10 pins — the same ones `deploy/docker-compose.yml` runs. */
  val KafkaImage: String = "apache/kafka:4.3.1"
  val PostgresImage: String = "postgres:18.4-alpine"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.trim.nonEmpty)

  private lazy val kafkaContainer: Option[KafkaContainer] =
    Option.when(env(BootstrapEnv).isEmpty)(KafkaContainer(DockerImageName.parse(KafkaImage))).flatMap: started =>
      Try(started.start()).toOption.map(_ => started)

  private lazy val postgresContainer: Option[PostgreSQLContainer] =
    Option
      .when(env(UrlEnv).isEmpty)(PostgreSQLContainer(dockerImageNameOverride = DockerImageName.parse(PostgresImage)))
      .flatMap: started =>
        Try(started.start()).toOption.map(_ => started)

  def bootstrapServers: Option[String] = env(BootstrapEnv).orElse(kafkaContainer.map(_.bootstrapServers))

  def jdbcUrl: Option[String] = env(UrlEnv).orElse(postgresContainer.map(_.jdbcUrl))

  def username: String = env(UserEnv).orElse(postgresContainer.map(_.username)).getOrElse("postgres")

  def password: String = env(PasswordEnv).orElse(postgresContainer.map(_.password)).getOrElse("postgres")

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

  /** `TRUNCATE`, not `DELETE`: the fact table is partitioned and append-only, and `V1__events.sql` tunes autovacuum for
    * insert-driven collection, so a `DELETE` leaves dead tuples nothing comes back for.
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
        com.worxbend.eventing.KafkaCodecs.producerConfig(settings),
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
