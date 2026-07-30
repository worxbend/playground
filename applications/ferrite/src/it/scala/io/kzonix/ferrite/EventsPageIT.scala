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

package io.kzonix.ferrite

import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.Json
import io.kzonix.ferrite.web.Urls
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.EventId
import io.kzonix.kernel.event.EventType
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.Source
import io.kzonix.kernel.event.Subject
import io.kzonix.persistence.Database
import io.kzonix.persistence.DatabaseConfig
import io.kzonix.persistence.Migrations
import io.kzonix.persistence.PoolConfig
import io.kzonix.persistence.repository.NewEvent
import io.kzonix.persistence.repository.PostgresEventRepository
import java.time.OffsetDateTime
import munit.FunSuite
import org.jsoup.Jsoup
import org.testcontainers.utility.DockerImageName
import play.api.Application
import play.api.http.Status
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.test.FakeRequest
import play.api.test.Helpers
import play.api.test.Helpers.contentAsString
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.route
import play.api.test.Helpers.status
import play.api.test.Helpers.writeableOf_AnyContentAsEmpty
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.Try
import scala.util.Using

/** The whole Play stack over a real PostgreSQL.
  *
  * `GuiceApplicationBuilder` + `route()` and not an HTTP client: the router, the filters (including CSRF), the module
  * bindings, the Hikari pools, the bounded search dispatcher and every template are exercised, and no socket is opened.
  * ADR §9.3 puts ferrite's integration tier here deliberately — `scalatestplus-play` is a standing rejection.
  *
  * **The database is a Testcontainer**, started once per forked JVM by the companion object and reaped by Ryuk.
  * `IT_POSTGRES_URL` still wins when it is set — that is how a CI job points several modules at one server rather than
  * starting one per module, and it is why [[seed]] truncates before it inserts. The suite ignores itself only when
  * Docker is unreachable: a red suite on a laptop with no Docker teaches people to ignore red suites.
  *
  * **Migrations are run by the fixture, never by the application.** That is the arrangement under test rather than a
  * convenience: `io.kzonix.ferrite.wiring.Databases` applies no DDL, and if ferrite ever started migrating on boot this
  * is where it would surface.
  */
final class EventsPageIT extends FunSuite:

  private def jdbcUrl: Option[String] = EventsPageIT.jdbcUrl
  private def username: String = EventsPageIT.username
  private def password: String = EventsPageIT.password

  /** Skip rather than fail when Docker is unreachable and nobody supplied a server. See the class comment. */
  override def munitIgnore: Boolean = jdbcUrl.isEmpty

  private val fixturePool: PoolConfig = PoolConfig(
    poolName = "ferrite-it",
    maximumPoolSize = 2,
    minimumIdle = 1,
    connectionTimeout = 5.seconds,
    idleTimeout = 1.minute,
    maxLifetime = 5.minutes,
    validationTimeout = 2.seconds,
    readOnly = false,
    connectionInitSql = None
  )

  /** Applies the schema and seeds exactly the rows this suite asserts on, on a pool of its own.
    *
    * **It seeds.** The page renders a results table when there are rows and an empty state when there are none, which
    * is correct and is also why a page assertion that does not control the row count is an assertion about whichever
    * other suite touched this database last: with `IT_POSTGRES_URL` pointing several modules at one server, this suite
    * passed after `modules/persistence` had run and failed when it ran first, and nothing about ferrite had changed in
    * between. `TRUNCATE` then insert makes the page deterministic in both directions.
    */
  private lazy val migrated: Boolean =
    jdbcUrl.exists { url =>
      val database = Database.open(DatabaseConfig(url, username, password, fixturePool, fixturePool))
      try
        val report = Migrations.migrate(database.write.get())
        seed(database)
        // `targetVersion` is the version the schema is at when the run finishes, so it is populated on a repeat run
        // too — which is the whole point of it not being Flyway's `targetSchemaVersion` verbatim.
        report.targetVersion.isDefined
      finally database.close()
    }

  /** One row, built through the kernel's encoder so the generated columns are computed from a real CloudEvent. */
  private def seed(database: Database): Unit =
    def force[A](result: Either[String, A]): A = result.fold(message => fail(message), identity)
    val envelope = Envelope(
      id = force(EventId("ferrite-it-1")),
      source = force(Source("/gateways/it")),
      eventType = force(EventType("io.kzonix.iot.telemetry")),
      time = Some(OffsetDateTime.parse("2026-07-15T12:00:00Z")),
      subject = Some(force(Subject("kitchen-1"))),
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(
        Json.obj(
          "deviceId" -> Json.fromString("kitchen-1"),
          "roomId" -> Json.fromString("kitchen"),
          "severity" -> Json.fromString("error"),
          "value" -> Json.fromDoubleOrNull(21.5)
        )
      )
    )
    Using.resource(database.write.get().getConnection): connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute("TRUNCATE TABLE events.cloud_event")
    // Written through the production repository, so this also proves ferrite's own classpath can run the insert.
    given ExecutionContext = ExecutionContext.parasitic
    val repository = PostgresEventRepository(database.read.transactor, database.write.transactor)
    val _ = Await.result(repository.insertAll(Vector(force(NewEvent.from(envelope)))), 30.seconds)

  private def application(): Application =
    GuiceApplicationBuilder()
      .configure(
        Map[String, Any](
          "database.jdbc-url" -> jdbcUrl.getOrElse(""),
          "database.username" -> username,
          "database.password" -> password,
          // Pool and dispatcher sized together, as production requires them to be.
          "database.read.maximum-pool-size" -> 4,
          "database.search-dispatcher.thread-pool-executor.fixed-pool-size" -> 4,
          "play.http.secret.key" -> "integration-test-secret-integration-test-secret-0123456789",
          "play.filters.hosts.allowed" -> List("localhost")
        )
      )
      .build()

  test("the list page renders against a real schema"):
    assert(migrated, "the schema could not be applied")
    val app = application()
    Helpers.running(app):
      val result = route(app, FakeRequest("GET", Urls.events(""))).getOrElse(fail("the list route is not wired"))
      assertEquals(status(result), Status.OK)
      val document = Jsoup.parse(contentAsString(result))
      // Structural, never a string match: the region a screen reader watches and the landmark it lives in.
      assertEquals(document.select("section#results[aria-live=polite]").size(), 1)
      assertEquals(document.select("main#main").size(), 1)
      assertEquals(document.select("table.event-table thead th").size(), 9)
      assertEquals(document.select("table.event-table tbody tr.event-row").size(), 1)

  test("a filter that matches nothing renders the empty state, not an empty table"):
    // The other half of the branch above, and the reason that one has to seed: `fragments/results` renders a table or
    // an empty state depending on the row count, so a page assertion that does not own its data is an assertion about
    // whichever suite wrote to this database last.
    assert(migrated, "the schema could not be applied")
    val app = application()
    Helpers.running(app):
      val url = Urls.events("v=1&device=nothing-called-this")
      val result = route(app, FakeRequest("GET", url)).getOrElse(fail("the list route is not wired"))
      assertEquals(status(result), Status.OK)
      val document = Jsoup.parse(contentAsString(result))
      assertEquals(document.select("table.event-table").size(), 0)
      assertEquals(document.select(".empty-state").size(), 1)

  test("a permalink round-trips through the real filter-to-SQL compiler"):
    assert(migrated, "the schema could not be applied")
    val app = application()
    Helpers.running(app):
      val url = Urls.events("v=1&type=io.kzonix.iot.telemetry&severity=%3E%3Dwarn&limit=10")
      val result = route(app, FakeRequest("GET", url)).getOrElse(fail("the list route is not wired"))
      assertEquals(status(result), Status.OK)

  test("a rejected permalink is a 400 and never a 500"):
    assert(migrated, "the schema could not be applied")
    val app = application()
    Helpers.running(app):
      val result = route(app, FakeRequest("GET", Urls.events("v=1&from=yesterday")))
        .getOrElse(fail("the list route is not wired"))
      assertEquals(status(result), Status.BAD_REQUEST)

  test("the probes and the metrics exposition answer once the pools are open"):
    assert(migrated, "the schema could not be applied")
    val app = application()
    Helpers.running(app):
      assertEquals(status(route(app, FakeRequest("GET", Urls.HealthLive)).getOrElse(fail("liveness"))), Status.OK)
      assertEquals(status(route(app, FakeRequest("GET", Urls.HealthReady)).getOrElse(fail("readiness"))), Status.OK)
      val metrics = route(app, FakeRequest("GET", Urls.Metrics)).getOrElse(fail("metrics"))
      assertEquals(status(metrics), Status.OK)
      assert(contentAsString(metrics).contains("jvm_"), "expected the JVM binders in the exposition")

  test("the real Guice wiring publishes the pool meters and the search histogram"):
    assert(migrated, "the schema could not be applied")
    val app = application()
    Helpers.running(app):
      // A search first, so `search.query.duration` exists at all — the timer is registered on first record.
      assertEquals(status(route(app, FakeRequest("GET", Urls.events(""))).getOrElse(fail("search"))), Status.OK)

      val exposition = contentAsString(route(app, FakeRequest("GET", Urls.Metrics)).getOrElse(fail("metrics")))

      // The pool meters only exist if `Databases` passed a MetricsTrackerFactory into `Database.open` *and* Guice
      // resolved Telemetry before the pools opened. Neither is visible in a unit test: the wiring is the thing.
      assert(
        exposition.contains("hikaricp_connections"),
        s"HikariCP published nothing — the MetricsTrackerFactory did not reach the pool"
      )
      assert(
        exposition.contains("""pool="observatory-read""""),
        "the read pool is not distinguishable from the write pool in the exposition"
      )
      assert(
        exposition.contains("search_query_duration_seconds_bucket"),
        "the search timer reached Prometheus without buckets, so no percentile panel can work"
      )
      assert(
        exposition.contains(s"""shape="${io.kzonix.ferrite.search.SearchShape.Unfiltered}""""),
        "the unfiltered landing query was not tagged with its shape"
      )

/** The database, started once per forked JVM.
  *
  * A companion object rather than a field: munit constructs a fresh suite instance for each test, so a `lazy val` on
  * the suite would start one PostgreSQL per test rather than one per run. (The *migration* deliberately stays on the
  * instance — it is idempotent, and re-seeding per test is what makes the "one row" and "no rows" assertions
  * independent of execution order.)
  */
object EventsPageIT:

  val UrlEnv: String = "IT_POSTGRES_URL"

  /** The image ADR §3.10 pins — the same one `deploy/docker-compose.yml` runs. */
  val Image: String = "postgres:18.4-alpine"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

  /** `Try`, so an unreachable Docker daemon skips the suite instead of failing every test with the same stack trace. A
    * missing *schema* is a defect; a missing Docker is a laptop.
    */
  private lazy val container: Option[PostgreSQLContainer] =
    Option
      .when(env(UrlEnv).isEmpty)(PostgreSQLContainer(dockerImageNameOverride = DockerImageName.parse(Image)))
      .flatMap(started => Try(started.start()).toOption.map(_ => started))

  lazy val jdbcUrl: Option[String] = env(UrlEnv).orElse(container.map(_.jdbcUrl))

  lazy val username: String = env("IT_POSTGRES_USER").orElse(container.map(_.username)).getOrElse("postgres")

  lazy val password: String = env("IT_POSTGRES_PASSWORD").orElse(container.map(_.password)).getOrElse("postgres")
