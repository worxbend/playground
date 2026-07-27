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

import io.kzonix.ferrite.web.Urls
import io.kzonix.persistence.Database
import io.kzonix.persistence.DatabaseConfig
import io.kzonix.persistence.Migrations
import io.kzonix.persistence.PoolConfig
import munit.FunSuite
import org.jsoup.Jsoup
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
import scala.concurrent.duration.DurationInt

/** The whole Play stack over a real PostgreSQL.
  *
  * `GuiceApplicationBuilder` + `route()` and not an HTTP client: the router, the filters (including CSRF), the module
  * bindings, the Hikari pools, the bounded search dispatcher and every template are exercised, and no socket is
  * opened. ADR §9.3 puts ferrite's integration tier here deliberately — `scalatestplus-play` is a standing rejection.
  *
  * **The database is provisioned outside the suite**, exactly as `modules/persistence`'s integration suites do: the
  * connection details arrive in `IT_POSTGRES_*` and the suite ignores itself when they are absent. That is the honest
  * behaviour for a fixture that cannot provision its own dependency — a red suite on a laptop with no database
  * teaches people to ignore red suites. Wiring Testcontainers here needs `testContainers.map(_ % IT)` on the ferrite
  * project in the root `build.sbt`; the dimafeng and Testcontainers artifacts are **not** on this module's IT
  * classpath today, so the import would not compile.
  *
  * **Migrations are run by the fixture, never by the application.** That is the arrangement under test rather than a
  * convenience: `io.kzonix.ferrite.wiring.Databases` applies no DDL, and if ferrite ever started migrating on boot
  * this is where it would surface.
  */
final class EventsPageIT extends FunSuite:

  /** The image ADR §3.10 pins, recorded so the value travels with the fixture when a container is wired up. */
  val Image: String = "postgres:18.4-alpine"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

  private val jdbcUrl: Option[String] = env("IT_POSTGRES_URL")
  private val username: String = env("IT_POSTGRES_USER").getOrElse("postgres")
  private val password: String = env("IT_POSTGRES_PASSWORD").getOrElse("postgres")

  /** Skip rather than fail when there is no database. See the class comment. */
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

  /** Applies the schema once per JVM, on a pool of the fixture's own. */
  private lazy val migrated: Boolean =
    jdbcUrl.exists { url =>
      val database = Database.open(DatabaseConfig(url, username, password, fixturePool, fixturePool))
      try
        val report = Migrations.migrate(database.write.get())
        // A repeat run executes nothing and is still a success; what matters is that a target version exists.
        report.targetVersion.isDefined
      finally database.close()
    }

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
