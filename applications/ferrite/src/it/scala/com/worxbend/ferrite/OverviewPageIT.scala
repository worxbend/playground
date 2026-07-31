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

package com.worxbend.ferrite

import com.worxbend.ferrite.controllers.TailController
import com.worxbend.ferrite.overview.OverviewRange
import com.worxbend.ferrite.tail.TailCursor
import com.worxbend.ferrite.tail.TailService
import com.worxbend.ferrite.web.Query
import com.worxbend.ferrite.web.Urls
import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.persistence.Database
import com.worxbend.persistence.DatabaseConfig
import com.worxbend.persistence.Migrations
import com.worxbend.persistence.PoolConfig
import com.worxbend.persistence.maintenance.PartitionMaintenance
import com.worxbend.persistence.maintenance.PartitionPolicy
import com.worxbend.persistence.maintenance.RollupRefresh
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.PostgresEventRepository
import io.circe.Json
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import munit.FunSuite
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.jsoup.Jsoup
import play.api.Application
import play.api.http.Status
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.test.FakeRequest
import play.api.test.Helpers
import play.api.test.Helpers.contentAsString
import play.api.test.Helpers.contentType
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.route
import play.api.test.Helpers.status
import play.api.test.Helpers.writeableOf_AnyContentAsEmpty
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.Using

/** The overview and the live tail through the whole Play stack, against a real PostgreSQL.
  *
  * What this tier adds over `OverviewSuite`, which stubs the repositories: that the Guice wiring actually binds an
  * `OverviewRepository`, that the statements it builds are accepted by a real `events.event_rollup_hourly`, and that
  * `/` is a page rather than the redirect it used to be. None of those can fail in a unit test — the first two are
  * wiring and SQL, and the third would pass against a stub that returned anything at all.
  *
  * **The rollup is refreshed by the fixture**, exactly as cobalt's scheduled job does it in production. Without that
  * step the view is the empty snapshot the migration created and this suite would assert that zero equals zero.
  *
  * Seeding is relative to now and inside the current hour, for the reasons `OverviewRepositoryIT` sets out: the view
  * only covers the last ninety days, and an event outside the current month would land in the `DEFAULT` partition that
  * `MigrationIT` requires to stay empty.
  */
final class OverviewPageIT extends FunSuite:

  private def jdbcUrl: Option[String] = EventsPageIT.jdbcUrl

  override def munitIgnore: Boolean = jdbcUrl.isEmpty

  private val now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)

  private val hour: OffsetDateTime = now.truncatedTo(ChronoUnit.HOURS)

  private val fixturePool: PoolConfig = PoolConfig(
    poolName = "ferrite-overview-it",
    maximumPoolSize = 2,
    minimumIdle = 1,
    connectionTimeout = 5.seconds,
    idleTimeout = 1.minute,
    maxLifetime = 5.minutes,
    validationTimeout = 2.seconds,
    readOnly = false,
    connectionInitSql = None
  )

  /** Schema, partitions, rows, refresh — in that order, and per suite instance so the numbers below do not depend on
    * which other suite touched this database last.
    */
  private lazy val prepared: Boolean =
    jdbcUrl.exists { url =>
      val database =
        Database.open(DatabaseConfig(url, EventsPageIT.username, EventsPageIT.password, fixturePool, fixturePool))
      try
        val report = Migrations.migrate(database.write.get())
        val _ = PartitionMaintenance(database.write.get(), PartitionPolicy(3, None, 5.seconds)).run(now.toInstant)
        seed(database)
        val refreshed = RollupRefresh(database.write.get()).run()
        assert(refreshed.exists(_.rows > 0L), "the rollup refresh produced no rows for events that were written")
        report.targetVersion.isDefined
      finally database.close()
    }

  private def seed(database: Database): Unit =
    def force[A](result: Either[String, A]): A = result.fold(message => fail(message), identity)
    def event(id: String, severity: String, minute: Int): NewEvent =
      val envelope = Envelope(
        id = force(EventId(id)),
        source = force(Source("/gateways/overview-it")),
        eventType = force(EventType("com.worxbend.iot.telemetry")),
        time = Some(hour.plusMinutes(minute.toLong)),
        subject = None,
        dataContentType = None,
        schema = None,
        extensions = Map.empty,
        payload = Payload.Structured(
          Json.obj(
            "deviceId" -> Json.fromString("kitchen-1"),
            "severity" -> Json.fromString(severity),
            "value" -> Json.fromDoubleOrNull(21.5)
          )
        )
      )
      force(NewEvent.from(envelope))

    Using.resource(database.write.get().getConnection): connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute("TRUNCATE TABLE events.cloud_event")

    given ExecutionContext = ExecutionContext.parasitic
    val repository = PostgresEventRepository(database.read.transactor, database.write.transactor)
    val events = Vector(event("ov-page-1", "info", 1), event("ov-page-2", "error", 2))
    val _ = Await.result(repository.insertAll(events), 30.seconds)

  private def application(): Application =
    GuiceApplicationBuilder()
      .configure(
        Map[String, Any](
          "database.jdbc-url" -> jdbcUrl.getOrElse(""),
          "database.username" -> EventsPageIT.username,
          "database.password" -> EventsPageIT.password,
          "database.read.maximum-pool-size" -> 4,
          "database.search-dispatcher.thread-pool-executor.fixed-pool-size" -> 4,
          "play.http.secret.key" -> "integration-test-secret-integration-test-secret-0123456789",
          "play.filters.hosts.allowed" -> List("localhost")
        )
      )
      .build()

  test("the landing page is the overview, served from the materialized view"):
    assert(prepared, "the fixture could not be prepared")
    val app = application()
    Helpers.running(app):
      val result = route(app, FakeRequest("GET", Urls.Root)).getOrElse(fail("the overview route is not wired"))
      // It used to be a 302 to /events. That it is a page now is the change, so it is the assertion.
      assertEquals(status(result), Status.OK)
      val document = Jsoup.parse(contentAsString(result))
      assertEquals(document.select("main#main").size(), 1)
      assertEquals(document.select(".tiles .tile").size(), 3)
      assertEquals(document.select("nav.range-picker a").size(), OverviewRange.values.length)
      // The counts are the seeded ones, which is only true if the query really read the refreshed rollup.
      assertEquals(document.select(".tiles .tile .tile-value").first().text(), "2")
      assert(document.select(".volume .histogram-bar").size() > 0, "the volume chart came back empty")
      assert(document.select(".overview-freshness").text().nonEmpty, "the page must state its own staleness")

  test("a wider range still answers, over a bucket width the rollup can be aggregated into"):
    assert(prepared, "the fixture could not be prepared")
    val app = application()
    Helpers.running(app):
      OverviewRange.values.foreach { range =>
        val url = Urls.overview(s"${OverviewRange.Key}=${range.key}")
        val result = route(app, FakeRequest("GET", url)).getOrElse(fail(s"no route for $url"))
        assertEquals(status(result), Status.OK, s"$url did not answer")
        val document = Jsoup.parse(contentAsString(result))
        assertEquals(document.select("nav.range-picker a[aria-current=page]").text(), range.label)
      }

  test("every breakdown link is a search this application can actually serve"):
    assert(prepared, "the fixture could not be prepared")
    val app = application()
    Helpers.running(app):
      val overview = route(app, FakeRequest("GET", Urls.Root)).getOrElse(fail("overview"))
      val document = Jsoup.parse(contentAsString(overview))
      val links = document.select(".panels a[href^=/events], .tiles a[href^=/events]")
      assert(links.size() >= 3, s"expected the overview to link into search, found ${links.size()}")
      // Following them is the only way to know they are not 400s: they are built from a versioned permalink grammar
      // whose rejections are invisible until somebody clicks.
      links.eachAttr("href").forEach { href =>
        val followed = route(app, FakeRequest("GET", href)).getOrElse(fail(s"no route for $href"))
        assertEquals(status(followed), Status.OK, s"the overview linked to a search that failed: $href")
      }

  test("the live tail streams, and its filter goes through the same parser search uses"):
    assert(prepared, "the fixture could not be prepared")
    val app = application()
    Helpers.running(app):
      val ok = route(app, FakeRequest("GET", Urls.live("v=1&severity=%3E%3Derror"))).getOrElse(fail("no live route"))
      assertEquals(status(ok), Status.OK)
      assertEquals(contentType(ok), Some("text/event-stream"))

      val rejected = route(app, FakeRequest("GET", Urls.live("v=1&from=yesterday"))).getOrElse(fail("no live route"))
      assertEquals(status(rejected), Status.BAD_REQUEST)

  test("a tail resumed from an earlier position really delivers rows over the wire"):
    // The only test in the repo that runs the stream. Everything cheaper — the framing, the cursor, the query — is
    // asserted in `TailSuite`; what is left, and what nothing else can reach, is that a materialised Play chunked
    // response actually carries those frames to a reader. A tail that framed perfectly and delivered nothing would
    // pass every other test in this codebase.
    //
    // The cursor is handed in deliberately, pointing before the seeded rows, so the first tick has something to send
    // and the test needs no sleep and no race with the ingest side.
    assert(prepared, "the fixture could not be prepared")
    val app = application()
    Helpers.running(app):
      val query = Query.render(
        Vector(
          "v" -> "1",
          TailCursor.AfterParam -> Rfc3339.render(hour.minusHours(1)),
          TailCursor.AfterUidParam -> TailService.NilUid.toString
        )
      )
      val result = route(app, FakeRequest("GET", Urls.live(query))).getOrElse(fail("no live route"))
      assertEquals(status(result), Status.OK)

      given Materializer = app.materializer
      val firstRow = Await
        .result(result, 30.seconds)
        .body
        .dataStream
        .map(_.utf8String)
        .scan("")(_ + _)
        .dropWhile(!_.contains(s"event: ${TailController.RowEvent}"))
        .runWith(Sink.head)
      // Long enough for several poll intervals; a tail that has sent nothing by then is not slow, it is broken.
      val received = Await.result(firstRow, TailService.PollInterval * 6)

      assert(received.contains("data: <tr"), received)
      assert(received.contains("/gateways/overview-it"), s"the frame carried no seeded event: $received")
