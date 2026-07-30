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

import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.kzonix.ferrite.controllers.EventsController
import io.kzonix.ferrite.search.SearchExecutionContext
import io.kzonix.ferrite.search.SearchMetrics
import io.kzonix.ferrite.search.SearchService
import io.kzonix.kernel.search.Filter
import io.kzonix.persistence.repository.EventDetail
import io.kzonix.persistence.repository.EventRef
import io.kzonix.persistence.repository.EventRepository
import io.kzonix.persistence.repository.EventSummary
import io.kzonix.persistence.repository.FacetDimension
import io.kzonix.persistence.repository.FacetRequest
import io.kzonix.persistence.repository.Facets
import io.kzonix.persistence.repository.FacetValue
import io.kzonix.persistence.repository.HistogramBucket
import io.kzonix.persistence.repository.HistogramRequest
import io.kzonix.persistence.repository.NewEvent
import io.kzonix.persistence.repository.SearchPage
import io.kzonix.persistence.repository.SearchRequest
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Clock
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.apache.pekko.actor.ActorSystem
import play.api.test.Helpers
import scala.concurrent.Future

/** Shared test data and doubles.
  *
  * A hand-written stub rather than a mocking library: this repository has six methods, the stub records what it was
  * asked for, and a test that fails tells you which query the web tier actually built. A mock would tell you that an
  * expectation was unmet.
  */
object Fixtures:

  /** Every relative timestamp in these tests is measured against this instant. A fixed clock is not a convenience — a
    * suite that asserts "3 m ago" against the wall clock fails at midnight and passes on a rerun.
    */
  val Now: OffsetDateTime = OffsetDateTime.parse("2026-07-26T12:00:00Z")

  val clock: Clock = Clock.fixed(Now.toInstant, ZoneOffset.UTC)

  val FirstUid: UUID = UUID.fromString("0f9c1e8a-1c2b-4d3e-8f5a-6b7c8d9e0f11")

  def summary(
    uid: UUID = FirstUid,
    occurredAt: OffsetDateTime = Now.minusMinutes(4),
    ceType: String = "io.kzonix.iot.telemetry",
    ceSource: String = "https://gateway.kzonix.io/hall",
    subject: Option[String] = Some("kitchen-1"),
    severity: Option[String] = Some("warning"),
    metric: Option[Double] = Some(21.5)
  ): EventSummary =
    EventSummary(
      occurredAt = occurredAt,
      eventUid = uid,
      ingestedAt = occurredAt.plusSeconds(1),
      ceId = s"evt-$uid",
      ceSource = ceSource,
      ceType = ceType,
      ceSubject = subject,
      deviceId = Some("kitchen-1"),
      roomId = Some("kitchen"),
      personId = None,
      severity = severity,
      severityRank = severity.flatMap(io.kzonix.kernel.search.Severity.rank).map(_.toShort),
      metricValue = metric
    )

  /** A CloudEvent whose payload the observation registry recognises, so the detail view has something to decode. */
  val rawEvent: Json = io.circe.parser
    .parse(
      """{
        |  "specversion": "1.0",
        |  "id": "evt-1",
        |  "source": "https://gateway.kzonix.io/hall",
        |  "type": "io.kzonix.iot.telemetry",
        |  "subject": "kitchen-1",
        |  "time": "2026-07-26T11:56:00Z",
        |  "datacontenttype": "application/json",
        |  "tenantid": "acme",
        |  "data": { "metric": "temperature", "value": 21.5, "unit": "C", "deviceId": "kitchen-1" }
        |}""".stripMargin
    )
    .getOrElse(Json.obj())

  /** A payload carrying markup, used to prove Twirl's escaping is not defeated anywhere. */
  val hostileSubject: String = """<script>alert("xss")</script>"""

  val facets: Facets = Facets(
    dimensions = Map(
      FacetDimension.Type -> Vector(FacetValue("io.kzonix.iot.telemetry", 12L)),
      FacetDimension.Device -> Vector(FacetValue("kitchen-1", 9L), FacetValue("hall-2", 3L)),
      FacetDimension.Severity -> Vector(FacetValue("warn", 4L))
    ),
    tags = Vector(FacetValue("indoor", 7L)),
    candidates = 12L,
    capped = false
  )

  val buckets: Vector[HistogramBucket] = Vector(
    HistogramBucket(Now.minusHours(2), 0L),
    HistogramBucket(Now.minusHours(1), 5L),
    HistogramBucket(Now, 12L)
  )

  /** Records every request it is handed, so a test can assert on the query the web tier *built* rather than only on the
    * HTML that came back.
    */
  final class StubRepository(
    rows: Vector[EventSummary] = Vector(summary()),
    nextCursor: Option[String] = None,
    detail: Option[EventDetail] = None,
    total: Long = 12L,
    facetResult: Facets = Fixtures.facets
  ) extends EventRepository:

    val lastSearch: AtomicReference[Option[SearchRequest]] = AtomicReference(None)
    val lastFacets: AtomicReference[Option[FacetRequest]] = AtomicReference(None)
    val lastHistogram: AtomicReference[Option[HistogramRequest]] = AtomicReference(None)
    val lastCount: AtomicReference[Option[(Option[Filter], Int)]] = AtomicReference(None)
    val lastRef: AtomicReference[Option[EventRef]] = AtomicReference(None)

    def search(request: SearchRequest): Future[SearchPage] =
      lastSearch.set(Some(request))
      Future.successful(SearchPage(rows, nextCursor))

    def facets(request: FacetRequest): Future[Facets] =
      lastFacets.set(Some(request))
      Future.successful(facetResult)

    def histogram(request: HistogramRequest): Future[Vector[HistogramBucket]] =
      lastHistogram.set(Some(request))
      Future.successful(buckets)

    def find(ref: EventRef): Future[Option[EventDetail]] =
      lastRef.set(Some(ref))
      Future.successful(detail)

    def countAtMost(filter: Option[Filter], cap: Int): Future[Long] =
      lastCount.set(Some((filter, cap)))
      Future.successful(total)

    /** ferrite never writes. A stub that quietly returned 0 would let a write path be introduced unnoticed. */
    def insertAll(events: Vector[NewEvent]): Future[Long] =
      Future.failed(UnsupportedOperationException("ferrite is a reader"))

  /** One actor system for the whole suite run.
    *
    * It is built from the real `application.conf`, which is what proves `database.search-dispatcher` exists and is
    * spelled the way [[SearchExecutionContext]] looks it up — a typo there would silently fall back to the default
    * dispatcher in production, and this is the cheapest place to catch it.
    */
  lazy val system: ActorSystem = ActorSystem("ferrite-suite", ConfigFactory.load())

  lazy val searchExecutionContext: SearchExecutionContext = SearchExecutionContext(system)

  /** A throwaway registry per service, so a suite can assert on the meters one search emitted without another test's
    * searches already being counted in it.
    */
  def service(repository: EventRepository, registry: MeterRegistry = SimpleMeterRegistry()): SearchService =
    SearchService(repository, SearchMetrics(registry), clock)(using searchExecutionContext)

  def controller(repository: EventRepository): EventsController =
    EventsController(Helpers.stubControllerComponents(), service(repository), clock)(using
      scala.concurrent.ExecutionContext.parasitic
    )
