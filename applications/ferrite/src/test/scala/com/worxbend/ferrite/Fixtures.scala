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

import com.typesafe.config.ConfigFactory
import com.worxbend.ferrite.controllers.EventsController
import com.worxbend.ferrite.controllers.OverviewController
import com.worxbend.ferrite.controllers.TailController
import com.worxbend.ferrite.overview.OverviewService
import com.worxbend.ferrite.search.SearchExecutionContext
import com.worxbend.ferrite.search.SearchMetrics
import com.worxbend.ferrite.search.SearchService
import com.worxbend.ferrite.tail.TailService
import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.repository.EventDetail
import com.worxbend.persistence.repository.EventRef
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.EventSummary
import com.worxbend.persistence.repository.FacetDimension
import com.worxbend.persistence.repository.FacetRequest
import com.worxbend.persistence.repository.Facets
import com.worxbend.persistence.repository.FacetValue
import com.worxbend.persistence.repository.HistogramBucket
import com.worxbend.persistence.repository.HistogramRequest
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.OverviewRepository
import com.worxbend.persistence.repository.OverviewRequest
import com.worxbend.persistence.repository.RollupDimension
import com.worxbend.persistence.repository.RollupSlice
import com.worxbend.persistence.repository.RollupTotals
import com.worxbend.persistence.repository.SearchPage
import com.worxbend.persistence.repository.SearchRequest
import com.worxbend.persistence.repository.VolumePoint
import io.circe.Json
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
    ceType: String = "com.worxbend.iot.telemetry",
    ceSource: String = "https://gateway.worxbend.io/hall",
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
      severityRank = severity.flatMap(com.worxbend.kernel.search.Severity.rank).map(_.toShort),
      metricValue = metric
    )

  /** A CloudEvent whose payload the observation registry recognises, so the detail view has something to decode. */
  val rawEvent: Json = io.circe.parser
    .parse(
      """{
        |  "specversion": "1.0",
        |  "id": "evt-1",
        |  "source": "https://gateway.worxbend.io/hall",
        |  "type": "com.worxbend.iot.telemetry",
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
      FacetDimension.Type -> Vector(FacetValue("com.worxbend.iot.telemetry", 12L)),
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

  // ----------------------------------------------------------------------------------------------------- overview

  val volume: Vector[VolumePoint] = Vector(
    VolumePoint(Now.minusHours(2), 0L, 0L),
    VolumePoint(Now.minusHours(1), 5L, 1L),
    VolumePoint(Now, 12L, 0L)
  )

  val slices: Map[RollupDimension, Vector[RollupSlice]] = Map(
    RollupDimension.Type -> Vector(RollupSlice("com.worxbend.iot.telemetry", 12L, 0L)),
    RollupDimension.Source -> Vector(RollupSlice("https://gateway.worxbend.io/hall", 17L, 1L)),
    // `none` is the rollup's own placeholder for an event that carried no severity at all, and it is the case that
    // proves a breakdown row can be un-linkable: the filter grammar has no way to express it.
    RollupDimension.Severity -> Vector(RollupSlice("error", 5L, 5L), RollupSlice("none", 2L, 0L))
  )

  /** Records the requests it is handed, like [[StubRepository]], so a test can assert on the window the web tier
    * computed rather than only on the rendered numbers.
    */
  final class StubOverviewRepository(
    points: Vector[VolumePoint] = Fixtures.volume,
    breakdowns: Map[RollupDimension, Vector[RollupSlice]] = Fixtures.slices,
    totalResult: RollupTotals = RollupTotals(17L, 5L),
    newest: Option[OffsetDateTime] = Some(Fixtures.Now)
  ) extends OverviewRepository:

    val lastRequest: AtomicReference[Option[OverviewRequest]] = AtomicReference(None)
    val asked: AtomicReference[Vector[RollupDimension]] = AtomicReference(Vector.empty)

    def volume(request: OverviewRequest): Future[Vector[VolumePoint]] =
      lastRequest.set(Some(request))
      Future.successful(points)

    def breakdown(dimension: RollupDimension, request: OverviewRequest): Future[Vector[RollupSlice]] =
      val _ = asked.updateAndGet(_ :+ dimension)
      Future.successful(breakdowns.getOrElse(dimension, Vector.empty))

    def totals(request: OverviewRequest): Future[RollupTotals] = Future.successful(totalResult)

    def freshness(): Future[Option[OffsetDateTime]] = Future.successful(newest)

  def overviewService(
    rollup: OverviewRepository = StubOverviewRepository(),
    events: EventRepository = StubRepository()
  ): OverviewService =
    OverviewService(rollup, events, clock)(using searchExecutionContext)

  def overviewController(
    rollup: OverviewRepository = StubOverviewRepository(),
    events: EventRepository = StubRepository()
  ): OverviewController =
    OverviewController(Helpers.stubControllerComponents(), overviewService(rollup, events), clock)(using
      scala.concurrent.ExecutionContext.parasitic
    )

  def tailService(repository: EventRepository = StubRepository()): TailService =
    TailService(repository, clock)(using searchExecutionContext)

  def tailController(service: TailService = tailService()): TailController =
    TailController(Helpers.stubControllerComponents(), service, clock)(using
      scala.concurrent.ExecutionContext.parasitic
    )

  /** The real router over stub controllers, so a routing test exercises the composition rather than a copy of it. */
  def webRouter(repository: EventRepository = StubRepository()): routing.WebRouter =
    routing.WebRouter(controller(repository), overviewController(events = repository), tailController())
