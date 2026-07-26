package io.kzonix.persistence

import io.circe.Json
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.EventId
import io.kzonix.kernel.event.EventType
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.Source
import io.kzonix.kernel.event.Subject
import io.kzonix.kernel.search.Filter
import io.kzonix.persistence.repository.EventRepository
import io.kzonix.persistence.repository.FacetDimension
import io.kzonix.persistence.repository.FacetRequest
import io.kzonix.persistence.repository.HistogramRequest
import io.kzonix.persistence.repository.NewEvent
import io.kzonix.persistence.repository.PostgresEventRepository
import io.kzonix.persistence.repository.SearchRequest
import io.kzonix.persistence.search.SortDirection
import java.time.OffsetDateTime

/** The repository against a real database.
  *
  * The point of these tests is the half of the design that unit tests structurally cannot reach: the generated columns.
  * `device_id`, `severity_rank`, `tags` and `search_doc` are computed by PostgreSQL from `raw`, so an event written
  * through [[NewEvent]] and read back through the repository is the only way to find out whether the JSON the domain
  * produces and the paths the DDL extracts actually agree.
  */
final class EventRepositoryIT extends PostgresSuite:

  private lazy val repository: EventRepository =
    PostgresEventRepository(database.read.transactor, database.write.transactor)

  private val base = OffsetDateTime.parse("2026-07-15T12:00:00Z")

  override def beforeEach(context: BeforeEach): Unit = truncateEvents()

  private def force[A](result: Either[String, A]): A =
    result.fold(message => fail(message), identity)

  /** Builds a real [[Envelope]] rather than hand-writing the jsonb, so the round trip covers the kernel's encoder as
    * well as the DDL's extraction paths — the two things that have to agree and are written in different languages.
    */
  private def event(
    index: Int,
    device: String,
    severity: String,
    value: Double,
    at: OffsetDateTime
  ): NewEvent =
    val envelope = Envelope(
      id = force(EventId(s"evt-$index")),
      source = force(Source("/gateways/1")),
      eventType = force(EventType("io.kzonix.iot.telemetry")),
      time = Some(at),
      subject = Some(force(Subject(device))),
      dataContentType = Some(ContentType("application/json").getOrElse(ContentType.OctetStream)),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(
        Json.obj(
          "deviceId" -> Json.fromString(device),
          "roomId" -> Json.fromString("kitchen"),
          "severity" -> Json.fromString(severity),
          "value" -> Json.fromDoubleOrNull(value),
          "tags" -> Json.arr(Json.fromString("indoor"), Json.fromString("hvac")),
          "message" -> Json.fromString("temperature reading taken")
        )
      )
    )
    force(NewEvent.from(envelope))

  private def seed(count: Int): Long =
    val events = (0 until count).toVector.map: index =>
      event(
        index,
        s"kitchen-${index % 3}",
        if index % 5 == 0 then "error" else "info",
        20.0 + index,
        base.plusMinutes(index.toLong)
      )
    await(repository.insertAll(events))

  test("an event written through the repository comes back with its generated columns populated"):
    val _ = seed(1)
    val page = await(repository.search(force(SearchRequest.first(None, SortDirection.Newest, 10))))
    assertEquals(page.rows.size, 1)
    val row = page.rows.head
    assertEquals(row.ceType, "io.kzonix.iot.telemetry")
    assertEquals(row.ceSource, "/gateways/1")
    assertEquals(row.deviceId, Some("kitchen-0"))
    assertEquals(row.roomId, Some("kitchen"))
    assertEquals(row.severity, Some("error"))
    assertEquals(row.severityRank, Some(50.toShort))
    assertEquals(row.metricValue, Some(20.0))

  test("the detail projection carries the payload the list projection deliberately omits"):
    val _ = seed(1)
    val page = await(repository.search(force(SearchRequest.first(None, SortDirection.Newest, 1))))
    val detail = await(repository.find(page.rows.head.ref))
    assert(detail.isDefined)
    assertEquals(detail.map(_.raw.hcursor.get[String]("type").toOption), Some(Some("io.kzonix.iot.telemetry")))

  test("keyset pagination walks the whole result set exactly once"):
    // The property that OFFSET cannot give: no row seen twice, none skipped, and the same total however many pages
    // it takes. The tiebreaker on event_uid is what makes it hold when timestamps collide.
    val total = 25
    val _ = seed(total)
    def walk(request: SearchRequest, seen: Vector[String]): Vector[String] =
      val page = await(repository.search(request))
      val ids = seen ++ page.rows.map(_.ceId)
      page.nextCursor match
        case None         => ids
        case Some(cursor) =>
          walk(force(SearchRequest.of(None, SortDirection.Newest, 7, Some(cursor))), ids)
    val ids = walk(force(SearchRequest.first(None, SortDirection.Newest, 7)), Vector.empty)
    assertEquals(ids.size, total)
    assertEquals(ids.distinct.size, total)

  test("a filter reaches the database as parameters and selects the rows it names"):
    val _ = seed(10)
    val filter = force(Filter.deviceIn(Vector("kitchen-1")))
    val request = force(SearchRequest.first(Some(filter), SortDirection.Newest, 50))
    val page = await(repository.search(request))
    assert(page.rows.nonEmpty)
    assert(page.rows.forall(_.deviceId.contains("kitchen-1")), page.rows.map(_.deviceId).toString)

  test("hostile input in a filter selects nothing and does not execute anything"):
    // If the payload were spliced into the statement, this test would not fail — the table would be gone.
    val _ = seed(3)
    val filter = force(Filter.deviceIn(Vector("'; DROP TABLE events.cloud_event; --")))
    val page = await(repository.search(force(SearchRequest.first(Some(filter), SortDirection.Newest, 50))))
    assertEquals(page.rows.size, 0)
    assertEquals(await(repository.countAtMost(None, 100)), 3L)

  test("facets count within the current selection and report whether the cap was reached"):
    val _ = seed(9)
    val facets = await(repository.facets(force(FacetRequest.of(None, 1000, 10))))
    assertEquals(facets.capped, false)
    assertEquals(facets.candidates, 9L)
    assertEquals(
      facets.dimensions.get(FacetDimension.Type).map(_.map(_.value)),
      Some(Vector("io.kzonix.iot.telemetry"))
    )
    assertEquals(facets.dimensions.get(FacetDimension.Device).map(_.size), Some(3))
    assertEquals(facets.tags.map(_.value).sorted, Vector("hvac", "indoor"))

  test("a facet cap is reported rather than silently truncating the counts"):
    val _ = seed(9)
    val facets = await(repository.facets(force(FacetRequest.of(None, 4, 10))))
    assert(facets.capped, "the cap was reached but not reported, so the UI would print an exact-looking number")
    assertEquals(facets.candidates, 4L)

  test("the histogram materialises empty buckets instead of collapsing them"):
    val _ = await(repository.insertAll(Vector(event(1, "kitchen-0", "info", 21.0, base))))
    val request = force(HistogramRequest.of(None, base.minusHours(6), base.plusHours(6)))
    val buckets = await(repository.histogram(request))
    assert(buckets.sizeIs > 1, buckets.toString)
    assertEquals(buckets.map(_.count).sum, 1L)
    assert(buckets.exists(_.count == 0L), "every bucket was non-empty; the generate_series skeleton is not working")

  test("a bounded count stops at the cap plus one so the UI can render a plus sign"):
    val _ = seed(12)
    assertEquals(await(repository.countAtMost(None, 5)), 6L)
    assertEquals(await(repository.countAtMost(None, 100)), 12L)
