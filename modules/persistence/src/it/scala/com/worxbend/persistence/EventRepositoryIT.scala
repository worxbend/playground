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

package com.worxbend.persistence

import com.worxbend.kernel.event.AttrValue
import com.worxbend.kernel.event.ContentType
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.kernel.event.Subject
import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.NumOp
import com.worxbend.kernel.search.Severity
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.FacetDimension
import com.worxbend.persistence.repository.FacetRequest
import com.worxbend.persistence.repository.HistogramRequest
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.PostgresEventRepository
import com.worxbend.persistence.repository.SearchRequest
import com.worxbend.persistence.search.SortDirection
import io.circe.Json
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
      eventType = force(EventType("com.worxbend.iot.telemetry")),
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

  /** An event carrying CloudEvents extension attributes, which the seed events deliberately do not. */
  private def tenanted: NewEvent =
    val envelope = Envelope(
      id = force(EventId("evt-tenanted")),
      source = force(Source("/gateways/1")),
      eventType = force(EventType("com.worxbend.iot.telemetry")),
      time = Some(base.plusHours(1)),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map("tenantid" -> AttrValue.Text("acme"), "sequence" -> AttrValue.Num(7)),
      payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("kitchen-9")))
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
    assertEquals(row.ceType, "com.worxbend.iot.telemetry")
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
    assertEquals(detail.map(_.raw.hcursor.get[String]("type").toOption), Some(Some("com.worxbend.iot.telemetry")))

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

  /** Every leaf of the filter compiler, executed.
    *
    * These are the branches whose *shape* the fast tier can assert and whose *legality* it cannot: `@??`, `@>` against
    * `text[]` and `jsonb`, `websearch_to_tsquery` against the `search_doc` weights, and `extensions ->>` against the
    * generated `raw - '{…}'` column. A parse error or an unresolvable operator in any of them is a 500 on a filter a
    * user typed, and until this suite ran there was nothing anywhere that had sent one to a server.
    */
  private def matching(filter: Filter): Vector[String] =
    await(repository.search(force(SearchRequest.first(Some(filter), SortDirection.Newest, 50)))).rows.map(_.ceId)

  test("a numeric payload comparison compiles to a jsonpath the server accepts"):
    // `data @?? ?::jsonpath`: `??` is pgjdbc's escape for a literal `?`, because the jsonb `@?` operator is otherwise
    // indistinguishable from a bind placeholder. If the escape were wrong the driver would either report a parameter
    // count mismatch or send `@??` to the server as an unknown operator — neither is visible without a server.
    val _ = seed(5) // values 20.0 .. 24.0
    assertEquals(
      matching(force(Filter.payloadCmp("value", NumOp.Gte, BigDecimal(23)))).sorted,
      Vector("evt-3", "evt-4")
    )
    assertEquals(matching(force(Filter.payloadCmp("value", NumOp.Lt, BigDecimal(21)))), Vector("evt-0"))
    assertEquals(matching(force(Filter.payloadCmp("value", NumOp.Eq, BigDecimal(22)))), Vector("evt-2"))
    // A path no event carries is an empty result, never an error.
    assertEquals(matching(force(Filter.payloadCmp("nosuch", NumOp.Gt, BigDecimal(0)))), Vector.empty)

  test("tag containment, payload containment and extension equality all resolve against their indexes"):
    val _ = seed(3)
    val _ = await(repository.insertAll(Vector(tenanted)))
    assertEquals(matching(force(Filter.tagsAll(Vector("indoor", "hvac")))).size, 3)
    assertEquals(matching(force(Filter.tagsAll(Vector("outdoor")))), Vector.empty)
    val containment = Json.obj("roomId" -> Json.fromString("kitchen"))
    assertEquals(matching(force(Filter.payloadContains(containment))).size, 3)
    // `extensions` is the generated `raw - '{specversion,id,…}'` column: this asserts that the reserved-attribute
    // list in the DDL and `Envelope.ReservedAttributes` agree about which keys are extensions and which are not.
    assertEquals(matching(force(Filter.extensionEq("tenantid", "acme"))), Vector("evt-tenanted"))
    assertEquals(matching(force(Filter.extensionEq("tenantid", "other"))), Vector.empty)
    // A reserved attribute is NOT an extension, however it is spelled in the filter.
    assertEquals(matching(force(Filter.extensionEq("type", "com.worxbend.iot.telemetry"))), Vector.empty)

  test("free text runs through websearch_to_tsquery against the generated search_doc"):
    val _ = seed(2)
    // Weight C is the 'english' prose field, so the stemmed form of a word in `message` must match.
    assert(matching(force(Filter.fullText("temperature"))).nonEmpty, "the prose weight is not searchable")
    // Weight A is the 'simple' type field: a reverse-DNS type must match verbatim and must not be stemmed away.
    assert(matching(force(Filter.fullText("com.worxbend.iot.telemetry"))).nonEmpty, "the type weight is not searchable")
    // websearch_to_tsquery never raises on junk, which is why it and not to_tsquery: this is a 0-row page, not a 500.
    assertEquals(matching(force(Filter.fullText("&&& |"))), Vector.empty)

  test("severity, time bounds and boolean composition survive a real planner"):
    val _ = seed(10) // index % 5 == 0 is "error" (rank 50), everything else "info" (rank 20)
    assertEquals(matching(Filter.severityAtLeast(Severity.Warn)).sorted, Vector("evt-0", "evt-5"))
    val window = force(Filter.occurred(Some(base), Some(base.plusMinutes(3))))
    assertEquals(matching(window).sorted, Vector("evt-0", "evt-1", "evt-2"))
    val both = force(Filter.and(Vector(window, Filter.severityAtLeast(Severity.Warn))))
    assertEquals(matching(both), Vector("evt-0"))
    // kitchen-1 is index % 3 == 1 (1, 4, 7); the alerts are 0 and 5. The union is what `Sql.parens` exists for:
    // without the parentheses around each branch, `NOT a OR b` and `a OR b AND c` would bind the wrong way.
    val device = force(Filter.deviceIn(Vector("kitchen-1")))
    val either = force(Filter.or(Vector(device, Filter.severityAtLeast(Severity.Warn))))
    assertEquals(matching(either).sorted, Vector("evt-0", "evt-1", "evt-4", "evt-5", "evt-7"))
    assertEquals(matching(Filter.not(window)).size, 7)

  test("facets count within the current selection and report whether the cap was reached"):
    val _ = seed(9)
    val facets = await(repository.facets(force(FacetRequest.of(None, 1000, 10))))
    assertEquals(facets.capped, false)
    assertEquals(facets.candidates, 9L)
    assertEquals(
      facets.dimensions.get(FacetDimension.Type).map(_.map(_.value)),
      Some(Vector("com.worxbend.iot.telemetry"))
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
