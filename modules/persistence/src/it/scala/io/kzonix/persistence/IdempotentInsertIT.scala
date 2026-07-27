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

import io.circe.Json
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.EventId
import io.kzonix.kernel.event.EventType
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.Source
import io.kzonix.persistence.repository.EventRepository
import io.kzonix.persistence.repository.NewEvent
import io.kzonix.persistence.repository.PostgresEventRepository
import io.kzonix.persistence.repository.SearchRequest
import io.kzonix.persistence.search.SortDirection
import java.time.OffsetDateTime

/** Idempotency of the write path.
  *
  * This is the test that justifies the whole `ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING` design and its
  * unique index. cobalt commits Kafka offsets *after* the durable write, so a crash between the write and the commit
  * replays the batch — which is the normal case, not an edge case. If the replay inserted duplicates, every dashboard
  * would over-count by an amount that varies with how often the consumer restarted.
  *
  * The dedup key is made of *generated* columns, so a producer cannot supply an identity different from the one its own
  * payload carries. That is the property being exercised here: nothing in the writing code chooses the key.
  */
final class IdempotentInsertIT extends PostgresSuite:

  private lazy val repository: EventRepository =
    PostgresEventRepository(database.read.transactor, database.write.transactor)

  private val at = OffsetDateTime.parse("2026-07-20T09:30:00Z")

  override def beforeEach(context: BeforeEach): Unit = truncateEvents()

  private def force[A](result: Either[String, A]): A = result.fold(message => fail(message), identity)

  /** The time an event carries is a function of the event, never of where it sits in a batch.
    *
    * It has to be, because `time` becomes `occurred_at` and `occurred_at` is a third of the dedup key: a redelivered
    * record carries the time it carried the first time. Deriving it from `zipWithIndex` made `b` at position 1 of one
    * batch and position 0 of the next into two genuinely different rows — so the overlapping-replay test measured a
    * fixture artefact and would have gone on passing had `ON CONFLICT` been dropped from the insert entirely.
    */
  private def timeOf(id: String): OffsetDateTime = at.plusSeconds((id.hashCode & 0xffff).toLong)

  private def batch(ids: Vector[String], source: String = "/gateways/1"): Vector[NewEvent] =
    ids.map: id =>
      val envelope = Envelope(
        id = force(EventId(id)),
        source = force(Source(source)),
        eventType = force(EventType("io.kzonix.iot.telemetry")),
        time = Some(timeOf(id)),
        subject = None,
        dataContentType = None,
        schema = None,
        extensions = Map.empty,
        payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("kitchen-0")))
      )
      force(NewEvent.from(envelope))

  test("replaying an identical batch writes nothing and changes no counts"):
    val events = batch(Vector("a", "b", "c"))
    assertEquals(await(repository.insertAll(events)), 3L)
    assertEquals(await(repository.insertAll(events)), 0L)
    assertEquals(await(repository.countAtMost(None, 100)), 3L)

  test("a partially overlapping replay writes only the new records"):
    // The realistic shape of a redelivery: the consumer re-reads from the last committed offset, so the batch is a
    // suffix of one already written plus whatever arrived since.
    assertEquals(await(repository.insertAll(batch(Vector("a", "b")))), 2L)
    assertEquals(await(repository.insertAll(batch(Vector("b", "c", "d")))), 2L)
    assertEquals(await(repository.countAtMost(None, 100)), 4L)

  test("the same id from a different source is a different event"):
    // CloudEvents scopes `id` uniqueness to `source`. Deduplicating on `id` alone would silently drop one of two
    // gateways' events whenever their sequence numbers happened to line up.
    assertEquals(await(repository.insertAll(batch(Vector("a")))), 1L)
    assertEquals(await(repository.insertAll(batch(Vector("a"), source = "/gateways/2"))), 1L)
    assertEquals(await(repository.countAtMost(None, 100)), 2L)

  test("the same id and source at a different time is a second row, because the key carries the partition column"):
    // Not a wart to be fixed later — a consequence to be known. A unique index on a partitioned table must contain
    // the partition key, so the dedup key is (occurred_at, ce_source, ce_id) and not (ce_source, ce_id). Redelivery
    // is therefore free only for a producer that resends the *same* event; one that re-emits an old id with a new
    // `time` gets two rows, and `payload_sha256` is what a cross-partition duplicate hunt would key on instead.
    val first = force(EventId("shifted"))
    def sample(time: OffsetDateTime): NewEvent =
      force(
        NewEvent.from(
          Envelope(
            id = first,
            source = force(Source("/gateways/1")),
            eventType = force(EventType("io.kzonix.iot.telemetry")),
            time = Some(time),
            subject = None,
            dataContentType = None,
            schema = None,
            extensions = Map.empty,
            payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("kitchen-0")))
          )
        )
      )
    assertEquals(await(repository.insertAll(Vector(sample(at)))), 1L)
    assertEquals(await(repository.insertAll(Vector(sample(at.plusSeconds(1))))), 1L)
    assertEquals(await(repository.countAtMost(None, 100)), 2L)

  test("an empty batch is not a database round trip"):
    assertEquals(await(repository.insertAll(Vector.empty)), 0L)

  test("what comes back out is the event that went in, as a domain value"):
    // jsonb is canonical, not byte-verbatim (ADR §12.4): key order and whitespace are gone by the time the row is
    // read. The contract worth asserting is therefore at the domain level — decode(stored) must equal the canonical
    // form of what was encoded — and not at the byte level, which the storage decision has already given up.
    val envelope = Envelope(
      id = force(EventId("round-trip")),
      source = force(Source("/gateways/1")),
      eventType = force(EventType("io.kzonix.iot.telemetry")),
      time = Some(at),
      subject = None,
      dataContentType = None,
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("kitchen-0")))
    )
    assertEquals(await(repository.insertAll(Vector(force(NewEvent.from(envelope))))), 1L)
    val page = await(repository.search(force(SearchRequest.first(None, SortDirection.Newest, 1))))
    val stored = await(repository.find(page.rows.head.ref))
    assertEquals(stored.map(detail => Envelope.decoder.decodeJson(detail.raw)), Some(Right(envelope.canonical)))
