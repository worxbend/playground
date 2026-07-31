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
import com.worxbend.ferrite.tail.TailBatch
import com.worxbend.ferrite.tail.TailCursor
import com.worxbend.ferrite.tail.TailService
import com.worxbend.ferrite.web.Urls
import com.worxbend.kernel.Rfc3339
import com.worxbend.persistence.search.SortDirection
import java.util.UUID
import munit.FunSuite
import org.apache.pekko.stream.Materializer
import org.jsoup.Jsoup
import play.api.http.Status
import play.api.mvc.AnyContentAsEmpty
import play.api.test.FakeRequest
import play.api.test.Helpers
import play.api.test.Helpers.contentAsString
import play.api.test.Helpers.contentType
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.header
import play.api.test.Helpers.status
import play.api.test.Helpers.writeableOf_AnyContentAsEmpty
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/** The live tail.
  *
  * The stream itself is a ticker on a two-second interval, so nothing here waits for it — that would buy one weak
  * assertion for four seconds of wall clock on every CI run. What is tested instead is everything that can be wrong
  * *without* a socket: the cursor grammar, the query the poll builds, the SSE framing, the capacity guard, and the
  * statuses the endpoint answers with. Each of those is a defect that is otherwise invisible until a browser is open.
  */
final class TailSuite extends FunSuite:

  private given Materializer = Materializer(Fixtures.system)

  private def get(url: String): FakeRequest[AnyContentAsEmpty.type] = FakeRequest("GET", url)

  private val at = Fixtures.Now.minusMinutes(1)

  // ----------------------------------------------------------------------------------------------------- cursors

  test("a cursor needs both halves of the primary key"):
    val uid = UUID.randomUUID()
    assertEquals(
      TailCursor.parse(Some(Rfc3339.render(at)), Some(uid.toString)),
      Some(TailCursor(at, uid))
    )
    // Half a cursor is not nearly a position: completing it would mean inventing a uid, and the two possible
    // inventions differ by whether the row at that exact timestamp is re-sent or skipped.
    assertEquals(TailCursor.parse(Some(Rfc3339.render(at)), None), None)
    assertEquals(TailCursor.parse(None, Some(uid.toString)), None)
    assertEquals(TailCursor.parse(Some("yesterday"), Some(uid.toString)), None)
    assertEquals(TailCursor.parse(Some(Rfc3339.render(at)), Some("not-a-uuid")), None)

  // ------------------------------------------------------------------------------------------------- the polling

  test("a poll is an ascending keyset page, capped, with the cursor in it"):
    val repository = Fixtures.StubRepository()
    val service = Fixtures.tailService(repository)
    val cursor = TailCursor(at, UUID.randomUUID())
    val _ = Await.result(service.poll(None, cursor), 5.seconds)
    val request = repository.lastSearch.get().getOrElse(fail("the tail never queried anything"))
    // Ascending, not descending. A descending query capped at fifty returns the NEWEST fifty and skips everything
    // between them and the cursor — silent data loss during exactly the burst somebody opened the tail to watch.
    assertEquals(request.sort, SortDirection.Oldest)
    assertEquals(request.limit, TailService.MaxRowsPerTick)
    assertEquals(request.cursor.map(_.occurredAt), Some(cursor.occurredAt))
    assertEquals(request.cursor.map(_.eventUid), Some(cursor.eventUid))

  test("the cursor advances to the last row of the batch, so no row is sent twice and none is skipped"):
    val oldest = Fixtures.summary(uid = UUID.fromString("00000000-0000-4000-8000-000000000001"), occurredAt = at)
    val newest =
      Fixtures.summary(uid = UUID.fromString("00000000-0000-4000-8000-000000000002"), occurredAt = at.plusSeconds(30))
    val service = Fixtures.tailService(Fixtures.StubRepository(rows = Vector(oldest, newest)))
    val batch = Await.result(service.poll(None, TailCursor(at.minusMinutes(5), TailService.NilUid)), 5.seconds)
    assertEquals(batch.rows.size, 2)
    assertEquals(batch.cursor, TailCursor(newest.occurredAt, newest.eventUid))

  test("an empty batch leaves the cursor exactly where it was"):
    val service = Fixtures.tailService(Fixtures.StubRepository(rows = Vector.empty))
    val cursor = TailCursor(at, UUID.randomUUID())
    assertEquals(Await.result(service.poll(None, cursor), 5.seconds), TailBatch(cursor, Vector.empty))

  test("a tail with no client position starts after the newest matching row, not at the wall clock"):
    // Starting at `now` skips every event whose occurred_at is a few seconds behind because of ingest lag, so a tail
    // opened on a busy system shows nothing for its first seconds and looks broken.
    val row = Fixtures.summary(occurredAt = at)
    val repository = Fixtures.StubRepository(rows = Vector(row))
    val service = Fixtures.tailService(repository)
    assertEquals(Await.result(service.head(None), 5.seconds), TailCursor(row.occurredAt, row.eventUid))
    val request = repository.lastSearch.get().getOrElse(fail("the head lookup never ran"))
    assertEquals(request.sort, SortDirection.Newest)
    assertEquals(request.limit, 1)

  test("a tail over an empty result set starts at the clock"):
    val service = Fixtures.tailService(Fixtures.StubRepository(rows = Vector.empty))
    assertEquals(Await.result(service.head(None), 5.seconds), TailCursor(Fixtures.Now, TailService.NilUid))

  // -------------------------------------------------------------------------------------------------- SSE frames

  test("a row is framed as a `row` event carrying the rendered table row, keyed by uid"):
    val summary = Fixtures.summary()
    val frames = TailController.frames(TailBatch(TailCursor(at, summary.eventUid), Vector(summary)), Fixtures.Now)
    assertEquals(frames.size, 1)
    val frame = frames.head
    assertEquals(frame.name, Some(TailController.RowEvent))
    assertEquals(frame.id, Some(summary.eventUid.toString))
    // The payload is markup the SERVER rendered, from the same template the search page uses — so a live row and a
    // searched row are the same row, and the client never templates a device-supplied string.
    val row = Jsoup.parseBodyFragment(s"<table><tbody>${frame.data.getOrElse("")}</tbody></table>")
    assertEquals(row.select("tr.event-row").size(), 1)
    assertEquals(row.select("tr.event-row").attr("data-uid"), summary.eventUid.toString)
    assert(row.select("tr.event-row").attr("data-at").nonEmpty, "the row must carry the other half of the cursor")

  test("the framed event is valid SSE: an event name and every line of the body as a data field"):
    val summary = Fixtures.summary()
    val formatted = TailController.frames(TailBatch(TailCursor(at, summary.eventUid), Vector(summary)), Fixtures.Now)
      .head
      .formatted
    assert(formatted.contains(s"event: ${TailController.RowEvent}"), formatted)
    assert(formatted.contains("data: "), formatted)
    // A frame that does not end in a blank line is a frame the browser never delivers.
    assert(formatted.endsWith("\n\n"), formatted.takeRight(10))

  test("an empty tick is a heartbeat, so a dead connection is distinguishable from a quiet one"):
    val frames = TailController.frames(TailBatch(TailCursor(at, TailService.NilUid), Vector.empty), Fixtures.Now)
    assertEquals(frames.map(_.name), Vector(Some(TailController.HeartbeatEvent)))
    assertEquals(frames.head.data, Some(Rfc3339.render(Fixtures.Now)))

  test("a carriage return in the payload cannot truncate a frame"):
    // SSE is a line protocol; a bare \r ends a field early. Twirl emits \n today, so this guards a template file ever
    // being saved with CRLF endings — a change nobody would connect to a broken live tail.
    assertEquals(TailController.sseSafe("<tr>\r\n  <td>a</td>\r</tr>\r\n"), "<tr>\n  <td>a</td>\n</tr>")

  // ------------------------------------------------------------------------------------------------- the endpoint

  test("the stream answers as an event stream that nothing is allowed to cache or buffer"):
    val controller = Fixtures.tailController()
    val result = Helpers.call(controller.stream, get(Urls.live("v=1&severity=%3E%3Derror")))
    assertEquals(status(result), Status.OK)
    assertEquals(contentType(result), Some("text/event-stream"))
    assertEquals(header("Cache-Control", result), Some("no-store"))
    // nginx holds a chunked response until it ends unless told otherwise, and for a tail it never ends.
    assertEquals(header("X-Accel-Buffering", result), Some("no"))

  test("the position parameters are not part of the filter grammar and must not reject the stream"):
    val controller = Fixtures.tailController()
    val url = Urls.live(s"v=1&after=${Rfc3339.render(at)}&afterUid=${UUID.randomUUID()}")
    assertEquals(status(Helpers.call(controller.stream, get(url))), Status.OK)

  test("a malformed filter is a plain-text 400, because EventSource cannot render a page"):
    val controller = Fixtures.tailController()
    val result = Helpers.call(controller.stream, get(Urls.live("v=1&from=yesterday")))
    assertEquals(status(result), Status.BAD_REQUEST)
    assert(contentType(result).contains("text/plain"), contentType(result).toString)
    assert(!contentAsString(result).contains("<html"), "an event-stream client has nowhere to put a document")

  test("the capacity guard refuses a tail rather than letting the read pool starve search"):
    val service = Fixtures.tailService()
    (1 to TailService.MaxConcurrent).foreach(_ => service.opened())
    assertEquals(service.openTails, TailService.MaxConcurrent)
    assert(!service.hasCapacity)

    val result = Helpers.call(Fixtures.tailController(service).stream, get(Urls.live("")))
    assertEquals(status(result), Status.SERVICE_UNAVAILABLE)
    assertEquals(contentAsString(result), TailController.BusyMessage)

    service.closed()
    assert(service.hasCapacity, "closing a tail must free the slot it held")
    assertEquals(status(Helpers.call(Fixtures.tailController(service).stream, get(Urls.live("")))), Status.OK)

  test("a stream that is answered but never consumed holds no slot"):
    // The counter is kept by the materialised stream, not by the action, precisely so a client that vanishes between
    // the headers and the first byte cannot leak a slot that only a restart would reclaim.
    val service = Fixtures.tailService()
    val result = Helpers.call(Fixtures.tailController(service).stream, get(Urls.live("")))
    assertEquals(status(result), Status.OK)
    assertEquals(service.openTails, 0)
