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

package com.worxbend.ferrite.controllers

import com.worxbend.ferrite.search.SearchQuery
import com.worxbend.ferrite.tail.TailBatch
import com.worxbend.ferrite.tail.TailCursor
import com.worxbend.ferrite.tail.TailService
import com.worxbend.ferrite.web.Query
import com.worxbend.ferrite.web.view.Presenter
import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.search.Filter
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import play.api.libs.EventSource
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** The live tail: Server-Sent Events carrying server-rendered rows.
  *
  * **SSE, not WebSockets and not polling from the browser.** The traffic is one-directional, it is text, and it has to
  * survive a proxy — which is the whole of what `text/event-stream` is for. Play ships `EventSource` so this costs no
  * dependency, and the browser's `EventSource` reconnects on its own, which is a reconnect policy nobody has to write.
  *
  * **The payload is HTML, not JSON.** Each event carries the same `<tr>` the search page renders, from the same
  * template, so a row that arrives live is identical to a row that arrives from a search — no second renderer, no
  * client-side templating of device-supplied strings, and Twirl's escaping applies to both. That is also what keeps
  * this inside ADR §8.4's rule about Alpine: the client inserts markup the server produced and never composes any.
  *
  * **Three bounds, because an unbounded tail takes down search.** The read pool has eight connections and a two-second
  * `statement_timeout`, and every connected tab is a repeating query:
  *
  *   - one query per tail per `TailService.PollInterval`, and it is a keyset seek, not a scan;
  *   - at most `TailService.MaxRowsPerTick` rows per tick, so a burst arrives over several ticks instead of one
  *     response that serialises everything;
  *   - at most `TailService.MaxConcurrent` tails per replica, and the next one is refused with a `503` naming the
  *     reason. Shedding here is visible; shedding by exhausting the pool is a slow search page nobody can explain.
  *
  * The stream ends when the client goes away: Play cancels the source, the ticker stops, and `watchTermination` returns
  * the slot. Pausing in the browser **closes the connection** rather than ignoring its messages, so a paused tail costs
  * nothing at all.
  *
  * **On `http.server.requests`.** This route is timed like every other, deliberately. `MetricsFilter` records when the
  * `Result` is produced — for a chunked response that is when the headers are ready, not when the body finishes — so an
  * hour-long tail contributes one short observation at connect and does not distort the latency histogram. Adding it to
  * `Meters.UninstrumentedPaths` would have thrown away the one signal that says how many tails were opened and how many
  * were refused; the tag is a route template of its own so it can be read apart from search.
  */
@Singleton
final class TailController @Inject() (cc: ControllerComponents, service: TailService, clock: Clock)(using
  ec: ExecutionContext
) extends AbstractController(cc):

  def stream: Action[AnyContent] = Action: request =>
    val pairs = Query.parse(request.rawQueryString)
    val cursor = TailCursor.parse(single(pairs, TailCursor.AfterParam), single(pairs, TailCursor.AfterUidParam))
    // The position parameters are peeled off before the filter is parsed. `FilterQuery` owns the filter grammar and
    // reports anything it does not know as an unknown parameter, so leaving them in would 400 every stream.
    val filterQuery =
      Query.render(Query.remove(Query.remove(pairs, TailCursor.AfterParam), TailCursor.AfterUidParam))

    SearchQuery.parse(filterQuery) match
      case Left(errors) =>
        // Plain text, not the HTML failure page: the only caller is `EventSource`, which cannot render a document and
        // whose `onerror` hands the page no body at all. The status is what the client acts on.
        BadRequest(errors.map(_.message).mkString("\n")).as(TEXT)
      case Right(_) if !service.hasCapacity => busy
      case Right(query)                     =>
        Ok.chunked(tailSource(query.filter, cursor))
          // A cached event stream is a contradiction, and `X-Accel-Buffering` is the one header nginx needs before it
          // will forward chunks instead of holding them until the response ends — which for a tail is never.
          .withHeaders(CACHE_CONTROL -> "no-store", "X-Accel-Buffering" -> "no")

  /** The stream: a lazily started ticker, a stateful poll, and one frame per row.
    *
    * **`lazyFutureSource` is what makes the concurrency count correct.** Everything inside it — resolving the starting
    * position and claiming a slot — happens when the response body is materialised, which is also the only thing that
    * can terminate and give the slot back. Claiming in the action instead would leak a slot for good on any request
    * whose body is never run, and an unreclaimable slot is only fixed by a restart.
    *
    * `scanAsync` keeps the cursor honest: it threads the position through and never runs two futures at once, so a tick
    * can neither overlap the query before it nor advance the cursor past rows that query has not returned yet. Its seed
    * is emitted before anything is polled, so it is dropped.
    *
    * `Source.tick` drops a tick when the downstream is not ready rather than queueing it, which is the backpressure
    * this feature wants: a slow client falls behind in *time*, never in a growing buffer in the server's heap.
    */
  private def tailSource(filter: Option[Filter], cursor: Option[TailCursor]): Source[EventSource.Event, ?] =
    // A stream can be cancelled before the lazy factory ever runs — a client that disconnects between the headers and
    // the first demand. The flag is what keeps the count from going negative in that case, and it makes the release
    // idempotent so the same termination cannot be counted twice.
    val counted = AtomicBoolean(false)
    Source
      .lazyFutureSource { () =>
        counted.set(true)
        service.opened()
        // No cursor means "start after whatever is newest right now", which costs one query and is the difference
        // between a tail that looks broken for its first seconds and one that does not. See `TailService.head`.
        cursor.fold(service.head(filter))(Future.successful).map { start =>
          Source
            .tick(TailService.PollInterval, TailService.PollInterval, ())
            .scanAsync(TailBatch(start, Vector.empty))((batch, _) => service.poll(filter, batch.cursor))
            .drop(1)
            .mapConcat(frames)
        }
      }
      .watchTermination() { (_, done) =>
        done.onComplete(_ => if counted.getAndSet(false) then service.closed())
        NotUsed
      }

  private def frames(batch: TailBatch): Vector[EventSource.Event] =
    TailController.frames(batch, OffsetDateTime.now(clock))

  private def single(pairs: Vector[(String, String)], key: String): Option[String] =
    pairs.collectFirst { case (k, v) if k == key => v }

  /** The refusal. `503` and not `429`: this is a capacity limit on one replica, not a quota on the caller, and a client
    * that retries after a tab is closed will succeed.
    */
  private def busy: play.api.mvc.Result =
    ServiceUnavailable(TailController.BusyMessage).as(TEXT).withHeaders(CACHE_CONTROL -> "no-store")

object TailController:

  /** The SSE event names. The client switches on them, so they are values rather than literals in two files. */
  val RowEvent: String = "row"
  val HeartbeatEvent: String = "heartbeat"

  /** One tick as SSE frames.
    *
    * Pure, and in the companion so it can be asserted on without a stream, a clock or a socket — the framing is where
    * this feature is most likely to break silently, because a malformed frame is not an error anywhere: the browser
    * simply never fires the event.
    *
    * **An empty tick still emits something.** The heartbeat is what lets the page tell "connected and quiet" apart from
    * "the connection died and the browser has not noticed yet", and it doubles as the keep-alive that stops an idle
    * proxy closing the stream. Thirty bytes every couple of seconds is the cheapest honest answer available.
    *
    * The `id` is the event's uid, which is what the client de-duplicates on — and what the browser sends back as
    * `Last-Event-ID` when it reconnects on its own.
    */
  def frames(batch: TailBatch, now: OffsetDateTime): Vector[EventSource.Event] =
    if batch.rows.isEmpty then Vector(EventSource.Event(Rfc3339.render(now), None, Some(HeartbeatEvent)))
    else
      batch.rows.map { summary =>
        val html = views.html.fragments.rows(Vector(Presenter.row(summary, now)), None).body
        EventSource.Event(sseSafe(html), Some(summary.eventUid.toString), Some(RowEvent))
      }

  /** Normalises line endings before framing.
    *
    * SSE is a line protocol: the field separator is `\n`, and a bare `\r` inside a `data:` line is not a line break the
    * browser will re-join — it is a byte that ends the field early and truncates the row. Twirl emits `\n` today, so
    * this is defence against a template file ever being saved with CRLF endings, which is a change nobody would connect
    * to a broken live tail.
    */
  def sseSafe(html: String): String = html.replace("\r\n", "\n").replace('\r', '\n').trim

  /** What a refused tail is told, in one place so the response and the tests cannot drift apart. */
  val BusyMessage: String =
    s"This replica is already serving ${TailService.MaxConcurrent} live tails. Close one and try again."
