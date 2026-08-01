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

package com.worxbend.ferrite.tail

import com.worxbend.ferrite.search.SearchExecutionContext
import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.EventSummary
import com.worxbend.persistence.repository.SearchRequest
import com.worxbend.persistence.search.Cursor
import com.worxbend.persistence.search.SortDirection
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** Where a tail has read up to: the primary key of the newest row it has already sent.
  *
  * The same `(occurred_at, event_uid)` pair the search cursor is built from, and deliberately so — the tail is a keyset
  * page in ascending order, which means it runs the *same* statement paging runs and cannot drift from it.
  */
final case class TailCursor(occurredAt: OffsetDateTime, eventUid: UUID)

object TailCursor:

  /** The query parameters carrying the client's position. `app.js` writes them; this reads them. They are not part of
    * the filter grammar, so they are stripped before the filter is parsed — `FilterQuery` would otherwise report them
    * as unknown parameters and the whole stream would 400.
    */
  val AfterParam: String = "after"
  val AfterUidParam: String = "afterUid"

  /** Both halves or nothing. A cursor with a timestamp and no uid is not "nearly a position": it would have to be
    * completed with an invented uid, and the two possible inventions differ by whether the row at that exact timestamp
    * is re-sent or skipped.
    */
  def parse(after: Option[String], afterUid: Option[String]): Option[TailCursor] =
    for
      rawAt <- after
      rawUid <- afterUid
      at <- Rfc3339.parse(rawAt).toOption
      uid <- Try(UUID.fromString(rawUid)).toOption
    yield TailCursor(at, uid)

/** One poll: the rows found and the position to resume from. */
final case class TailBatch(cursor: TailCursor, rows: Vector[EventSummary])

/** The live tail's data access.
  *
  * **Polling a keyset cursor, not `LISTEN`/`NOTIFY`.** The rejected alternative is the tempting one, so the reasoning
  * is recorded here rather than left to be rediscovered:
  *
  *   - `LISTEN` is a property of a *session*, and every session ferrite has comes from HikariCP. A pooled connection
  *     that has `LISTEN` registered on it carries that registration to whichever request borrows it next, so the tail
  *     would need a connection held outside the pool for the process's lifetime, with its own reconnect, its own health
  *     check and its own shutdown hook — a second connection lifecycle to operate, in the service whose whole database
  *     story today is "two pools, closed by one stop hook".
  *   - It would also couple the writer to the reader: something has to execute `NOTIFY` on every insert, and that
  *     something is cobalt's batch write path. The web tier's refresh mechanism would then be a requirement on the
  *     ingestion path, which is exactly the direction ADR §1 keeps dependencies from pointing.
  *   - Notifications are *not* durable. A listener that is disconnected for a second misses everything sent in that
  *     second with no way to detect it, so a correct implementation needs the polling query anyway as its catch-up path
  *     — at which point the notification is an optimisation on top of the thing being built here, not a replacement for
  *     it.
  *
  * What polling costs is one index seek per open tail per [[TailService.PollInterval]]. The predicate is
  * `(occurred_at, event_uid) > (?, ?) ORDER BY occurred_at ASC LIMIT 50`, which the composite primary-key index serves
  * as a single descent; at the [[TailService.MaxConcurrent]] cap that is a handful of sub-millisecond queries a second
  * against a pool of eight connections with a two-second `statement_timeout`.
  *
  * **The honest limitation: `occurred_at` is the producer's clock.** An event that is ingested now but stamped earlier
  * than a row the tail has already sent sorts *behind* the cursor and will never appear in the tail. The alternatives
  * were weighed and rejected: ordering by `ingested_at` has only a BRIN index (ADR §5, index 6), which cannot serve an
  * ordered seek and would make every tick a scan; holding the cursor back by a grace period would re-send every row
  * inside that grace on every tick — for a five-second grace at a two-second interval, two and a half copies of the
  * feed — to catch a case that search, ordered the same way, also does not catch. It is a documented property of the
  * tail, surfaced in the UI, and not a bug that would be fixed by trying harder here.
  */
@Singleton
final class TailService @Inject() (repository: EventRepository, clock: Clock)(using ec: SearchExecutionContext):

  import TailService.*

  private val open: AtomicInteger = AtomicInteger(0)

  /** Whether another tail may be admitted.
    *
    * **This is the load-shedding point and it is deliberately a hard number.** Every connected tab is a repeating query
    * against the read pool, and the read pool is also what serves search. Without a cap, twenty forgotten tabs are
    * twenty pollers, the pool's eight connections are permanently busy, and the first symptom is that *search* gets
    * slow for everyone — a failure whose cause is invisible from the page that is failing. Refusing the seventeenth
    * tail with a `503` keeps the damage inside the feature that caused it.
    *
    * **Admission is checked here and the count is kept by [[opened]]/[[closed]], which is a deliberate split rather
    * than one atomic `acquire`.** A combined claim has to happen while the request is being answered, and a claim taken
    * there is only ever returned if the response body is then materialised — so a client that vanishes in the instant
    * between the two leaks a slot permanently, and the only cure is a restart. Counting at materialisation makes every
    * increment paired with the stream that will decrement it. The cost is that two requests arriving together can both
    * be admitted, overshooting the cap by the number of simultaneous opens; that self-corrects the moment either
    * closes, and a transient overshoot is a far better failure than a slot nobody can reclaim.
    */
  def hasCapacity: Boolean = open.get() < MaxConcurrent

  /** Counts a tail whose stream has actually been materialised. */
  def opened(): Unit =
    val _ = open.incrementAndGet()

  /** Counts it back out. Called from the stream's termination, whatever ended it. */
  def closed(): Unit =
    val _ = open.decrementAndGet()

  /** Tails currently connected. Exposed for tests, not as a meter — see `TailController` for why the stream is timed by
    * `MetricsFilter` like any other request and gets no meter family of its own.
    */
  def openTails: Int = open.get()

  /** Where a tail with no client-supplied position should start: after the newest row matching the filter *now*.
    *
    * One extra query at connect, and worth it. The alternative — starting at the wall clock — silently skips every
    * event whose `occurred_at` is a few seconds behind `now` because of ingest lag, so a tail opened on a busy system
    * would show nothing for its first few seconds and the user would conclude the feature is broken.
    *
    * When nothing matches at all, the clock is the right answer: there is no row to be after.
    */
  def head(filter: Option[Filter]): Future[TailCursor] =
    SearchRequest.first(filter, SortDirection.Newest, 1) match
      case Left(_)        => Future.successful(TailCursor(OffsetDateTime.now(clock), NilUid))
      case Right(request) =>
        repository
          .search(request)
          .map(page => page.rows.headOption.fold(TailCursor(OffsetDateTime.now(clock), NilUid))(cursorOf))

  /** One tick: everything strictly newer than `cursor`, oldest first, capped at [[MaxRowsPerTick]].
    *
    * **Ascending, even though the tail renders newest-first.** The cursor has to advance past every row it emitted, so
    * a tick that hits the cap must return the *oldest* unseen rows and leave the rest for the next tick; a descending
    * query capped at fifty would return the newest fifty and skip everything between them and the cursor, which is
    * silent data loss during exactly the burst an operator opened the tail to watch. The client prepends in arrival
    * order, so the newest still ends up on top.
    *
    * A failed build of the request (only possible if the cap were ever set outside its bounds) returns the cursor
    * unchanged and no rows, so a tail degrades to a heartbeat rather than to a broken stream.
    */
  def poll(filter: Option[Filter], cursor: TailCursor): Future[TailBatch] =
    request(filter, cursor) match
      case Left(_)      => Future.successful(TailBatch(cursor, Vector.empty))
      case Right(built) =>
        repository.search(built).map { page =>
          TailBatch(page.rows.lastOption.fold(cursor)(cursorOf), page.rows)
        }

  /** The keyset request, minted from a cursor this service built rather than from one a client sent.
    *
    * `Cursor` carries a fingerprint of the filter and `SearchRequest.of` checks it, which is what stops a paging cursor
    * being replayed against a different search. Here both halves are ours, so the check passes by construction — the
    * point of going through the same constructor anyway is that the tail then runs byte-for-byte the statement paging
    * runs, including the row-value keyset predicate that a hand-written tail query is exactly the place to get wrong.
    */
  private def request(filter: Option[Filter], cursor: TailCursor): Either[String, SearchRequest] =
    for
      base <- SearchRequest.first(filter, SortDirection.Oldest, MaxRowsPerTick)
      encoded = Cursor(cursor.occurredAt, cursor.eventUid, base.fingerprint).encode
      seeking <- SearchRequest.of(filter, SortDirection.Oldest, MaxRowsPerTick, Some(encoded))
    yield seeking

  private def cursorOf(row: EventSummary): TailCursor = TailCursor(row.occurredAt, row.eventUid)

object TailService:

  /** How often an open tail asks the database.
    *
    * Two seconds is a compromise with a name on both sides: below it the page stops feeling like a feed and starts
    * feeling like a poll (and multiplies the pool's load by the number of tabs), above it an operator watching a
    * deployment sees events arrive in visible clumps.
    *
    * **It is not what stops two ticks of the same tail overlapping.** That used to be claimed here on the grounds that
    * the interval equals the read pool's `statement_timeout`, and the arithmetic does not survive contact with the
    * pool: a tick waits for a connection (up to `connection-timeout`, 3 s) and for a dispatcher thread *before* the
    * statement starts, so its wall time can exceed the interval comfortably. Non-overlap is a property of the stream
    * topology — `TailController` threads the cursor through `scanAsync`, which runs one future at a time, and
    * `Source.tick` drops a tick the downstream is not ready for. Anything that replaced `scanAsync` with `mapAsync`
    * would break it, and the timeout would not notice.
    */
  val PollInterval: FiniteDuration = 2.seconds

  /** Rows one tick may return.
    *
    * The cap is what makes a burst survivable: ten thousand events arriving in two seconds produce fifty rows now and
    * fifty more on each following tick, rather than one response that serialises ten thousand `<tr>` elements into the
    * web tier's heap and then into a browser that stops responding.
    */
  val MaxRowsPerTick: Int = 50

  /** Tails one replica will serve at once.
    *
    * The arithmetic, written out because it is the whole justification for the number: 16 tails at one query per
    * [[PollInterval]] is 8 queries a second, each a keyset seek returning at most [[MaxRowsPerTick]] rows and each
    * borrowing one of the read pool's eight connections and one of the eight `SearchExecutionContext` threads for its
    * duration. At a millisecond a query that is under 1 % of either resource — a rounding error next to one search
    * page, which issues four.
    *
    * The number that would break it is not the tail count but the per-tick *duration*. The seek is bounded by the
    * cursor, which sits at the newest row the tail has seen, so the scan is over a suffix of one partition however
    * broad the rest of the filter is; and the read pool's `statement_timeout` caps the worst case at two seconds. Eight
    * simultaneous worst cases would hold the whole dispatcher, which is why the cap is here and not at twice this.
    */
  val MaxConcurrent: Int = 16

  /** The uid half of a cursor that means "before every real row at this instant". Any real `event_uid` sorts after it,
    * so a tail seeded with the clock does not skip an event that happens to land on the same microsecond.
    */
  val NilUid: UUID = UUID(0L, 0L)
