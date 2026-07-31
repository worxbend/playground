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

package com.worxbend.persistence.repository

import com.augustnagro.magnum.DbCodec
import com.worxbend.kernel.event.Envelope
import com.worxbend.persistence.Codecs
import io.circe.Json
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.OffsetDateTime
import java.util.UUID

/** The primary key of an event.
  *
  * `occurred_at` is part of the identity, not a convenience: the table is partitioned on it, so a lookup by `event_uid`
  * alone would have to scan every partition. Handing callers a two-field reference makes that structural instead of
  * something a repository has to remember.
  */
final case class EventRef(occurredAt: OffsetDateTime, eventUid: UUID)

/** A row of the result list.
  *
  * **`raw` and `data` are deliberately absent.** ADR §6.3: including them would make the planner de-TOAST every payload
  * on the page, including the rows a user scrolls past without opening. The detail view fetches the payload by key,
  * which is one extra round trip for the one event that was actually asked for.
  *
  * Field order is the column order of the `SELECT` in `PostgresEventRepository`. Magnum's derived codec reads
  * positionally, so the two are a matched pair — reordering either alone compiles and then silently returns a device id
  * in the room field.
  */
final case class EventSummary(
  occurredAt: OffsetDateTime,
  eventUid: UUID,
  ingestedAt: OffsetDateTime,
  ceId: String,
  ceSource: String,
  ceType: String,
  ceSubject: Option[String],
  deviceId: Option[String],
  roomId: Option[String],
  personId: Option[String],
  severity: Option[String],
  severityRank: Option[Short],
  metricValue: Option[Double]
) derives DbCodec:

  def ref: EventRef = EventRef(occurredAt, eventUid)

/** A single event with its payload.
  *
  * Composed of a [[EventSummary]] plus `raw` rather than being a flat 14-field record, so the list projection and the
  * detail projection cannot drift: the detail query is literally the list query plus one column.
  */
final case class EventDetail(summary: EventSummary, raw: Json)

object EventDetail:

  private val summaryCodec: DbCodec[EventSummary] = summon[DbCodec[EventSummary]]

  /** Hand-composed rather than derived.
    *
    * Derivation would need `DbCodec[Json]` as a *given import* at this site, and `-Wunused:all` + `-Werror` is known to
    * flag given-imports that are only consumed inside Magnum's macro (ADR §12.4). Referencing `Codecs.jsonb` as an
    * ordinary value sidesteps the question entirely, and it makes the column layout — summary columns, then `raw` —
    * visible instead of implied.
    */
  given codec: DbCodec[EventDetail] with

    def queryRepr: String = s"${summaryCodec.queryRepr}, ?"

    def cols: IArray[Int] = summaryCodec.cols :+ Types.OTHER

    def readSingle(rs: ResultSet, pos: Int): EventDetail =
      EventDetail(
        summaryCodec.readSingle(rs, pos),
        Codecs.jsonb.readSingle(rs, pos + summaryCodec.cols.length)
      )

    def writeSingle(value: EventDetail, ps: PreparedStatement, pos: Int): Unit =
      summaryCodec.writeSingle(value.summary, ps, pos)
      Codecs.jsonb.writeSingle(value.raw, ps, pos + summaryCodec.cols.length)

/** One bucket of the time histogram. `count` is `bigint`, so `Long`: a month of a busy smart home overflows `Int`
  * sooner than anyone expects, and the failure is a negative bar on a chart.
  */
final case class HistogramBucket(bucket: OffsetDateTime, count: Long) derives DbCodec

/** One facet entry: a dimension value and how many candidate rows carry it. */
final case class FacetValue(value: String, count: Long) derives DbCodec

/** A row to be written.
  *
  * `occurredAt` is passed separately from `raw` even though it is *in* `raw`, because it is the partition key and ADR
  * §5 forbids generating a partition key column (`text::timestamptz` is only `STABLE`). The ingestion layer is what
  * validated and clamped it; the database takes it on trust and files the row accordingly.
  */
final case class NewEvent(occurredAt: OffsetDateTime, raw: Json, payloadSha256: IArray[Byte])

object NewEvent:

  /** Hashes the *canonical* JSON rendering.
    *
    * This is not a substitute for the wire bytes and does not pretend to be: `jsonb` already discards key order,
    * insignificant whitespace and duplicate keys (ADR §12.4), so hashing the raw octets would produce a digest that
    * could never be recomputed from what was stored. Hashing `noSpaces` gives a digest that *can* be recomputed from
    * the stored row, which is what makes it useful for detecting silent corruption and cross-partition duplicates.
    */
  def of(occurredAt: OffsetDateTime, raw: Json): NewEvent =
    val digest = MessageDigest.getInstance("SHA-256").digest(raw.noSpaces.getBytes(UTF_8))
    NewEvent(occurredAt, raw, IArray.unsafeFromArray(digest))

  /** The bridge from the domain.
    *
    * Fails when the envelope carries no `time`: the CloudEvents spec makes `time` optional, but the partition key is
    * `NOT NULL` and inventing a timestamp would file the event in a month it did not happen in. The caller — wolfram at
    * ingest, cobalt at the DLQ boundary — is the layer that knows what to do about it.
    */
  def from(envelope: Envelope): Either[String, NewEvent] =
    envelope.time.toRight("event has no time attribute and cannot be assigned to a partition").map { time =>
      of(time, envelope.toJson)
    }
