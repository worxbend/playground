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

package com.worxbend.eventing

import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.event.Binary
import com.worxbend.kernel.event.ContentType
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.kernel.event.Subject
import com.worxbend.kernel.event.Topics
import io.circe.Decoder
import io.circe.DecodingFailure
import io.circe.Encoder
import io.circe.HCursor
import io.circe.Json
import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Where a record came from, in the coordinates that identify it uniquely and forever.
  *
  * `(topic, partition, offset)` is the only identifier a record that failed to *decode* has — it has no CloudEvents
  * `id`, because parsing the `id` is what failed. Everything about replay and about not double-writing the DLQ hangs
  * off that triple.
  */
final case class RecordOrigin(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Option[Long],
  key: Option[String]
):

  /** The DLQ record key (ADR §4.3): keying on the origin coordinates means a replayed poison record *overwrites* its
    * predecessor under log compaction rather than adding a copy on every retry.
    */
  def dlqKey: String = Topics.dlqKey(topic, partition, offset)

object RecordOrigin:

  def of(record: ConsumerRecord[String, Array[Byte]]): RecordOrigin =
    RecordOrigin(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      timestamp = Option(record.timestamp).filter(_ >= 0L).map(_.longValue),
      key = Option(record.key)
    )

  given encoder: Encoder[RecordOrigin] = Encoder.instance: origin =>
    Json.fromFields(
      Vector(
        "topic" -> Json.fromString(origin.topic),
        "partition" -> Json.fromInt(origin.partition),
        "offset" -> Json.fromLong(origin.offset)
      ) ++ origin.timestamp.map(v => "timestamp" -> Json.fromLong(v)).toVector
        ++ origin.key.map(v => "key" -> Json.fromString(v)).toVector
    )

  given decoder: Decoder[RecordOrigin] = Decoder.instance: cursor =>
    for
      topic <- cursor.get[String]("topic")
      partition <- cursor.get[Int]("partition")
      offset <- cursor.get[Long]("offset")
      timestamp <- cursor.get[Option[Long]]("timestamp")
      key <- cursor.get[Option[String]]("key")
    yield RecordOrigin(topic, partition, offset, timestamp, key)

/** The wrapper that makes a poison pill diagnosable and replayable.
  *
  * A record that cannot be decoded is the one case where the offset must be committed *without* the event having been
  * processed (ADR §4.3). That is only defensible if nothing is lost, so this carries three things:
  *
  *   - **why** — [[DecodeFailure.reason]], a bounded tag safe for `consume.records.poison{reason}`, next to the
  *     unbounded `detail` a human actually reads;
  *   - **where** — the origin coordinates, which are the record's only identity when its `id` is the thing that failed
  *     to parse, and which double as the DLQ key so retries overwrite;
  *   - **what** — the original value bytes verbatim, plus the original headers. Replay is republishing these two, so
  *     anything less makes the DLQ a graveyard rather than a queue.
  *
  * **Headers are recorded as text, the value as bytes.** The binding defines every `ce_*` header as a UTF-8 string, so
  * text is lossless for anything this system or a conformant producer writes, and it keeps the DLQ record readable in
  * `kcat` — which is the entire reason the DLQ is in structured mode. The *value* is where arbitrary bytes actually
  * live, and it is kept exactly, base64-encoded.
  *
  * **The dead letter is itself a CloudEvent.** [[toEnvelope]] wraps it as a normal envelope with this build's own
  * `type`, so the DLQ topic holds the same kind of thing as every other topic: it can be consumed by the same decoder,
  * rendered by the same UI and stored by the same insert. A bespoke DLQ format would need a second reader that nobody
  * exercises until the day it matters.
  */
final case class DeadLetter(
  origin: RecordOrigin,
  reason: String,
  detail: String,
  failedAt: OffsetDateTime,
  headers: Map[String, String],
  payload: Option[Binary],
  source: Source
):

  /** The dead letter as a CloudEvent envelope, ready for structured-mode publication to the DLQ.
    *
    * The `id` is the origin key rather than a fresh UUID, and deliberately: `(source, id)` is this pipeline's
    * deduplication key everywhere else, so deriving the id from the coordinates makes a replayed poison record
    * idempotent at the database in exactly the way a replayed good record is. A random id would let one bad byte
    * accumulate a row per retry.
    */
  def toEnvelope: Envelope =
    Envelope(
      id = EventId(origin.dlqKey).getOrElse(DeadLetter.fallbackId),
      source = source,
      eventType = DeadLetter.EventTypeName,
      time = Some(failedAt),
      subject = Subject(origin.topic).toOption,
      dataContentType = Some(DeadLetter.PayloadContentType),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(DeadLetter.encoder(this))
    )

object DeadLetter:

  /** The `type` of a dead-letter event. Reverse-DNS and unversioned, per ADR §4.2: the payload's shape is versioned by
    * `dataschema` if it ever needs to be, not by forking this string.
    */
  val EventTypeName: EventType =
    EventType("com.worxbend.eventing.dead-letter").getOrElse(throw IllegalStateException("dead-letter type is invalid"))

  /** The default `source` — overridden by each consumer with its own identity, because "which consumer gave up on this
    * record" is the first question asked of a DLQ with more than one reader.
    */
  val DefaultSource: Source =
    Source("urn:worxbend:eventing:dead-letter").getOrElse(throw IllegalStateException("dead-letter source is invalid"))

  /** The dead-letter body is plain JSON, not `application/cloudevents+json`: the *envelope* is the CloudEvent, and its
    * `data` is this record. Spelling the inner type as a CloudEvent too would invite a reader to unwrap twice.
    */
  val PayloadContentType: ContentType =
    ContentType("application/json").getOrElse(throw IllegalStateException("dead-letter content type is invalid"))

  /** Used only if the origin coordinates somehow produce a blank id, which `Topics.dlqKey` cannot. Present so
    * [[DeadLetter.toEnvelope]] is total: a DLQ path that can itself throw is worthless.
    */
  private val fallbackId: EventId =
    EventId("dead-letter").getOrElse(throw IllegalStateException("dead-letter fallback id is invalid"))

  /** Builds the dead letter for a record the consumer could not decode. */
  def of(
    record: ConsumerRecord[String, Array[Byte]],
    failure: DecodeFailure,
    source: Source = DefaultSource,
    failedAt: OffsetDateTime = OffsetDateTime.now()
  ): DeadLetter =
    DeadLetter(
      origin = RecordOrigin.of(record),
      reason = failure.reason,
      detail = failure.detail,
      failedAt = failedAt,
      headers = CloudEventHeaders.toMap(record.headers),
      payload = Option(record.value).map(Binary.copyOf),
      source = source
    )

  /** Hand-written rather than derived, for the same reason kernel's envelope codec is: `payload` is bytes and has to
    * become base64, and the timestamp has to go through `Rfc3339` rather than `OffsetDateTime.toString` (which is ISO
    * 8601 and drops a zero seconds field — see `com.worxbend.kernel.Rfc3339`).
    */
  given encoder: Encoder[DeadLetter] = Encoder.instance: value =>
    Json.fromFields(
      Vector(
        "reason" -> Json.fromString(value.reason),
        "detail" -> Json.fromString(value.detail),
        "failedAt" -> Json.fromString(Rfc3339.render(value.failedAt)),
        "source" -> Json.fromString(value.source),
        "origin" -> RecordOrigin.encoder(value.origin),
        "headers" -> Json.fromFields(value.headers.toVector.sortBy((name, _) => name).map { (name, header) =>
          name -> Json.fromString(header)
        })
      ) ++ value.payload.map(bytes => "payload" -> Json.fromString(bytes.base64)).toVector
    )

  given decoder: Decoder[DeadLetter] = Decoder.instance: cursor =>
    for
      reason <- cursor.get[String]("reason")
      detail <- cursor.get[String]("detail")
      failedAt <- cursor.get[String]("failedAt").flatMap(refine(cursor, Rfc3339.parse))
      source <- cursor.get[String]("source").flatMap(refine(cursor, Source.apply))
      origin <- cursor.get[RecordOrigin]("origin")
      headers <- cursor.get[Map[String, String]]("headers")
      payload <- cursor.get[Option[String]]("payload").flatMap(traverse(refine(cursor, Binary.fromBase64)))
    yield DeadLetter(origin, reason, detail, failedAt, headers, payload, source)

  /** Reads a dead letter back out of the envelope [[DeadLetter.toEnvelope]] produced — the replay path. */
  def fromEnvelope(envelope: Envelope): Either[String, DeadLetter] =
    if envelope.eventType != EventTypeName then
      Left(s"not a dead letter: type is '${envelope.eventType}', expected '$EventTypeName'")
    else
      envelope.payload match
        case Payload.Structured(json) => decoder.decodeJson(json).left.map(_.message)
        case _                        => Left("dead-letter event carries no JSON payload")

  private def refine[A](cursor: HCursor, f: String => Either[String, A]): String => Decoder.Result[A] =
    raw => f(raw).left.map(message => DecodingFailure(message, cursor.history))

  private def traverse[A, B](f: A => Decoder.Result[B])(value: Option[A]): Decoder.Result[Option[B]] =
    value match
      case None    => Right(None)
      case Some(a) => f(a).map(Some.apply)
