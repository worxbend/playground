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

package com.worxbend.kernel.event

import com.worxbend.kernel.Rfc3339
import io.circe.Decoder
import io.circe.DecodingFailure
import io.circe.Encoder
import io.circe.HCursor
import io.circe.Json
import io.circe.JsonObject
import io.circe.parser
import java.time.OffsetDateTime

/** A CloudEvents 1.0 event, as this system's domain type.
  *
  * The SDK's `io.cloudevents.CloudEvent` is an adapter and lives in `modules/eventing` only (ADR §4): it is a Java
  * interface with nullable getters, a throwing mutable builder and a byte-oriented data model, none of which
  * pattern-matches. This type is the one the three services agree on.
  *
  * Two deviations from ADR §4.1, both forced by the losslessness requirement:
  *
  *   - `time` is `Option[OffsetDateTime]`. The ADR declares it required; the spec makes it OPTIONAL, and inventing a
  *     time for an event that did not carry one would corrupt the partition it lands in (ADR §5). It remains
  *     `OffsetDateTime` and never `Instant`, so the producer's local offset survives.
  *   - `dataContentType` is an explicit attribute. `Payload.Opaque` also carries a media type, but that field describes
  *     the bytes for consumers holding only a `Payload` (notably `Observation.Unrecognised`); the envelope attribute is
  *     the single source of truth for the wire, and [[canonical]] reconciles the two.
  *
  * Unknown context attributes land in `extensions` and unknown payload shapes in `Payload.Structured`, so an event this
  * build has never seen still parses, still persists, and still comes back out unchanged.
  */
final case class Envelope(
  id: EventId,
  source: Source,
  eventType: EventType,
  time: Option[OffsetDateTime],
  subject: Option[Subject],
  dataContentType: Option[ContentType],
  schema: Option[SchemaRef],
  extensions: Map[String, AttrValue],
  payload: Payload
):

  /** The single definition of the Kafka partition key (ADR §4.1).
    *
    * `source#subject` rather than `subject` alone: subjects are producer-local (`kitchen-1` means different things
    * behind two gateways) so keying on the subject would interleave two devices' timelines onto one partition and
    * destroy the per-key ordering the whole design rests on. Events without a subject key on the source, which keeps a
    * gateway's aggregate stream ordered.
    *
    * Changing this function is as breaking as changing the partition count — it rehashes every key.
    */
  def partitionKey: String = subject.fold[String](source)(s => s"$source#$s")

  /** The form this envelope takes after a JSON round-trip.
    *
    * Three normalisations, each of which is an erasure the JSON format performs and this method makes explicit:
    * reserved attribute names are not extensions and are dropped rather than allowed to collide on encode; extension
    * values lose their CE type (see `AttrValue.canonical`); and a binary payload's media type is whatever
    * `datacontenttype` said, defaulting to `application/octet-stream` when the attribute is absent.
    *
    * Idempotent, and the encoder applies it, so `decode(encode(e)) == e.canonical` holds for every envelope.
    */
  def canonical: Envelope =
    val cleanExtensions =
      extensions.iterator
        .filterNot((name, _) => Envelope.ReservedAttributes(name))
        .map((name, value) => name -> value.canonical)
        .toMap
    val cleanPayload = payload match
      case Payload.Opaque(bytes, _) => Payload.Opaque(bytes, dataContentType.getOrElse(ContentType.OctetStream))
      case other                    => other
    copy(extensions = cleanExtensions, payload = cleanPayload)

  /** Structured-mode JSON. */
  def toJson: Json = Envelope.encoder(this)

  /** Compact structured-mode JSON — the DLQ body and the `raw jsonb` column. */
  def render: String = toJson.noSpaces

object Envelope:

  /** The only spec version this build accepts. ADR §5 encodes the same rule as a CHECK constraint. */
  val SpecVersion: String = "1.0"

  /** Context attributes that are *not* extensions. Mirrors the `raw - '{…}'::text[]` list in the DDL of ADR §5; the two
    * must stay identical or the `extensions` column and this model disagree about what an extension is.
    */
  val ReservedAttributes: Set[String] =
    Set(
      "specversion",
      "id",
      "source",
      "type",
      "subject",
      "time",
      "dataschema",
      "datacontenttype",
      "data",
      "data_base64"
    )

  /** Hand-written, not derived (ADR §4.2).
    *
    * Derivation cannot express either of the two things this format actually does: extensions are *flattened* into the
    * top-level object rather than nested under a field, and the data slot is one of two differently named keys chosen
    * by the payload's shape. Attribute order is fixed so that a permalink-style content hash over the rendered event is
    * stable.
    */
  given encoder: Encoder[Envelope] = Encoder.instance[Envelope]: raw =>
    val e = raw.canonical
    val required = Vector(
      "specversion" -> Json.fromString(SpecVersion),
      "id" -> Json.fromString(e.id),
      "source" -> Json.fromString(e.source),
      "type" -> Json.fromString(e.eventType)
    )
    val optional = Vector(
      e.subject.map(v => "subject" -> Json.fromString(v)),
      e.time.map(v => "time" -> Json.fromString(Rfc3339.render(v))),
      e.dataContentType.map(v => "datacontenttype" -> Json.fromString(v)),
      e.schema.map(v => "dataschema" -> Json.fromString(v.uri.toString))
    ).flatten
    val flattened = e.extensions.toVector.sortBy((name, _) => name).map((name, value) => name -> value.toJson)
    val data = e.payload match
      case Payload.Structured(json) => Vector("data" -> json)
      case Payload.Opaque(bytes, _) => Vector("data_base64" -> Json.fromString(bytes.base64))
      case Payload.Empty            => Vector.empty
    Json.fromFields(required ++ optional ++ flattened ++ data)

  /** Total over anything spec-valid, and it never inspects `type` — refinement into the known-type ADT is a separate,
    * separately total step ([[Observation.from]]). That split is what lets an unheard-of firmware event reach Postgres.
    *
    * Leniency is confined to one place: an optional attribute explicitly set to `null` is treated as absent. The spec
    * forbids null attributes, so nothing is lost, and rejecting the event instead would fail a whole batch over a
    * producer's serialiser default.
    */
  given decoder: Decoder[Envelope] = Decoder.instance: cursor =>
    for
      obj <- cursor.value.asObject.toRight(fail(cursor, "a CloudEvent must be a JSON object"))
      _ <- checkSpecVersion(cursor, obj)
      id <- required(cursor, obj, "id").flatMap(refine(cursor, EventId.apply))
      source <- required(cursor, obj, "source").flatMap(refine(cursor, Source.apply))
      eventType <- required(cursor, obj, "type").flatMap(refine(cursor, EventType.apply))
      subject <- optional(cursor, obj, "subject").flatMap(traverse(refine(cursor, Subject.apply)))
      time <- optional(cursor, obj, "time").flatMap(traverse(refine(cursor, Rfc3339.parse)))
      dataContentType <- optional(cursor, obj, "datacontenttype").flatMap(traverse(refine(cursor, ContentType.apply)))
      schema <- optional(cursor, obj, "dataschema").flatMap(traverse(refine(cursor, SchemaRef.parse)))
      payload <- decodePayload(cursor, obj, dataContentType)
    yield Envelope(
      id = id,
      source = source,
      eventType = eventType,
      time = time,
      subject = subject,
      dataContentType = dataContentType,
      schema = schema,
      extensions = extensionsOf(obj),
      payload = payload
    )

  /** Parse structured-mode JSON text. The two failure modes — not JSON at all, and JSON that is not a CloudEvent — are
    * collapsed into one message because every caller (wolfram's 400, cobalt's DLQ) treats them identically.
    */
  def parse(text: String): Either[String, Envelope] =
    parser.parse(text).left.map(_.message).flatMap(json => decoder.decodeJson(json).left.map(_.message))

  private def fail(cursor: HCursor, message: String): DecodingFailure =
    DecodingFailure(message, cursor.history)

  private def refine[A](cursor: HCursor, f: String => Either[String, A]): String => Decoder.Result[A] =
    raw => f(raw).left.map(message => fail(cursor, message))

  private def traverse[A, B](f: A => Decoder.Result[B])(value: Option[A]): Decoder.Result[Option[B]] =
    value match
      case None    => Right(None)
      case Some(a) => f(a).map(Some.apply)

  private def checkSpecVersion(cursor: HCursor, obj: JsonObject): Decoder.Result[Unit] =
    required(cursor, obj, "specversion").flatMap: found =>
      if found == SpecVersion then Right(())
      else Left(fail(cursor, s"unsupported specversion '$found'; this build speaks $SpecVersion only"))

  private def required(cursor: HCursor, obj: JsonObject, key: String): Decoder.Result[String] =
    obj(key) match
      case None       => Left(fail(cursor, s"required attribute '$key' is missing"))
      case Some(json) => json.asString.toRight(fail(cursor, s"attribute '$key' must be a JSON string"))

  private def optional(cursor: HCursor, obj: JsonObject, key: String): Decoder.Result[Option[String]] =
    obj(key) match
      case None                      => Right(None)
      case Some(json) if json.isNull => Right(None)
      case Some(json)                =>
        json.asString.map(Some.apply).toRight(fail(cursor, s"attribute '$key' must be a JSON string"))

  private def decodePayload(
    cursor: HCursor,
    obj: JsonObject,
    contentType: Option[ContentType]
  ): Decoder.Result[Payload] =
    (obj("data"), obj("data_base64")) match
      case (Some(_), Some(_)) =>
        Left(fail(cursor, "data and data_base64 must not both be present"))
      case (Some(json), None) =>
        Right(Payload.Structured(json))
      case (None, Some(json)) =>
        json.asString
          .toRight(fail(cursor, "data_base64 must be a JSON string"))
          .flatMap(text => Binary.fromBase64(text).left.map(message => fail(cursor, message)))
          .map(bytes => Payload.Opaque(bytes, contentType.getOrElse(ContentType.OctetStream)))
      case (None, None) =>
        Right(Payload.Empty)

  private def extensionsOf(obj: JsonObject): Map[String, AttrValue] =
    obj.toIterable.iterator
      .filterNot((name, _) => ReservedAttributes(name))
      .map((name, json) => name -> AttrValue.fromJson(json))
      .toMap
