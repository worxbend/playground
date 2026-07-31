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

import java.net.URI
import scala.util.Try

/** CloudEvents `id` — unique per producer within the scope of a [[Source]].
  *
  * Modelled as `opaque type … <: String` rather than a wrapper class: the upper bound gives one-way assignability, so
  * an `EventId` flows into a JDBC setter, a circe encoder or a log statement with zero allocation and zero unwrapping,
  * while a [[Source]] can still never be passed where an `EventId` is expected. That asymmetry is the whole point —
  * `(source, id)` is the deduplication key of the entire pipeline, and silently swapping the two would produce a system
  * that looks correct and deduplicates nothing.
  */
opaque type EventId <: String = String

object EventId:

  /** The spec requires a non-empty string and says nothing else, so neither does this. Over-validating identifiers
    * rejects perfectly good producers; the uniqueness contract is theirs to keep, not ours to police.
    */
  def apply(raw: String): Either[String, EventId] = Attr.nonBlank("id", raw)

/** CloudEvents `source` — an RFC 3986 URI-reference identifying the producing context.
  *
  * Kept as a string rather than a `java.net.URI` because the wire form must survive verbatim: `URI` normalises nothing
  * on construction but `toString` is only guaranteed to reproduce the input for URIs built from a string, and the
  * dimension tables key on the exact bytes the producer sent.
  */
opaque type Source <: String = String

object Source:

  /** Validated as a URI-*reference*, so relative forms such as `/sensors/kitchen-1` are accepted — the spec explicitly
    * allows them and MQTT-style gateways use them constantly.
    */
  def apply(raw: String): Either[String, Source] = Attr.uriReference("source", raw)

/** CloudEvents `type` — reverse-DNS, versioned by `dataschema` rather than by a suffix on this string.
  *
  * See ADR §4.2: putting the version in the type string forks the registry on every additive change and makes "give me
  * all telemetry" a prefix match instead of an equality match.
  */
opaque type EventType <: String = String

object EventType:

  def apply(raw: String): Either[String, EventType] = Attr.nonBlank("type", raw)

/** CloudEvents `subject` — the device or entity within the [[Source]].
  *
  * This is half of the Kafka partition key ([[Envelope.partitionKey]]), which is why it is a distinct type: appending
  * the wrong attribute to the key silently destroys per-device ordering, and that failure is invisible until someone
  * plots a device timeline months later.
  */
opaque type Subject <: String = String

object Subject:

  /** The spec is explicit that `subject`, when present, MUST NOT be empty — an empty subject would also collapse the
    * partition key onto the bare source.
    */
  def apply(raw: String): Either[String, Subject] = Attr.nonBlank("subject", raw)

/** CloudEvents `datacontenttype` — an RFC 2046 media type describing `data`. */
opaque type ContentType <: String = String

object ContentType:

  /** Shape-checked only (`type/subtype` plus optional parameters). Full RFC 2045 parameter parsing would reject real
    * producers over quoting details that never affect how this system routes or stores an event.
    */
  def apply(raw: String): Either[String, ContentType] = Attr.mediaType(raw)

  /** `data_base64` without a `datacontenttype` is legal on the wire but meaningless to a consumer; this is the value
    * [[Payload.Opaque]] carries in that case so the payload is never described by `null`.
    */
  val OctetStream: ContentType = "application/octet-stream"

  /** The structured-mode media type — the one wolfram's Tapir endpoint advertises and the DLQ writes. */
  val CloudEventsJson: ContentType = "application/cloudevents+json"

/** Validation shared by the context attributes.
  *
  * Package-private and deliberately anaemic: the kernel's job is to reject what the CloudEvents spec calls invalid, not
  * to impose a house dialect that would make this build unable to read events other conformant tools produce.
  */
private object Attr:

  def nonBlank(what: String, raw: String): Either[String, String] =
    if raw.isEmpty then Left(s"$what must not be empty")
    else if raw.isBlank then Left(s"$what must not be blank")
    else Right(raw)

  def uriReference(what: String, raw: String): Either[String, String] =
    nonBlank(what, raw).flatMap: s =>
      Try(URI(s)).toEither.left.map(e => s"$what is not a URI-reference: ${e.getMessage}").map(_ => s)

  private val MediaTypePattern = "^[A-Za-z0-9!#$%&'*+.^_`|~-]+/[A-Za-z0-9!#$%&'*+.^_`|~-]+(;.*)?$".r

  def mediaType(raw: String): Either[String, String] =
    nonBlank("datacontenttype", raw).flatMap: s =>
      if MediaTypePattern.matches(s) then Right(s)
      else Left(s"datacontenttype '$s' is not a media type")
