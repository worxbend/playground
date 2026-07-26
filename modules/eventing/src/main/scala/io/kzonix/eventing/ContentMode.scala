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

package io.kzonix.eventing

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.util.control.NonFatal

import io.cloudevents.SpecVersion
import io.cloudevents.kafka.impl.KafkaBinaryMessageReaderImpl
import io.kzonix.kernel.Rfc3339
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import org.apache.kafka.common.header.Headers

/** The two ways the CloudEvents Kafka binding can lay an event onto a record, and the encode/decode pair for each.
  *
  * **Binary is the mode of `events.cloudevents.v1`; structured is the mode of the DLQ.** ADR §4.3 picks both
  * deliberately and for opposite reasons:
  *
  *   - *Binary on the main topic* puts the context attributes in headers and the payload in the value untouched.
  *     Brokers, single-message transforms and `kcat` route on `ce_type` / `ce_source` without deserializing anything;
  *     the payload is never re-encoded, so an event whose schema this build has never seen round-trips
  *     byte-identically; and binary data is not double-base64'd. The cost — extension attributes lose their type,
  *     because a header is bytes — is named and made explicit by [[CloudEventAdapter.binaryCanonical]] rather than
  *     discovered later.
  *   - *Structured on the DLQ* puts the entire event, attributes and all, into the value as one self-contained
  *     `application/cloudevents+json` blob. A poison record is read by a human under time pressure with `kcat`, and
  *     reassembling an event from a dozen headers at that moment is exactly the wrong task. Self-containment also
  *     makes replay a copy rather than a reconstruction.
  *
  * A single [[read]] handles both, because the consumer that drains the DLQ and the consumer that drains the main
  * topic are the same code, and because a producer misconfigured into the other mode should be read rather than
  * silently dead-lettered.
  */
enum ContentMode:
  case Binary
  case Structured

object ContentMode:

  /** The media type that marks a record as structured mode. Matched on the essence only, so a producer that appends
    * `; charset=utf-8` is still understood.
    */
  val StructuredMediaType: ContentType = ContentType.CloudEventsJson

  /** Which mode, if any, the headers say this record is in.
    *
    * **`ce_specversion` is checked first, and that order is load-bearing.** In binary mode the `content-type` header
    * describes the *payload*, and a payload may perfectly well be `application/cloudevents+json` — a forwarder, a
    * replay tool, or an event that quotes another event. Deciding on `content-type` first, as the SDK's own
    * `MessageUtils.parseStructuredOrBinaryMessage` does, misreads exactly those records as structured and loses every
    * attribute in the headers. A genuine structured record, by contrast, never carries `ce_specversion`: its
    * specversion is inside the JSON. So the presence of that header is the unambiguous signal and the media type is
    * only the fallback.
    *
    * Mode detection also has to happen here rather than being delegated to the SDK, because this build deliberately
    * keeps `cloudevents-json-jackson` off the classpath (ADR §3.3 — a second JSON stack next to circe). With no
    * `EventFormat` registered, handing a DLQ record to `KafkaMessageFactory` raises "unknown encoding" instead of
    * parsing it. Deciding here and using kernel's own JSON codec for structured records is what keeps the DLQ readable
    * without a second JSON library.
    */
  def of(headers: Headers): Option[ContentMode] =
    if CloudEventHeaders.get(headers, CloudEventHeaders.SpecVersion).isDefined then Some(Binary)
    else
      val structured = CloudEventHeaders
        .get(headers, CloudEventHeaders.ContentType)
        .exists(value => value.trim.toLowerCase(Locale.ROOT).startsWith(StructuredMediaType))
      if structured then Some(Structured) else None

  /** Writes `envelope` into `headers` in `mode` and returns the record value.
    *
    * `headers` is mutated, because that is the shape Kafka's own `Serializer.serialize(topic, headers, data)` imposes;
    * matching it makes [[KafkaCodecs.envelopeSerializer]] a thin wrapper rather than a second implementation.
    *
    * `None` means the record value must be `null` — the event carries no `data`. That is the binding's encoding of
    * "no data"; an empty value instead would be indistinguishable from a genuinely zero-byte payload, and this build
    * keeps that distinction because kernel's `Payload` does.
    */
  def write(mode: ContentMode, envelope: Envelope, headers: Headers): Either[String, Option[Array[Byte]]] =
    mode match
      case Binary     => writeBinary(envelope, headers)
      case Structured => writeStructured(envelope, headers)

  /** Reads a record back, choosing the mode from the headers. Total: every failure is a [[DecodeFailure]] value. */
  def read(headers: Headers, value: Option[Array[Byte]]): Either[DecodeFailure, Envelope] =
    of(headers) match
      case None             =>
        Left(
          DecodeFailure.UnknownEncoding(
            s"record carries neither a '${CloudEventHeaders.ContentType}: $StructuredMediaType' header " +
              s"nor a '${CloudEventHeaders.SpecVersion}' header"
          )
        )
      case Some(Structured) => readStructured(value)
      case Some(Binary)     => readBinary(headers, value)

  /** Structured decode goes through kernel's hand-written JSON Format 1.0 codec — the same code path the `raw jsonb`
    * column and wolfram's structured endpoint use, so a DLQ record and a stored event cannot disagree about the JSON.
    */
  private def readStructured(value: Option[Array[Byte]]): Either[DecodeFailure, Envelope] = value match
    case None        => Left(DecodeFailure.MalformedStructured("structured-mode record has no value"))
    case Some(bytes) => Envelope.parse(String(bytes, UTF_8)).left.map(DecodeFailure.MalformedStructured.apply)

  /** Binary decode goes through the SDK's header reader rather than a hand-written header parser.
    *
    * The asymmetry with [[writeBinary]] is intentional. Writing is where this build is the author and must get
    * RFC 3339 right; reading is where it must accept whatever a conformant third-party producer emits, and the SDK is
    * the conformance oracle for that. It also makes the round-trip property prove interoperability rather than merely
    * proving two functions in this file are inverses.
    *
    * **The reader is constructed directly instead of through `KafkaMessageFactory.createReader`,** because the factory
    * re-runs the mode detection [[of]] has already done — and gets it wrong in one specific way. Its `MessageUtils`
    * sees a `content-type` starting with `application/cloudevents`, finds no registered `EventFormat` (this build has
    * none by design, ADR §3.3) and throws "unknown encoding" *before* ever looking at `ce_specversion`. So a perfectly
    * well-formed binary record whose *payload* happens to be a CloudEvents document — a forwarder, a replay tool —
    * would be dead-lettered. Having decided the mode on the unambiguous signal, this hands the SDK the one job it is
    * actually the authority on: turning `ce_*` headers into attributes.
    *
    * **The payload is taken from the record value, not from the SDK's `CloudEventData`,** for a second reason of the
    * same kind: the SDK's Kafka reader treats a zero-length value as *no data at all*, collapsing an event whose data
    * is genuinely zero bytes into an event with none. Kernel's `Payload` distinguishes the two and structured mode
    * preserves the distinction, so letting binary mode lose it would mean an event changed shape merely by being
    * copied from the main topic to the DLQ.
    */
  private def readBinary(headers: Headers, value: Option[Array[Byte]]): Either[DecodeFailure, Envelope] =
    val event =
      try
        val specVersion = SpecVersion.parse(CloudEventHeaders.get(headers, CloudEventHeaders.SpecVersion).orNull)
        Right(KafkaBinaryMessageReaderImpl(specVersion, headers, value.orNull).toEvent())
      catch
        case NonFatal(error) =>
          Left(DecodeFailure.MalformedBinary(Option(error.getMessage).getOrElse(error.getClass.getName)))
    event
      .flatMap(CloudEventAdapter.toEnvelope(_).left.map(DecodeFailure.Unconvertible.apply))
      .map(envelope => envelope.copy(payload = CloudEventAdapter.payloadOf(value, envelope.dataContentType)))

  /** Binary encode is written out here rather than delegated to `io.cloudevents.kafka.CloudEventSerializer`.
    *
    * The SDK serializer renders `time` with `DateTimeFormatter.ISO_OFFSET_DATE_TIME`, which omits the seconds field
    * when it is zero — `2024-01-01T17:31Z` — and that is not RFC 3339. It parses back fine, so the defect is invisible
    * in any Java-only round trip and surfaces only against a stricter consumer in someone else's stack.
    * `io.kzonix.kernel.Rfc3339` exists precisely so this build does not emit that, and reintroducing it two modules
    * later would throw the effort away. Everything else about the layout is the binding's, and [[readBinary]] proves
    * it by reading these records back with the SDK.
    */
  private def writeBinary(envelope: Envelope, headers: Headers): Either[String, Option[Array[Byte]]] =
    val event = CloudEventAdapter.binaryCanonical(envelope)
    val invalid = CloudEventAdapter.invalidExtensionNames(event)
    if invalid.nonEmpty then Left(CloudEventAdapter.invalidExtensionMessage(invalid))
    else
      val required = Vector(
        CloudEventHeaders.SpecVersion         -> Envelope.SpecVersion,
        CloudEventHeaders.attribute("id")     -> (event.id: String),
        CloudEventHeaders.attribute("source") -> (event.source: String),
        CloudEventHeaders.attribute("type")   -> (event.eventType: String)
      )
      val optional = Vector(
        event.subject.map(v => CloudEventHeaders.attribute("subject") -> (v: String)),
        event.time.map(v => CloudEventHeaders.attribute("time") -> Rfc3339.render(v)),
        event.schema.map(v => CloudEventHeaders.attribute("dataschema") -> v.uri.toString),
        event.dataContentType.map(v => CloudEventHeaders.ContentType -> (v: String))
      ).flatten
      val extensions = event.extensions.toVector
        .sortBy((name, _) => name)
        .map((name, value) => CloudEventHeaders.attribute(name) -> CloudEventAdapter.headerValue(value))
      (required ++ optional ++ extensions).foreach: (name, value) =>
        val _ = CloudEventHeaders.put(headers, name, value)
      Right(CloudEventAdapter.dataOf(event.payload))

  /** Structured encode is kernel's `Envelope.render`, unchanged: one JSON object holding attributes, extensions and
    * data, which is what makes a DLQ record readable with nothing but `kcat`.
    */
  private def writeStructured(envelope: Envelope, headers: Headers): Either[String, Option[Array[Byte]]] =
    val _ = CloudEventHeaders.put(headers, CloudEventHeaders.ContentType, StructuredMediaType)
    Right(Some(envelope.render.getBytes(UTF_8)))
