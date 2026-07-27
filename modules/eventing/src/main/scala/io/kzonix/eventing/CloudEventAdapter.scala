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

import io.circe.parser
import io.cloudevents.CloudEvent
import io.cloudevents.SpecVersion
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.data.BytesCloudEventData
import io.kzonix.kernel.Rfc3339
import io.kzonix.kernel.event.AttrValue
import io.kzonix.kernel.event.Binary
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.EventId
import io.kzonix.kernel.event.EventType
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.SchemaRef
import io.kzonix.kernel.event.Source
import io.kzonix.kernel.event.Subject
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** The one place where the domain [[io.kzonix.kernel.event.Envelope]] and the SDK's `io.cloudevents.CloudEvent` meet.
  *
  * ADR §4 is emphatic that the SDK type is infrastructure, never a domain type: it is a Java interface with nullable
  * getters, a throwing mutable builder and a byte-oriented data model. Keeping the conversion in a single object means
  * wolfram (producer) and cobalt (consumer) cannot disagree about what a given envelope looks like on the wire — the
  * whole reason `modules/eventing` exists at all.
  *
  * **Losslessness is stated as an equation, not asserted.** The SDK cannot represent everything an `Envelope` can, so
  * "lossless" is defined as: `toEnvelope(toCloudEvent(e)) == Right(canonical(e))`, with [[canonical]] an *idempotent*
  * function that names every normalisation exactly once. Anything [[canonical]] does not change survives untouched;
  * anything it does change was never representable, and the change is visible in one readable function rather than
  * hidden across a codec. The property is tested for every generated envelope, and [[canonical]] is tested idempotent.
  *
  * The three normalisations, and why each is forced:
  *
  *   - **Reserved names are not extensions.** `id`, `time`, `data` &c. are context attributes; an extension so named
  *     would collide with the attribute on the wire. They are dropped, matching `Envelope.canonical`.
  *   - **`AttrValue.Other` becomes `Text`.** The SDK's extension model has exactly six types and a JSON array or object
  *     is not among them. Encoding the JSON text and decoding it back as a string keeps the data; guessing on the way
  *     back — "does this string parse as JSON?" — would corrupt every string extension that happens to look like JSON,
  *     which is the far more common case. The other five `AttrValue` shapes round-trip *typed*, which is the point of
  *     the ADT and is why this is not simply `Map[String, String]`.
  *   - **The payload's shape follows the media type.** `CloudEventData` is bytes; the `data` versus `data_base64`
  *     distinction that kernel's JSON codec relies on does not exist here. So [[canonical]] resolves the shape by
  *     literally encoding and re-decoding the payload, which makes agreement between the two directions structural
  *     rather than a pair of functions someone has to keep in step.
  *
  * **Extension *names* cannot be normalised, so they are rejected.** CloudEvents requires lowercase alphanumerics, and
  * the SDK builder throws on anything else. Renaming would lose data and dropping would lose more, so [[toCloudEvent]]
  * returns a `Left` naming the offending keys — which is exactly the ingest-time validation ADR §4.3 asks for ("Reject;
  * never invent defaults") rather than a surprise `RuntimeException` inside a producer callback.
  */
object CloudEventAdapter:

  /** The media types whose payload is JSON text rather than opaque bytes.
    *
    * An **absent** `datacontenttype` counts as JSON-ish. That is the reading that keeps kernel's two payload shapes
    * distinguishable: its JSON codec puts a bare `data` value (structured) next to `data_base64` (opaque), and the only
    * signal that survives into a byte-oriented binding is the media type.
    */
  def isJsonMediaType(contentType: ContentType): Boolean =
    val essence = contentType.takeWhile(_ != ';').trim.toLowerCase(Locale.ROOT)
    essence.endsWith("/json") || essence.endsWith("+json")

  /** True when the SDK — and the spec — will accept `name` as an extension attribute name.
    *
    * CloudEvents 1.0 restricts extension names to lowercase letters and digits. The SDK enforces exactly this and
    * throws otherwise, so the check is duplicated here in order to fail as a *value* at the boundary instead of as an
    * exception three frames into a builder.
    */
  def isValidExtensionName(name: String): Boolean =
    name.nonEmpty && name.forall(c => (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))

  /** The extension names of `envelope` that no conformant CloudEvents representation can carry. Empty means
    * [[toCloudEvent]] will succeed.
    */
  def invalidExtensionNames(envelope: Envelope): Set[String] =
    envelope.extensions.keySet.filterNot(isValidExtensionName)

  /** The rejection message for [[invalidExtensionNames]]. Shared with the binary writer so wolfram's 400 body and
    * cobalt's dead-letter `detail` say the same thing about the same event.
    */
  def invalidExtensionMessage(names: Set[String]): String =
    s"extension name(s) ${names.toVector.sorted.mkString(", ")} are not CloudEvents-conformant: " +
      "extension names may contain lowercase letters and digits only"

  /** The form `envelope` takes after a round trip through the SDK. Idempotent; see the class Scaladoc for the three
    * normalisations and why each is unavoidable.
    */
  def canonical(envelope: Envelope): Envelope =
    val extensions = envelope.extensions.iterator
      .filterNot((name, _) => Envelope.ReservedAttributes(name))
      .map((name, value) => name -> representable(value))
      .toMap
    envelope.copy(extensions = extensions, payload = payloadOf(dataOf(envelope.payload), envelope.dataContentType))

  /** [[canonical]], plus the further collapse that *binary content mode* forces: every extension becomes `Text`.
    *
    * Kafka headers are bytes, so the binding writes extension values as strings and a reader has no way to recover that
    * `sequence` was an Integer and `sampled` a Boolean — the type is simply not on the wire. Making that erasure an
    * explicit function, rather than a surprise in a round-trip assertion, is what lets the binary-mode property be an
    * equation. Structured mode does not need this: kernel's JSON codec keeps Integer and Boolean typed.
    */
  def binaryCanonical(envelope: Envelope): Envelope =
    val normalised = canonical(envelope)
    normalised.copy(extensions = normalised.extensions.view.mapValues(v => AttrValue.Text(headerValue(v))).toMap)

  /** The string form of an extension value in a Kafka header.
    *
    * Written here rather than delegated to the SDK's writer on purpose: the SDK renders a timestamp with
    * `DateTimeFormatter.ISO_OFFSET_DATE_TIME`, which drops the seconds field when it is zero and is therefore not RFC
    * 3339 — the precise deviation `io.kzonix.kernel.Rfc3339` exists to prevent. Any value whose `toString` the SDK
    * would have used unchecked (notably `byte[]`, which renders as `[B@1f2a3b`) is spelled out instead.
    */
  def headerValue(value: AttrValue): String = value match
    case AttrValue.Text(v)  => v
    case AttrValue.Num(v)   => v.toString
    case AttrValue.Flag(v)  => v.toString
    case AttrValue.Time(v)  => Rfc3339.render(v)
    case AttrValue.Ref(v)   => v.toString
    case AttrValue.Bytes(v) => v.base64
    case AttrValue.Other(v) => v.noSpaces

  /** Domain to SDK.
    *
    * `Left` only when an extension name is not spec-valid, or when the SDK builder objects to something this build did
    * not anticipate — the latter is caught rather than propagated so that a producer never has to wrap this call in a
    * `try`.
    */
  def toCloudEvent(envelope: Envelope): Either[String, CloudEvent] =
    val event = canonical(envelope)
    val invalid = invalidExtensionNames(event)
    if invalid.nonEmpty then Left(invalidExtensionMessage(invalid))
    else
      try Right(build(event))
      catch case NonFatal(error) => Left(s"cannot represent envelope as a CloudEvent: ${messageOf(error)}")

  /** SDK to domain.
    *
    * Every failure mode of the SDK's nullable getters is turned into a `Left` here, which is what lets the consumer
    * treat a structurally broken event as data destined for the DLQ instead of as an exception thrown into a stream
    * operator (ADR §4.3, step 3).
    */
  def toEnvelope(event: CloudEvent): Either[String, Envelope] =
    for
      _ <- Either.cond(
        event.getSpecVersion == SpecVersion.V1,
        (),
        s"unsupported specversion '${event.getSpecVersion}'; this build speaks ${Envelope.SpecVersion} only"
      )
      id <- required("id", Option(event.getId)).flatMap(EventId.apply)
      source <- required("source", Option(event.getSource)).flatMap(uri => Source(uri.toString))
      eventType <- required("type", Option(event.getType)).flatMap(EventType.apply)
      subject <- Option(event.getSubject).fold(rightNone[Subject])(Subject.apply(_).map(Some.apply))
      mediaType <- Option(event.getDataContentType).fold(rightNone[ContentType])(ContentType.apply(_).map(Some.apply))
    yield Envelope(
      id = id,
      source = source,
      eventType = eventType,
      time = Option(event.getTime),
      subject = subject,
      dataContentType = mediaType,
      schema = Option(event.getDataSchema).map(SchemaRef.apply),
      extensions = extensionsOf(event),
      payload = payloadOf(Option(event.getData).map(_.toBytes), mediaType)
    )

  /** The bytes a payload occupies on the wire, or `None` when the event carries no `data` at all.
    *
    * `None` and `Some(empty array)` are different things — the first is an event without data, the second an event
    * whose data happens to be zero bytes — and the distinction is preserved through the Kafka value (null versus an
    * empty value). Collapsing them would make an attribute-only event indistinguishable from an empty binary one.
    */
  def dataOf(payload: Payload): Option[Array[Byte]] = payload match
    case Payload.Structured(json) => Some(json.noSpaces.getBytes(UTF_8))
    case Payload.Opaque(bytes, _) => Some(bytes.toArray)
    case Payload.Empty            => None

  /** The inverse of [[dataOf]] for a given media type. Used by both directions of the adapter *and* by [[canonical]],
    * which is what makes encode and decode agree by construction rather than by review.
    */
  def payloadOf(data: Option[Array[Byte]], contentType: Option[ContentType]): Payload = data match
    case None        => Payload.Empty
    case Some(bytes) =>
      val opaque = Payload.Opaque(Binary.copyOf(bytes), contentType.getOrElse(ContentType.OctetStream))
      if contentType.forall(isJsonMediaType) then
        parser.parse(String(bytes, UTF_8)).fold(_ => opaque, json => Payload.Structured(json))
      else opaque

  private def representable(value: AttrValue): AttrValue = value match
    case AttrValue.Other(json) => AttrValue.Text(json.noSpaces)
    case typed                 => typed

  private def rightNone[A]: Either[String, Option[A]] = Right(None)

  private def required[A](name: String, value: Option[A]): Either[String, A] =
    value.toRight(s"required attribute '$name' is missing")

  private def messageOf(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getName)

  private def build(event: Envelope): CloudEvent =
    val base: CloudEventBuilder = CloudEventBuilder
      .v1()
      .withId(event.id)
      .withSource(URI(event.source))
      .withType(event.eventType)
    val withSubject = event.subject.fold(base)(base.withSubject)
    val withTime = event.time.fold(withSubject)(withSubject.withTime)
    val withContentType = event.dataContentType.fold(withTime)(withTime.withDataContentType)
    val withSchema = event.schema.fold(withContentType)(ref => withContentType.withDataSchema(ref.uri))
    val withExtensions =
      event.extensions.foldLeft(withSchema)((builder, entry) => extended(builder, entry._1, entry._2))
    val withData = dataOf(event.payload).fold(withExtensions)(bytes =>
      withExtensions.withData(BytesCloudEventData.wrap(bytes))
    )
    withData.build()

  private def extended(builder: CloudEventBuilder, name: String, value: AttrValue): CloudEventBuilder = value match
    case AttrValue.Text(v)  => builder.withExtension(name, v)
    case AttrValue.Num(v)   => builder.withExtension(name, java.lang.Integer.valueOf(v))
    case AttrValue.Flag(v)  => builder.withExtension(name, java.lang.Boolean.valueOf(v))
    case AttrValue.Time(v)  => builder.withExtension(name, v)
    case AttrValue.Ref(v)   => builder.withExtension(name, v)
    case AttrValue.Bytes(v) => builder.withExtension(name, v.toArray)
    case AttrValue.Other(v) => builder.withExtension(name, v.noSpaces)

  /** The SDK hands extension values back as `Object`. The six shapes it can hold map onto `AttrValue` exactly; a
    * `Number` that is not an `Int` (the only width kernel's `Num` has) degrades to its text rather than being
    * truncated, because a silently narrowed sequence number is worse than an honest string.
    */
  private def extensionsOf(event: CloudEvent): Map[String, AttrValue] =
    event.getExtensionNames.asScala.iterator
      .flatMap(name => Option(event.getExtension(name)).map(value => name -> attrValueOf(value)))
      .toMap

  private def attrValueOf(value: AnyRef): AttrValue = value match
    case v: String                   => AttrValue.Text(v)
    case v: java.lang.Boolean        => AttrValue.Flag(v.booleanValue)
    case v: java.lang.Integer        => AttrValue.Num(v.intValue)
    case v: java.net.URI             => AttrValue.Ref(v)
    case v: java.time.OffsetDateTime => AttrValue.Time(v)
    case v: Array[Byte]              => AttrValue.Bytes(Binary.copyOf(v))
    case other                       => AttrValue.Text(other.toString)
