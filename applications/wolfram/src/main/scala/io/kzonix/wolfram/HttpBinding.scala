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

package io.kzonix.wolfram

import io.circe.Json
import io.circe.parser
import io.kzonix.eventing.ContentMode
import io.kzonix.kernel.event.Envelope
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.Locale

/** The CloudEvents **HTTP** protocol binding: HTTP headers plus a body, in, an [[Envelope]] out.
  *
  * `modules/eventing` owns the *Kafka* binding, and the two are deliberately not the same code. They differ in the one
  * place that matters — Kafka header names are `ce_type`, HTTP header names are `ce-type`, because Kafka historically
  * disallowed `-` in header keys and the CloudEvents spec follows each transport's convention. Sharing an
  * implementation across them would mean a prefix parameter threaded through every function and a class of bug
  * (accepting `ce_type` over HTTP, which no conformant client sends) that no test would think to look for.
  *
  * What *is* shared, and must be, is the decoding of the attributes themselves: this object assembles a CloudEvents
  * JSON Format 1.0 document from the headers and hands it to `modules/kernel`'s hand-written codec. That gives binary
  * mode and structured mode literally the same validation, the same error sentences and the same notion of what an
  * extension is — which is what makes "the same event sent in either mode produces the same row" true by construction
  * rather than by a test that compares two parsers.
  *
  * The cost is one round trip through `Json` for a binary-mode request. It is worth it: the alternative is a second
  * attribute parser in a second module, and ADR §2 exists precisely because a second copy of the wire contract drifts.
  */
object HttpBinding:

  /** The structured-mode media type (ADR §4.3). Matched on the essence, so `; charset=utf-8` is still understood. */
  val StructuredMediaType: String = ContentMode.StructuredMediaType

  /** The batch media type. A JSON array of CloudEvents documents. */
  val BatchMediaType: String = "application/cloudevents-batch+json"

  /** HTTP header prefix for context attributes in binary mode. Note the hyphen — see the object Scaladoc. */
  val AttributePrefix: String = "ce-"

  /** The header that carries `datacontenttype` in binary mode.
    *
    * In binary mode the CloudEvents spec maps `datacontenttype` onto HTTP's own `Content-Type` rather than onto a
    * `ce-datacontenttype` header, because the body genuinely *is* the data and any intermediary must be able to see its
    * type. A producer that sends `ce-datacontenttype` anyway is being read here as well, since ignoring it would drop
    * an attribute the producer clearly meant.
    */
  val ContentTypeHeader: String = "content-type"

  /** Which content mode a request is in, or `None` when the headers claim neither.
    *
    * The decision order mirrors `modules/eventing`'s `ContentMode.of` and is load-bearing for the same reason: in
    * binary mode `Content-Type` describes the *payload*, and a payload may perfectly well be a CloudEvents document (a
    * forwarder, a replay tool, an event that quotes another event). Deciding on the media type first misreads exactly
    * those requests as structured and throws away every attribute in the headers. `ce-specversion` is unambiguous — a
    * structured request never carries it, because its specversion is inside the JSON — so it is checked first.
    */
  def modeOf(headers: Map[String, String]): Option[ContentMode] =
    if headers.contains(AttributePrefix + "specversion") then Some(ContentMode.Binary)
    else if essence(headers.get(ContentTypeHeader)).contains(StructuredMediaType) then Some(ContentMode.Structured)
    else None

  /** True when the request declares itself a batch. Batch is not a [[ContentMode]] — the spec models it as its own
    * media type carrying an array of *structured* events — so it is asked about separately.
    */
  def isBatch(headers: Map[String, String]): Boolean =
    essence(headers.get(ContentTypeHeader)).contains(BatchMediaType)

  /** Lower-cases header names and keeps the last value for a repeated name.
    *
    * HTTP header names are case-insensitive and this is the only place that fact is handled, so every lookup below can
    * be a plain map lookup. "Last wins" matches what the Kafka binding does with `Headers.lastHeader` and what proxies
    * do in practice; a repeated CloudEvents attribute is a malformed request either way, and rejecting on it would
    * reject requests that traversed a proxy that appended rather than replaced.
    */
  def normalise(headers: Iterable[(String, String)]): Map[String, String] =
    headers.iterator.map((name, value) => name.toLowerCase(Locale.ROOT) -> value).toMap

  /** Decodes one request in whichever mode its headers declare.
    *
    * A request that declares neither mode is [[Rejection.Malformed]] rather than defaulted to structured: guessing here
    * would accept a plain `application/json` POST as a CloudEvent and then reject it three lines later with a message
    * about a missing `id`, which sends the client looking in the wrong place.
    */
  def decode(headers: Map[String, String], body: Array[Byte]): Either[Rejection, Envelope] =
    modeOf(headers) match
      case Some(ContentMode.Binary)     => decodeBinary(headers, body)
      case Some(ContentMode.Structured) => decodeStructured(body)
      case None                         =>
        Left(
          Rejection.Malformed(
            s"request carries neither a '${AttributePrefix}specversion' header (binary mode) nor a " +
              s"'$ContentTypeHeader: $StructuredMediaType' header (structured mode)"
          )
        )

  /** Structured mode: the body is the whole event. Straight to kernel's codec. */
  def decodeStructured(body: Array[Byte]): Either[Rejection, Envelope] =
    if body.isEmpty then Left(Rejection.Malformed("structured-mode request has an empty body"))
    else classify(Envelope.parse(String(body, UTF_8)))

  /** Binary mode: attributes are headers, the body is the data, untouched.
    *
    * The body is *not* re-encoded. When `Content-Type` says JSON it is spliced into the `data` slot as JSON so that the
    * stored `raw jsonb` is queryable; otherwise it goes into `data_base64`, which is exactly the choice the JSON format
    * makes and therefore keeps `decode(binary) == decode(structured)` for the same event.
    */
  def decodeBinary(headers: Map[String, String], body: Array[Byte]): Either[Rejection, Envelope] =
    val contentType = headers
      .get(AttributePrefix + "datacontenttype")
      .orElse(headers.get(ContentTypeHeader))
      .map(_.trim)
      .filter(_.nonEmpty)

    val attributes = headers.iterator
      .collect:
        case (name, value) if name.startsWith(AttributePrefix) =>
          name.drop(AttributePrefix.length) -> Json.fromString(value)
      .toVector

    dataField(body, contentType).flatMap: data =>
      val fields = attributes ++
        contentType.map(value => "datacontenttype" -> Json.fromString(value)).toVector ++
        data.toVector
      classify(Envelope.decoder.decodeJson(Json.fromFields(fields)).left.map(_.message))

  /** Decodes an `application/cloudevents-batch+json` document into one result per element.
    *
    * The outer failure (not JSON, not an array) is a whole-request rejection; per-element failures are *not*, because a
    * batch is a convenience for the producer and not a transaction. Losing 255 good events because the 256th has a typo
    * would make the batch endpoint strictly worse than 256 single POSTs, which is the opposite of why it exists.
    */
  def decodeBatch(body: Array[Byte]): Either[Rejection, Vector[Either[Rejection, Envelope]]] =
    if body.isEmpty then Left(Rejection.Malformed("batch request has an empty body"))
    else
      parser
        .parse(String(body, UTF_8))
        .left
        .map(failure => Rejection.Malformed(failure.message))
        .flatMap: json =>
          json.asArray.toRight(
            Rejection.Malformed(s"a '$BatchMediaType' body must be a JSON array of CloudEvents")
          )
        .map(_.map(element => classify(Envelope.decoder.decodeJson(element).left.map(_.message))))

  /** Splits a kernel decode failure into the two categories the API distinguishes.
    *
    * Kernel collapses "not JSON" and "not a CloudEvent" into one message because both of its callers treat them alike.
    * An HTTP client does not: "your body is not JSON" and "your event is missing `id`" are fixed in different places,
    * and the metric tags differ too. The split is done on kernel's own sentences, which are stable because they are
    * asserted on in `modules/kernel`'s own tests.
    */
  private def classify(result: Either[String, Envelope]): Either[Rejection, Envelope] =
    result.left.map: message =>
      if message.contains("must be a JSON object") || message.startsWith("expected") then
        Rejection.Malformed(message)
      else Rejection.InvalidAttributes(message)

  /** The `data` / `data_base64` field for a binary-mode body, or none when the body is empty. */
  private def dataField(body: Array[Byte], contentType: Option[String]): Either[Rejection, Option[(String, Json)]] =
    if body.isEmpty then Right(None)
    else if isJson(contentType) then
      parser
        .parse(String(body, UTF_8))
        .left
        .map(failure =>
          Rejection.Malformed(s"body is declared ${contentType.getOrElse("JSON")} but is not JSON: ${failure.message}")
        )
        .map(json => Some("data" -> json))
    else Right(Some("data_base64" -> Json.fromString(Base64.getEncoder.encodeToString(body))))

  /** JSON by the `+json` structured-suffix rule of RFC 6839, which is what the CloudEvents spec defers to. A body with
    * no declared type is treated as opaque: guessing JSON from the bytes would make an event's stored shape depend on
    * whether its first byte happened to be `{`.
    */
  private def isJson(contentType: Option[String]): Boolean =
    essence(contentType).exists(value => value == "application/json" || value.endsWith("+json"))

  /** The media type without parameters, lower-cased. */
  private def essence(contentType: Option[String]): Option[String] =
    contentType.map(_.takeWhile(_ != ';').trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty)
