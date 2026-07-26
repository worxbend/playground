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

package io.kzonix.kernel.event

import scala.util.control.NonFatal

import io.circe.Decoder
import io.circe.DecodingFailure
import io.kzonix.kernel.search.Severity

/** The reverse-DNS `type` strings this build recognises.
  *
  * Versions are absent by design: a type string that carries its version forks the registry on every additive schema
  * change and turns "all telemetry" into a prefix match. Versioning hangs off `dataschema` (ADR §4.2).
  */
object EventTypes:

  val Telemetry: String     = "io.kzonix.iot.telemetry"
  val StateChanged: String  = "io.kzonix.iot.state-changed"
  val Alarm: String         = "io.kzonix.iot.alarm"

/** The strongly typed reading refined out of an [[Envelope]].
  *
  * `Unrecognised` is not an error case, it is the *total fallback*: without it, persistence and the UI would depend on
  * this enum being complete, and a firmware update that ships a new event type would start dropping data (ADR §4.2).
  * Every `Unrecognised` carries the payload and, when the type was known but the payload did not fit, a reason — that
  * pair is what the `event.unrecognised{type,reason}` counter is tagged with, and it is the only way to tell a new
  * device apart from a broken decoder.
  */
enum Observation:
  case Telemetry(device: Subject, metric: String, value: Double, unit: String)
  case StateChanged(device: Subject, from: String, to: String)
  case Alarm(device: Subject, severity: Int, message: String)
  case Unrecognised(eventType: EventType, payload: Payload, reason: Option[String])

/** An envelope paired with its refinement.
  *
  * Refinement is lossy by construction — `Unrecognised` keeps the payload but not the source, extensions or time — so
  * anything that both routes on the observation and persists the event carries this pair rather than choosing one.
  */
final case class Observed(envelope: Envelope, observation: Observation)

object Observed:

  def of(envelope: Envelope): Observed = Observed(envelope, Observation.from(envelope))

object Observation:

  /** The major assumed when an event carries no `dataschema` at all, which is the common case for a device that has
    * only ever emitted one shape. There is deliberately **no** fallback from an unregistered major to this one: a major
    * bump means the payload changed incompatibly, so decoding 2.x with the 1.x decoder would produce a confidently
    * wrong reading rather than an honest `Unrecognised`.
    */
  val DefaultMajor: Int = 1

  /** Keyed on `(type, schema major)` (ADR §4.2). Minor and patch bumps must be additive and these decoders ignore
    * unknown fields, so one entry serves every 1.x of a schema; a major bump is a deliberate new registration.
    *
    * The value is a function of the resolved device rather than a bare `Decoder[Observation]`: the device identity
    * comes from the envelope's `subject`, not from `data`, so it cannot be recovered from the payload cursor alone.
    */
  private val registry: Map[(String, Int), Subject => Decoder[Observation]] = Map(
    (EventTypes.Telemetry, DefaultMajor)    -> telemetryDecoder,
    (EventTypes.StateChanged, DefaultMajor) -> stateChangedDecoder,
    (EventTypes.Alarm, DefaultMajor)        -> alarmDecoder
  )

  /** Total. Never throws, never returns `Either`.
    *
    * An `Either` here would push the decision onto every call site and the answer would be the same every time: keep
    * the event. Failure is therefore data — an `Unrecognised` with a reason — not control flow.
    *
    * The `NonFatal` guard is belt and braces around third-party decoders: circe's own combinators return `Left`, but
    * this function's totality is a promise made to the ingest path, and a promise that depends on a library's internal
    * discipline is not a promise.
    */
  def from(envelope: Envelope): Observation =
    try
      val major = envelope.schema.flatMap(_.major).getOrElse(DefaultMajor)
      registry.get((envelope.eventType, major)).fold(unrecognised(envelope, None))(refine(envelope, _))
    catch case NonFatal(error) => unrecognised(envelope, Some(reasonOf(error)))

  private def refine(envelope: Envelope, decoder: Subject => Decoder[Observation]): Observation =
    (deviceOf(envelope), envelope.payload) match
      case (None, _) =>
        unrecognised(envelope, Some("no device identity: neither subject nor data.deviceId is present"))
      case (Some(device), Payload.Structured(json)) =>
        decoder(device).decodeJson(json) match
          case Right(observation) => observation
          case Left(failure)      => unrecognised(envelope, Some(failure.message))
      case (Some(_), _) =>
        unrecognised(envelope, Some("data is not a JSON payload"))

  /** `subject` first: it is the attribute the partition key and every index are built on, so a disagreement between it
    * and `data.deviceId` must resolve the same way here as it does in the database.
    */
  private def deviceOf(envelope: Envelope): Option[Subject] =
    envelope.subject.orElse:
      envelope.payload match
        case Payload.Structured(json) =>
          json.hcursor.get[String]("deviceId").toOption.flatMap(Subject.apply(_).toOption)
        case _ => None

  private def unrecognised(envelope: Envelope, reason: Option[String]): Observation =
    Unrecognised(envelope.eventType, envelope.payload, reason)

  private def reasonOf(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getName)

  private def telemetryDecoder(device: Subject): Decoder[Observation] =
    Decoder.instance: cursor =>
      for
        metric <- cursor.get[String]("metric")
        value  <- cursor.get[Double]("value")
        unit   <- cursor.get[String]("unit")
      yield Telemetry(device, metric, value, unit)

  private def stateChangedDecoder(device: Subject): Decoder[Observation] =
    Decoder.instance: cursor =>
      for
        from <- cursor.get[String]("from")
        to   <- cursor.get[String]("to")
      yield StateChanged(device, from, to)

  private def alarmDecoder(device: Subject): Decoder[Observation] =
    Decoder.instance: cursor =>
      for
        severity <- severityRank.tryDecode(cursor.downField("severity"))
        message  <- cursor.get[String]("message")
      yield Alarm(device, severity, message)

  /** Producers spell severity either way, and the DDL of ADR §5 stores the text while indexing the rank; accepting both
    * forms here keeps the domain value and the generated column in agreement.
    */
  private val severityRank: Decoder[Int] =
    Decoder.instance: cursor =>
      cursor
        .as[Int]
        .orElse:
          cursor.as[String].flatMap { raw =>
            Severity.parse(raw).left.map(message => DecodingFailure(message, cursor.history)).map(_.rank)
          }

  /** Test and tooling seam: the `(type, major)` pairs the registry currently answers for. The `event.unrecognised`
    * alert of ADR §4.2 is only meaningful against a known list.
    */
  def knownTypes: Set[(String, Int)] = registry.keySet
