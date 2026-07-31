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

package com.worxbend.cobalt

import com.worxbend.eventing.CloudEventHeaders
import com.worxbend.eventing.ContentMode
import com.worxbend.eventing.DeadLetter
import com.worxbend.kernel.Rfc3339
import io.circe.Json
import io.circe.parser
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import scala.util.control.NonFatal

/** One record as it currently sits on the DLQ topic.
  *
  * `entry` is an `Either` rather than a [[DeadLetter]] because the DLQ is a topic like any other, and nothing stops a
  * misconfigured producer, an older build, or a hand-run `kafka-console-producer` from putting something else on it. An
  * inspection tool that throws on the first unreadable record is useless precisely when the DLQ is the thing that has
  * gone wrong, so an unreadable record is a value with a stated reason and is listed beside the readable ones.
  *
  * @param key
  *   the DLQ record key, which by construction is the origin `topic/partition/offset` of the record that died.
  */
final case class DlqRecord(
  partition: Int,
  offset: Long,
  timestamp: Option[Long],
  key: Option[String],
  entry: Either[String, DeadLetter]
):

  /** The stable handle for this dead letter.
    *
    * Normally the DLQ key — the origin coordinates — which is what makes naming a record in a replay mean the same
    * thing twice: the key is a property of the record that died, so it survives the DLQ being compacted and the dead
    * letter moving to a different DLQ offset. The `@partition/offset` fallback covers a record written without a key,
    * which this build never produces but a foreign writer might.
    */
  def ref: String = key.getOrElse(s"@$partition/$offset")

/** Why the planner declined to replay a dead letter.
  *
  * A closed set, and it stays closed: these strings appear in the response body an operator reads under time pressure
  * and in the log line that outlives the request. They are deliberately *not* a meter tag — see
  * [[com.worxbend.observability.Meters.DlqReplayRecords]] for why the skip count is untagged.
  */
enum ReplaySkip(val tag: String):

  /** The DLQ record is not a readable dead letter. Replaying it would mean guessing what it was. */
  case Undecodable extends ReplaySkip("undecodable")

  /** The dead letter's origin topic is not the topic this consumer owns.
    *
    * **The check that keeps this endpoint from being a general-purpose producer.** The destination is read out of the
    * record's own payload, so without it the DLQ becomes a way to make cobalt write to any topic in the cluster that a
    * foreign writer — or a copied record from another environment — can name.
    */
  case ForeignTopic extends ReplaySkip("foreign-topic")

  /** Already replayed [[ReplayConfig.maxAttempts]] times, or the attempt header is unreadable.
    *
    * **This is the bound on the poison loop.** A record that fails again after a replay lands back on the DLQ under a
    * *new* key — the origin coordinates changed, because the replayed record occupies a new offset on the main topic —
    * so the DLQ's own compaction key does not stop one defect accumulating a dead letter per replay. The counter in the
    * headers does, and it survives every generation because a dead letter records the record's headers verbatim.
    */
  case BudgetExhausted extends ReplaySkip("budget-exhausted")

/** What a replay operation was asked to act on.
  *
  * Two shapes, because an operator means two different things and they need different failure behaviour.
  *
  *   - [[Recent]] is "whatever is in there, up to N". The set is discovered, so a record the planner declines is
  *     reported and the rest still go.
  *   - [[Named]] is "these exact records". The set is stated, so anything the planner cannot find or cannot replay
  *     refuses the whole operation — publishing three of the five records somebody named is the half-success this
  *     endpoint exists not to produce.
  */
enum ReplayScope:

  case Recent(limit: Int, reason: Option[String])

  case Named(refs: Vector[String])

/** A parsed, bounded replay request. Build it with [[ReplayRequest.parse]]; the fields are validated by then. */
final case class ReplayRequest(scope: ReplayScope, dryRun: Boolean):

  /** How many DLQ records the store must fetch to answer this. Both shapes are bounded before any I/O happens. */
  def fetchLimit(maxRecords: Int): Int = scope match
    case ReplayScope.Recent(limit, _) => limit
    case ReplayScope.Named(_)         => maxRecords

object ReplayRequest:

  /** What the routes pass when the caller supplied no `limit`.
    *
    * **A sentinel exists because a Cask annotation's default must be a compile-time constant, and the ceiling is
    * configuration.** With a literal default of 20 and a deployment that sets `max-records = 5`, every bare
    * `GET /admin/dlq/records` would answer 400 forever — the request nobody supplies parameters to, refused for
    * exceeding a limit nobody asked to exceed. Resolving "unspecified" here against the actual ceiling removes that
    * whole class, and zero is a safe spelling for it: nobody meaningfully asks for no records.
    */
  val UnspecifiedLimit: Int = 0

  /** The page size an unspecified request gets, capped by the ceiling. Small enough to read, large enough that the
    * usual answer to "what is in the DLQ" needs one request.
    */
  val DefaultLimit: Int = 20

  /** Parses the query parameters, refusing anything out of bounds.
    *
    * **Out-of-range limits are refused, never clamped.** An operator who asks to replay 500 records and is silently
    * given 200 believes 500 events are back in the pipeline, and the 300 that are not get discovered by somebody else,
    * later, from a gap in a dashboard. Refusing costs one more command and removes the whole failure class. The
    * *unspecified* case is the one exception, and it is not clamping: nothing was asked for, so nothing is overridden.
    *
    * `refs` and `limit` are alternatives rather than a conjunction: naming records explicitly *is* the bound, so a
    * `refs` list is checked against `maxRecords` on its own and `limit` is ignored. Combining them would create a
    * request whose meaning — "the first ten of these forty" — nobody would guess the same way twice.
    */
  def parse(
    limit: Int,
    reason: String,
    refs: String,
    dryRun: Boolean,
    maxRecords: Int
  ): Either[String, ReplayRequest] =
    val named = refs.split(',').iterator.map(_.trim).filter(_.nonEmpty).toVector.distinct
    val asked = if limit == UnspecifiedLimit then math.min(DefaultLimit, maxRecords) else limit
    if named.nonEmpty then
      if named.size > maxRecords then
        Left(s"${named.size} records named, but one replay may touch at most $maxRecords (cobalt.replay.max-records)")
      else Right(ReplayRequest(ReplayScope.Named(named), dryRun))
    else if asked < 1 then Left(s"limit must be at least 1, was $limit")
    else if asked > maxRecords then
      Left(s"limit $asked exceeds the ceiling of $maxRecords on one replay (cobalt.replay.max-records)")
    else Right(ReplayRequest(ReplayScope.Recent(asked, Option(reason).map(_.trim).filter(_.nonEmpty)), dryRun))

/** What the planner decided about one candidate. */
enum ReplayDecision:

  /** Replayable, as generation `attempt` — `1` for a record that has never been replayed. */
  case Replay(source: DlqRecord, deadLetter: DeadLetter, attempt: Int)

  case Skip(source: DlqRecord, reason: ReplaySkip)

/** Everything a replay would do, computed before anything is published.
  *
  * **The plan is the product; committing it is an afterthought.** Splitting the two is what makes the dry run honest:
  * one function produces the plan whether or not it will be executed, so a dry run is not an approximation of the
  * replay — it *is* the replay, minus the produce loop. An operator who cannot see what a replay would do will not run
  * one during an incident, which makes the tool worthless exactly when it is needed.
  *
  * @param missing
  *   named refs that were not among the records fetched. Non-empty means the operation is refused; see [[refusal]].
  */
final case class ReplayPlan(
  scope: ReplayScope,
  dryRun: Boolean,
  decisions: Vector[ReplayDecision],
  missing: Vector[String]
):

  def replays: Vector[ReplayDecision.Replay] = decisions.collect { case replay: ReplayDecision.Replay => replay }

  def skips: Vector[ReplayDecision.Skip] = decisions.collect { case skip: ReplayDecision.Skip => skip }

  /** Why this plan must not be committed at all, or `None`.
    *
    * Only [[ReplayScope.Named]] can refuse, and it refuses for both halves of "these exact records": a ref that was not
    * found, and a ref that was found but declined. Either way the stated set cannot be satisfied in full, and the
    * contract for a stated set is all-or-nothing. A [[ReplayScope.Recent]] selection never refuses — the operator asked
    * for whatever is there, and "these three were skipped" is an answer to that question rather than a failure of it.
    */
  def refusal: Option[String] = scope match
    case ReplayScope.Recent(_, _) => None
    case ReplayScope.Named(refs)  =>
      if missing.nonEmpty then
        Some(s"${missing.size} of ${refs.size} named dead letters are not in the fetch window; nothing was published")
      else if skips.nonEmpty then
        val reasons = skips.map(_.reason.tag).distinct.sorted.mkString(", ")
        Some(s"${skips.size} of ${refs.size} named dead letters cannot be replayed ($reasons); nothing was published")
      else None

/** The replay markers this build adds to a republished record.
  *
  * **Neither name carries the `ce_` prefix, and that is load-bearing.** A `ce_`-prefixed header *is* a CloudEvents
  * context attribute under the Kafka binding, so spelling these `ce_replayattempt` would change the event: a different
  * extension set, a different `raw jsonb`, a different row. As plain transport headers they are invisible to the
  * decoder and to everything downstream of it, which is exactly what "indistinguishable from the original" needs.
  */
object ReplayHeaders:

  /** How many times this record has been replayed. Absent means never. */
  val Attempt: String = "x-worxbend-replay-attempt"

  /** The DLQ key of the dead letter this record was rebuilt from — the previous generation's origin coordinates.
    *
    * Together with [[Attempt]] this turns a poison loop into a readable chain rather than a pile of unrelated dead
    * letters: each generation names its predecessor, so "this event has been going round for three days" is one field
    * instead of a correlation exercise.
    */
  val Of: String = "x-worxbend-replay-of"

  /** How many replays the recorded headers say this record has survived.
    *
    * A `Left` — a header present but not a non-negative integer — is read by the planner as *budget exhausted*, not as
    * zero. Nothing in this build writes anything else there, so an unparseable value means something outside it did,
    * and the conservative reading of "I cannot tell how many times this has gone round" is "do not send it round
    * again".
    */
  def attemptsOf(headers: Map[String, String]): Either[String, Int] =
    headers.get(Attempt) match
      case None        => Right(0)
      case Some(value) =>
        value.trim.toIntOption match
          case Some(count) if count >= 0 => Right(count)
          case _                         => Left(s"$Attempt is '$value', which is not a replay count")

/** Selection, bounding and record reconstruction — every decision a replay makes, with no broker in sight.
  *
  * The split from [[DeadLetterAdmin]] is the one [[AdminHandlers]] already makes and for the same reason: the
  * interesting behaviour is *which records* and *in what order*, and asserting a `Vector` should not cost a container.
  */
object DeadLetterReplay:

  /** How much of a payload the listing shows, in bytes taken from the front.
    *
    * Bounded because the alternative is an OOM in the service whose job is to tell you it is unhealthy: `max-records`
    * dead letters times an unbounded payload is an unbounded response, and the DLQ is exactly where the oversized
    * records end up. 512 bytes is past the point where a malformed JSON document reveals what is wrong with it; the
    * full record is one `kcat` away, and the listing exists to say which record to go and look at.
    */
  val PreviewBytes: Int = 512

  /** The CloudEvents attributes a listing entry reports, in the order they are useful. */
  private val Attributes: Vector[String] = Vector("id", "type", "source", "subject", "time")

  /** Turns the fetched DLQ records into a complete plan.
    *
    * `records` arrives newest-first, because "the last N dead letters" is what an operator means and the newest are the
    * ones from the incident in progress. Publication order is the reverse: [[ReplayPlan.replays]] comes out
    * oldest-first, so a device's events reach the topic in the order they originally did rather than backwards.
    * Ordering *within* a partition is preserved for free — the origin key is republished unchanged, so the same
    * partitioner sends the record to the same partition.
    */
  def plan(records: Vector[DlqRecord], request: ReplayRequest, ownTopic: String, maxAttempts: Int): ReplayPlan =
    val (candidates, missing) = request.scope match
      case ReplayScope.Recent(limit, reason) =>
        val matching = records.filter(record => reason.forall(want => record.entry.exists(_.reason == want)))
        (matching.take(limit), Vector.empty[String])
      case ReplayScope.Named(refs) =>
        val byRef = records.iterator.map(record => record.ref -> record).toMap
        (refs.flatMap(byRef.get), refs.filterNot(byRef.contains))
    val ordered =
      candidates.sortBy(record => (record.timestamp.getOrElse(Long.MinValue), record.partition, record.offset))
    ReplayPlan(request.scope, request.dryRun, ordered.map(classify(_, ownTopic, maxAttempts)), missing)

  /** The per-record decision. Total: every candidate becomes a `Replay` or a `Skip` with a stated reason. */
  def classify(record: DlqRecord, ownTopic: String, maxAttempts: Int): ReplayDecision =
    record.entry match
      case Left(_)           => ReplayDecision.Skip(record, ReplaySkip.Undecodable)
      case Right(deadLetter) =>
        if deadLetter.origin.topic != ownTopic then ReplayDecision.Skip(record, ReplaySkip.ForeignTopic)
        else
          ReplayHeaders.attemptsOf(deadLetter.headers) match
            case Left(_)                            => ReplayDecision.Skip(record, ReplaySkip.BudgetExhausted)
            case Right(done) if done >= maxAttempts => ReplayDecision.Skip(record, ReplaySkip.BudgetExhausted)
            case Right(done)                        => ReplayDecision.Replay(record, deadLetter, done + 1)

  /** Rebuilds the record that died, ready to go back on the topic it came from.
    *
    * **Nothing is re-encoded.** The value is the original bytes and the headers are the original headers, copied
    * verbatim; the envelope is never decoded and re-serialised. That matters twice. It is the only way to replay a
    * record that *cannot* be decoded — which is most of what is on a DLQ — and it keeps the CloudEvents `id` and
    * `source` byte-identical, which is the entire reason replaying an event that DID land is a no-op:
    * `ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING` only absorbs a duplicate whose identity survived.
    *
    * The key is the original key, so the record lands on the partition it originally did and per-device ordering
    * survives the replay. `traceparent`, if the original carried one, is copied with everything else, so the replayed
    * event continues the trace that started at wolfram's ingress days earlier — which is what makes "this row came from
    * a replay of that request" answerable at all.
    *
    * Partition and timestamp are left null for the broker to assign, as `KafkaCodecs.producerRecord` does: pinning the
    * original partition number breaks the day the topic is expanded, and pinning the original timestamp would hide the
    * replay in the log's own time ordering.
    */
  def producerRecord(replay: ReplayDecision.Replay): ProducerRecord[String, Array[Byte]] =
    val headers = RecordHeaders()
    replay.deadLetter.headers.toVector.sortBy((name, _) => name).foreach: (name, value) =>
      val _ = CloudEventHeaders.put(headers, name, value)
    val _ = CloudEventHeaders.put(headers, ReplayHeaders.Attempt, replay.attempt.toString)
    val _ = CloudEventHeaders.put(headers, ReplayHeaders.Of, replay.deadLetter.origin.dlqKey)
    val noPartition: java.lang.Integer = null
    val noTimestamp: java.lang.Long = null
    ProducerRecord(
      replay.deadLetter.origin.topic,
      noPartition,
      noTimestamp,
      replay.deadLetter.origin.key.orNull,
      replay.deadLetter.payload.map(_.toArray).orNull,
      headers
    )

  /** The CloudEvents identity of a dead letter, best effort.
    *
    * Best effort is the only kind on offer: a record is usually on the DLQ *because* its attributes could not be read,
    * so there is no decode to lean on. The headers are tried first, since the main topic is binary mode and that is
    * where the attributes live; the payload is tried second, for a structured-mode record. A record that yields neither
    * is reported with an empty object rather than omitted — "a dead letter whose id is unknowable" is itself the
    * diagnosis, and hiding it would make the listing shorter than the DLQ.
    */
  def identity(deadLetter: DeadLetter): Map[String, String] =
    val fromHeaders = Attributes.flatMap: name =>
      deadLetter.headers.get(CloudEventHeaders.attribute(name)).filter(_.nonEmpty).map(name -> _)
    if fromHeaders.nonEmpty then fromHeaders.toMap
    else
      deadLetter.payload
        .flatMap(bytes => parser.parse(String(bytes.toArray, UTF_8)).toOption)
        .flatMap(_.asObject)
        .map: obj =>
          Attributes.flatMap(name => obj(name).flatMap(_.asString).filter(_.nonEmpty).map(name -> _)).toMap
        .getOrElse(Map.empty)

  /** The first [[PreviewBytes]] of the payload as text, plus its true length.
    *
    * Decoded as UTF-8 with replacement, which cannot throw on arbitrary bytes and cannot produce a string circe refuses
    * to render. The cut is by *bytes*, not characters, so the work is bounded by the constant rather than by the
    * payload: a 4 MB record costs 512 bytes of decoding here and not 4 MB. The price is at most one replacement
    * character where the cut lands mid-sequence, which is a fair trade for a preview.
    */
  def preview(deadLetter: DeadLetter): Json =
    deadLetter.payload match
      case None        => Json.obj("bytes" -> Json.fromInt(0), "truncated" -> Json.False, "text" -> Json.Null)
      case Some(value) =>
        val bytes = value.toArray
        val head = if bytes.length > PreviewBytes then bytes.take(PreviewBytes) else bytes
        Json.obj(
          "bytes" -> Json.fromInt(bytes.length),
          "truncated" -> Json.fromBoolean(bytes.length > PreviewBytes),
          "text" -> Json.fromString(String(head, UTF_8))
        )

  /** One listing entry: why it died, where it came from, who it claims to be, and enough payload to tell. */
  def render(record: DlqRecord): Json =
    val base = Vector(
      "ref" -> Json.fromString(record.ref),
      "dlqPartition" -> Json.fromInt(record.partition),
      "dlqOffset" -> Json.fromLong(record.offset),
      "dlqTimestamp" -> record.timestamp.fold(Json.Null)(Json.fromLong)
    )
    record.entry match
      case Left(problem) =>
        Json.fromFields(base ++ Vector("readable" -> Json.False, "problem" -> Json.fromString(problem)))
      case Right(deadLetter) =>
        Json.fromFields(
          base ++ Vector(
            "readable" -> Json.True,
            "reason" -> Json.fromString(deadLetter.reason),
            "detail" -> Json.fromString(deadLetter.detail),
            "failedAt" -> Json.fromString(Rfc3339.render(deadLetter.failedAt)),
            "consumer" -> Json.fromString(deadLetter.source),
            "origin" -> Json.obj(
              "topic" -> Json.fromString(deadLetter.origin.topic),
              "partition" -> Json.fromInt(deadLetter.origin.partition),
              "offset" -> Json.fromLong(deadLetter.origin.offset),
              "key" -> deadLetter.origin.key.fold(Json.Null)(Json.fromString)
            ),
            "event" -> Json.fromFields(
              identity(deadLetter).toVector.sortBy((name, _) => name).map((k, v) => k -> Json.fromString(v))
            ),
            "replayAttempts" -> ReplayHeaders.attemptsOf(deadLetter.headers).fold(_ => Json.Null, Json.fromInt),
            "payload" -> preview(deadLetter)
          )
        )

  /** Reads a DLQ record back into a dead letter, never throwing.
    *
    * Totality is the requirement, not a nicety: this runs once per record in a listing, and an exception on record
    * seventeen would lose the other ninety-nine — including, quite possibly, the one the operator is looking for.
    */
  def readDeadLetter(headers: Headers, value: Option[Array[Byte]]): Either[String, DeadLetter] =
    try ContentMode.read(headers, value).left.map(_.message).flatMap(DeadLetter.fromEnvelope)
    catch case NonFatal(error) => Left(RecordDecoder.describe(error))
