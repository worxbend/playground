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

import io.circe.Codec
import io.circe.Encoder
import io.circe.Json
import java.time.Instant

/** What the consumer is doing right now.
  *
  * **A closed enum and not a boolean pair.** "running" and "paused" as two flags admits four states, two of which are
  * nonsense, and every reader then has to know which combination means what. One value with six cases makes the
  * illegal states unrepresentable and makes the admin response a single word an operator can read.
  *
  * The distinction that matters most is [[Stopped]] versus [[Failed]]. Both mean "not consuming"; only one of them is
  * somebody's fault. A supervisor that reported a crashed stream as `stopped` would look exactly like one an operator
  * had paused on purpose, and lag would grow while the dashboard said everything was fine.
  */
enum RunState(val name: String, val consuming: Boolean):

  /** A stream is being materialised. Transient; if it persists, the broker is unreachable. */
  case Starting extends RunState("starting", true)

  /** Consuming. The only state in which lag should be shrinking. */
  case Running extends RunState("running", true)

  /** Deliberately not consuming. The stream is torn down, not merely idle — see `ConsumerSupervisor.pause`. */
  case Paused extends RunState("paused", false)

  /** A drain is in flight: the fetcher is stopped, the in-flight batch is finishing and its offsets will commit. */
  case Stopping extends RunState("stopping", false)

  /** Not consuming, by request, and nothing is wrong. */
  case Stopped extends RunState("stopped", false)

  /** Not consuming because the stream died and the restart policy gave up. Always carries the cause. */
  case Failed extends RunState("failed", false)

object RunState:

  /** Parses the wire form. Total, because it decodes operator input. */
  def parse(value: String): Option[RunState] =
    values.find(_.name.equalsIgnoreCase(value.trim))

  given Encoder[RunState] = Encoder.encodeString.contramap(_.name)

/** Where a restart should begin.
  *
  * **`Stored` is the default and the interesting one.** Kafka's `auto.offset.reset` only applies when the group has no
  * committed offset at all, so it cannot express "resume from the position I durably recorded" — which is what an
  * operator almost always means after an incident. The externalised checkpoint can.
  */
enum SeekTarget(val name: String):

  /** Whatever the group has committed to the broker. The plain restart. */
  case Committed extends SeekTarget("committed")

  /** The externalised checkpoints in `events.consumer_checkpoint`, which outlive `offsets.retention.minutes` and are
    * written in the same transaction as the rows they account for.
    */
  case Stored extends SeekTarget("stored")

  /** The beginning of the retained log. Replays everything; safe only because the insert deduplicates, and expensive
    * regardless — a full replay of a week of retention is a week of writes.
    */
  case Earliest extends SeekTarget("earliest")

  /** The end of the log. **Skips** whatever has not been consumed; the only target here that loses data. */
  case Latest extends SeekTarget("latest")

  /** Explicit per-partition offsets, supplied by the operator. */
  case Explicit extends SeekTarget("explicit")

object SeekTarget:

  def parse(value: String): Option[SeekTarget] = values.find(_.name.equalsIgnoreCase(value.trim))

  given Encoder[SeekTarget] = Encoder.encodeString.contramap(_.name)

/** One partition's position, as the admin API reports it.
  *
  * The four numbers side by side are the point. `committed` is where the group says it is, `stored` is where the
  * checkpoint says it is, `endOffset` is where the log actually ends, and `lag` is the gap. A disagreement between
  * `committed` and `stored` is the single most useful diagnostic this API produces — it means one of the two commits
  * did not happen, and which one tells you whether events were lost or will be replayed.
  */
final case class PartitionPosition(
  topic: String,
  partition: Int,
  committed: Option[Long],
  stored: Option[Long],
  endOffset: Option[Long],
  lag: Option[Long]
) derives Codec.AsObject

/** The consumer's whole observable state, in one response.
  *
  * One object rather than four endpoints because these values are only meaningful together: a `lag` of 40 000 means
  * something different when `state` is `paused` than when it is `running`, and an operator who has to make two
  * requests to find that out will make them minutes apart during an incident.
  */
final case class ConsumerStatus(
  state: RunState,
  since: Instant,
  generation: Int,
  groupId: String,
  topic: String,
  consuming: Boolean,
  lastError: Option[String],
  restarts: Int,
  positions: List[PartitionPosition],
  totalLag: Option[Long]
)

object ConsumerStatus:

  /** Hand-written rather than derived so `state` renders as its wire name and the JSON field order is the reading
    * order — `state` first, because that is the answer to the question anybody asks this endpoint.
    */
  given Encoder[ConsumerStatus] = status =>
    Json.obj(
      "state" -> Json.fromString(status.state.name),
      "consuming" -> Json.fromBoolean(status.consuming),
      "since" -> Json.fromString(status.since.toString),
      "generation" -> Json.fromInt(status.generation),
      "restarts" -> Json.fromInt(status.restarts),
      "groupId" -> Json.fromString(status.groupId),
      "topic" -> Json.fromString(status.topic),
      "totalLag" -> status.totalLag.fold(Json.Null)(Json.fromLong),
      "lastError" -> status.lastError.fold(Json.Null)(Json.fromString),
      "positions" -> Json.arr(status.positions.map(Encoder[PartitionPosition].apply)*)
    )

/** The outcome of a lifecycle command.
  *
  * Carries the state *before* as well as after, because "pause" applied to an already-paused consumer and "pause"
  * applied to a running one are both successes and an operator needs to know which one happened — particularly when
  * the command was issued twice because the first response was slow.
  */
final case class LifecycleResult(command: String, from: RunState, status: ConsumerStatus, changed: Boolean)

object LifecycleResult:

  given Encoder[LifecycleResult] = result =>
    Json.obj(
      "command" -> Json.fromString(result.command),
      "changed" -> Json.fromBoolean(result.changed),
      "from" -> Json.fromString(result.from.name),
      "status" -> Encoder[ConsumerStatus].apply(result.status)
    )

/** An explicit seek, as an operator supplies it: `topic/partition/offset`.
  *
  * Named `SeekOffset` and not `OffsetSpec` because Kafka's admin API already owns that name in this package
  * (`org.apache.kafka.clients.admin.OffsetSpec`, used by `AdminOffsets`), and two `OffsetSpec`s one import apart is a
  * compile error at best and the wrong type at worst.
  *
  * Parsed rather than taken as JSON so the same value works as a query parameter and in a request body, and so a
  * malformed one is refused with a sentence naming the expected shape instead of a decoder's field path.
  */
final case class SeekOffset(topic: String, partition: Int, offset: Long)

object SeekOffset:

  /** Parses `topic/partition/offset`. Total, and specific about what went wrong.
    *
    * A topic name may not contain `/`, which is what makes splitting from the right unnecessary and the grammar
    * unambiguous. Kafka's own topic-name validation forbids it.
    */
  def parse(raw: String): Either[String, SeekOffset] =
    raw.trim.split('/').toList match
      case topic :: partition :: offset :: Nil =>
        for
          p <- partition.toIntOption.toRight(s"'$partition' is not a partition number in '$raw'")
          o <- offset.toLongOption.toRight(s"'$offset' is not an offset in '$raw'")
          _ <- Either.cond(topic.nonEmpty, (), s"'$raw' has an empty topic")
          _ <- Either.cond(p >= 0, (), s"partition $p is negative in '$raw'")
          _ <- Either.cond(o >= 0, (), s"offset $o is negative in '$raw'")
        yield SeekOffset(topic, p, o)
      case _ =>
        Left(s"'$raw' is not 'topic/partition/offset'")

  /** Parses a comma-separated list, reporting **every** malformed entry rather than the first.
    *
    * An operator pasting eight coordinates during an incident should be told about all eight mistakes at once; failing
    * on the first turns one round trip into eight.
    */
  def parseAll(raw: String): Either[String, Vector[SeekOffset]] =
    val entries = raw.split(',').map(_.trim).filter(_.nonEmpty).toVector
    if entries.isEmpty then Left("no offsets given; expected 'topic/partition/offset,…'")
    else
      val (problems, parsed) = entries.map(parse).partitionMap(identity)
      if problems.nonEmpty then Left(problems.mkString("; ")) else Right(parsed)
