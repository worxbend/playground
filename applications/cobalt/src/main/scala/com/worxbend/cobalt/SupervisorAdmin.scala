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

import com.typesafe.scalalogging.StrictLogging
import io.circe.Encoder
import io.circe.Json
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/** The consumer lifecycle, over HTTP.
  *
  * **Why this exists.** A consumer that can only be stopped by stopping the process is a consumer whose every incident
  * response is `docker compose restart` plus a `kafka-consumer-groups.sh` invocation nobody has memorised, run inside a
  * container that has to be given a shell first. Every operation here replaces one of those.
  *
  * **Every mutating route is a POST, and one of them defaults to refusing.** `restart` moves a consumer group's
  * committed offsets, which is the single most destructive thing an operator can do to this pipeline: `latest` skips
  * unconsumed events outright. So it plans by default and commits only when told to, in the same shape the DLQ replay
  * uses — one convention for both dangerous operations rather than two to remember at 3am.
  *
  * **The blocking is deliberate and bounded.** These handlers `Await` on the supervisor. Cask serves each request on
  * its own Undertow worker thread, a lifecycle transition is inherently one-at-a-time, and an operator issuing a stop
  * wants the response to mean *stopped* — a 202 with an opaque promise would put them straight back into polling the
  * status endpoint. The timeout is what keeps a hung drain from holding the worker forever.
  */
final class SupervisorAdmin(supervisor: ConsumerSupervisor, timeout: FiniteDuration) extends StrictLogging:

  /** `GET /admin/consumer` — the state machine plus committed, stored and end offsets per partition. */
  def status(): AdminReply =
    respond(supervisor.status)(status => Encoder[ConsumerStatus].apply(status))

  /** `POST /admin/consumer:pause` — drain, commit, leave the group, and stay out. */
  def pause(): AdminReply = lifecycle(supervisor.pause())

  /** `POST /admin/consumer:resume` — materialise a fresh stream and rejoin the group. */
  def resume(): AdminReply = lifecycle(supervisor.resume())

  /** `POST /admin/consumer:stop` — the same as pause, reported as an outage rather than as intent. */
  def stop(): AdminReply = lifecycle(supervisor.stop())

  /** `POST /admin/consumer:start` */
  def start(): AdminReply = lifecycle(supervisor.start())

  /** `POST /admin/consumer:restart` — stop, move the group's offsets, start.
    *
    * `target` selects where to resume from; `offsets` supplies the coordinates when it is `explicit`. `dryRun` defaults
    * to **true**, so the first call answers "here is what I would set" and changes nothing.
    */
  def restart(target: String, offsets: String, dryRun: Boolean): AdminReply =
    val request =
      for
        seekTarget <- SeekTarget
          .parse(target)
          .toRight(s"target '$target' is not one of ${SeekTarget.values.map(_.name).mkString(", ")}")
        explicit <-
          if seekTarget == SeekTarget.Explicit then SeekOffset.parseAll(offsets)
          else if offsets.trim.nonEmpty then
            // Refused rather than ignored. An operator who pasted coordinates and chose the wrong target would
            // otherwise get a successful response that silently did something else with the pipeline.
            Left(s"offsets were supplied but target is '${seekTarget.name}'; use target=explicit or drop them")
          else Right(Vector.empty)
      yield (seekTarget, explicit)

    request match
      case Left(problem)                           => AdminRoutes.json(400, error(problem))
      case Right((seekTarget, explicit)) if dryRun =>
        // A dry run resolves the target without stopping anything, which is the whole value: an operator can see
        // what `stored` or `earliest` actually means for this group before it costs a rebalance.
        respond(supervisor.status): status =>
          Json.obj(
            "dryRun" -> Json.fromBoolean(true),
            "committed" -> Json.fromBoolean(false),
            "target" -> Json.fromString(seekTarget.name),
            "wouldSeek" -> Json.arr(preview(seekTarget, explicit, status)*),
            "status" -> Encoder[ConsumerStatus].apply(status)
          )
      case Right((seekTarget, explicit)) =>
        attempt(Await.result(supervisor.restart(seekTarget, explicit), timeout)) match
          case Left(problem)  => AdminRoutes.json(409, error(problem))
          case Right(outcome) =>
            respond(supervisor.status): status =>
              Json.obj(
                "dryRun" -> Json.fromBoolean(false),
                "committed" -> Json.fromBoolean(true),
                "target" -> Json.fromString(outcome.target.name),
                "from" -> Json.fromString(outcome.from.name),
                "seek" -> Json.arr(outcome.offsets.map(offset)*),
                "status" -> Encoder[ConsumerStatus].apply(status)
              )

  /** `POST /admin/consumer:clearCheckpoints` — forget the externalised offsets.
    *
    * **Does not touch Kafka's `__consumer_offsets`.** This is the "the stored position is wrong, use the broker's
    * answer instead" escape hatch, and conflating it with a Kafka offset reset would make one word mean two
    * irreversible things.
    */
  def clearCheckpoints(): AdminReply =
    respond(supervisor.clearCheckpoints()): cleared =>
      Json.obj(
        "cleared" -> Json.fromInt(cleared),
        "note" -> Json.fromString("Kafka's own committed offsets are untouched; only the external checkpoints are gone")
      )

  // --- plumbing ------------------------------------------------------------------------------------------------

  private def lifecycle(action: => scala.concurrent.Future[LifecycleResult]): AdminReply =
    respond(action)(result => Encoder[LifecycleResult].apply(result))

  /** Awaits a supervisor call and renders it, turning a timeout or a failure into a status code rather than a stack
    * trace. 504 for a timeout specifically: it says "the operation may still be in progress", which for a drain that
    * outran its bound is the truth.
    */
  private def respond[A](action: => scala.concurrent.Future[A])(render: A => Json): AdminReply =
    try AdminRoutes.json(200, render(Await.result(action, timeout)))
    catch
      case _: java.util.concurrent.TimeoutException =>
        AdminRoutes.json(504, error(s"the consumer did not answer within $timeout; it may still be transitioning"))
      case NonFatal(problem) =>
        logger.error("a consumer admin operation failed", problem)
        AdminRoutes.json(500, error(Option(problem.getMessage).getOrElse(problem.getClass.getName)))

  private def attempt[A](result: => Either[String, A]): Either[String, A] =
    try result
    catch case NonFatal(problem) => Left(Option(problem.getMessage).getOrElse(problem.getClass.getName))

  /** What a restart *would* seek to, computed from the status this endpoint already fetched.
    *
    * Reuses the status rather than resolving the target for real, because resolving `stored` or `earliest` means more
    * admin round trips and a dry run should be cheap enough to run twice. The values shown are the same ones the commit
    * path would use — they come from the same two sources.
    */
  private def preview(target: SeekTarget, explicit: Vector[SeekOffset], status: ConsumerStatus): List[Json] =
    target match
      case SeekTarget.Explicit  => explicit.toList.map(offset)
      case SeekTarget.Committed => Nil
      case SeekTarget.Stored    =>
        status.positions.flatMap(p => p.stored.map(o => offset(SeekOffset(p.topic, p.partition, o))))
      case SeekTarget.Latest =>
        status.positions.flatMap(p => p.endOffset.map(o => offset(SeekOffset(p.topic, p.partition, o))))
      case SeekTarget.Earliest =>
        // The status carries no log-start offsets — nothing else needs them — so a dry run reports the intent rather
        // than inventing a number. Saying "0" here would be a guess, and wrong on any partition that has aged out.
        Nil

  private def offset(spec: SeekOffset): Json =
    Json.obj(
      "topic" -> Json.fromString(spec.topic),
      "partition" -> Json.fromInt(spec.partition),
      "offset" -> Json.fromLong(spec.offset)
    )

  private def error(message: String): Json = Json.obj("error" -> Json.fromString(message))
