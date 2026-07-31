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
import com.worxbend.observability.Meters
import io.circe.Json
import scala.util.control.NonFatal

/** Where a partially-completed replay stopped. */
final case class ReplayFailure(ref: String, detail: String)

/** What a replay actually did, as distinct from what it planned to do. */
final case class ReplayOutcome(published: Vector[String], failure: Option[ReplayFailure])

/** The DLQ's operator surface: look at it, and put things back.
  *
  * **Why this exists.** Until it did, `docs/operations.md` told an operator to "re-publish the original events through
  * `POST /events` on wolfram" — an instruction that assumes the events are still somewhere other than the DLQ, that
  * their bytes can be reconstructed by hand, and that there is time to do it. Kafka retention on the DLQ topic is seven
  * days, so the real situation is a seven-day window in which the only tool is a console consumer. Every decision below
  * follows from that: an operator under time pressure needs to see what is there, see what a replay *would* do, and
  * then do it, without composing a single record by hand.
  *
  * **The shape mirrors [[AdminHandlers]]:** answers are computed here and returned as [[AdminReply]] values, so every
  * status code and every body in this file is asserted by a unit test that binds no socket and starts no broker.
  *
  * **Idempotence, stated exactly.** A replay is *not* idempotent at the broker: each one appends new records to the
  * main topic, and running the same replay twice produces two copies on the log. It *is* idempotent at the database,
  * because [[DeadLetterReplay.producerRecord]] copies the original bytes verbatim, so the CloudEvents `id` and `source`
  * survive and `ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING` absorbs the second copy — which is exactly what
  * makes retrying a replay that failed halfway a safe thing to do rather than a data-duplication risk.
  *
  * **The poison loop, stated exactly.** A record that fails again after being replayed lands back on the DLQ, and under
  * a *new* key: the origin coordinates are the coordinates of the replayed record, which is at a new offset. The DLQ's
  * compaction key therefore does nothing to stop one defect accumulating one dead letter per replay. What does stop it
  * is [[ReplayHeaders.Attempt]] — a counter carried in the record's own headers, preserved into each successive dead
  * letter because a dead letter records its record's headers verbatim, and checked by the planner against
  * `cobalt.replay.max-attempts`. The loop is bounded at that many generations, it is visible in every listing as
  * `replayAttempts`, and each generation names its predecessor through [[ReplayHeaders.Of]].
  */
final class DeadLetterAdmin(
  store: DeadLetterStore,
  metrics: ReplayMetrics,
  config: ReplayConfig,
  ownTopic: String,
  dlqTopic: String
) extends StrictLogging:

  /** How deep the DLQ is, and what the replay endpoint will and will not do.
    *
    * The limits are in the body on purpose. An operator meeting this endpoint for the first time is meeting it during
    * an incident, and "what is the largest replay I am allowed to ask for" is a question they should not have to answer
    * by reading a HOCON file on a machine they may not have open.
    */
  def summary(): AdminReply =
    guard("read the dead-letter topic"):
      val depth = store.depth()
      AdminRoutes.json(
        200,
        Json.obj(
          "topic" -> Json.fromString(depth.topic),
          "sourceTopic" -> Json.fromString(ownTopic),
          "outstanding" -> Json.fromLong(depth.outstanding),
          "outstandingIsUpperBound" -> Json.True,
          "partitions" -> Json.arr(
            depth.partitions.map { partition =>
              Json.obj(
                "partition" -> Json.fromInt(partition.partition),
                "earliest" -> Json.fromLong(partition.earliest),
                "latest" -> Json.fromLong(partition.latest),
                "records" -> Json.fromLong(partition.records)
              )
            }*
          ),
          "replay" -> Json.obj(
            "enabled" -> Json.fromBoolean(config.enabled),
            "maxRecords" -> Json.fromInt(config.maxRecords),
            "maxAttempts" -> Json.fromInt(config.maxAttempts)
          )
        )
      )

  /** A bounded, newest-first page of dead letters.
    *
    * There is no cursor and no `offset` parameter, deliberately. The DLQ is a log with a seven-day retention that is
    * being written to while it is being read; a paging cursor over it would either be an offset that means something
    * different on every partition, or a promise of stability the topic cannot keep. What an operator needs is the tail
    * and a filter, and both are here — anything deeper than `max-records` is a job for `kcat`, and the listing gives
    * the exact refs to feed it.
    */
  def records(limit: Int, reason: String): AdminReply =
    ReplayRequest.parse(limit, reason, "", dryRun = true, config.maxRecords) match
      case Left(problem)  => AdminRoutes.json(400, Json.obj("error" -> Json.fromString(problem)))
      case Right(request) =>
        guard("read the dead-letter topic"):
          val wanted = Option(reason).map(_.trim).filter(_.nonEmpty)
          val fetched = store.recent(request.fetchLimit(config.maxRecords))
          val matching = fetched.filter(record => wanted.forall(want => record.entry.exists(_.reason == want)))
          AdminRoutes.json(
            200,
            Json.obj(
              "topic" -> Json.fromString(dlqTopic),
              "limit" -> Json.fromInt(request.fetchLimit(config.maxRecords)),
              "reason" -> wanted.fold(Json.Null)(Json.fromString),
              "fetched" -> Json.fromInt(fetched.size),
              "returned" -> Json.fromInt(matching.size),
              "records" -> Json.arr(matching.map(DeadLetterReplay.render)*)
            )
          )

  /** Plans a replay, and — unless this is a dry run — commits it.
    *
    * **`dryRun` defaults to true at the route, and the default is the whole design.** The failure this endpoint can
    * cause is a produce storm at a broker that is already the reason these records died, so the shape that must take
    * one extra deliberate keystroke is the one that publishes. An operator who cannot see what a replay would do will
    * not run one during an incident; an operator who *can* runs the dry run first, reads the plan, and then adds
    * `dryRun=false`.
    *
    * **Refuse rather than half-succeed, in the two places it is achievable.** The plan is computed in full before a
    * single record is published, so a request that cannot be satisfied is rejected having published nothing —
    * unconditionally for a named set (see [[ReplayPlan.refusal]]). The produce loop itself cannot be atomic; Kafka
    * offers no transaction that would help here and using one would put the replay's fate in the same transaction
    * coordinator that may be the problem. So it is sequential, it stops at the first refusal, and it reports the exact
    * number published and the ref it stopped on. Retrying from there is safe because the insert is idempotent.
    */
  def replay(limit: Int, reason: String, refs: String, dryRun: Boolean): AdminReply =
    ReplayRequest.parse(limit, reason, refs, dryRun, config.maxRecords) match
      case Left(problem)  =>
        metrics.operation(Meters.Outcomes.Failure)
        AdminRoutes.json(400, Json.obj("error" -> Json.fromString(problem)))
      case Right(request) =>
        if !dryRun && !config.enabled then
          // A dry run is inspection and stays available: a deployment that has switched replay off still benefits from
          // being able to see what one would do, and refusing that too would only push the operator to a console
          // consumer, which is the tool this endpoint exists to replace.
          metrics.operation(Meters.Outcomes.Failure)
          AdminRoutes.json(
            403,
            Json.obj(
              "error" -> Json.fromString("replay is disabled on this deployment (cobalt.replay.enabled = false)"),
              "hint" -> Json.fromString("dryRun=true still works and shows exactly what a replay would publish")
            )
          )
        else
          guard("read the dead-letter topic"):
            val fetched = store.recent(request.fetchLimit(config.maxRecords))
            val plan = DeadLetterReplay.plan(fetched, request, ownTopic, config.maxAttempts)
            plan.refusal match
              case Some(problem) => refused(plan, problem)
              case None          => if dryRun then planned(plan) else commit(plan)

  /** A dry run: the plan, nothing published, and the operation counted as [[Meters.Outcomes.Skipped]]. */
  private def planned(plan: ReplayPlan): AdminReply =
    metrics.operation(Meters.Outcomes.Skipped)
    metrics.records(Meters.Outcomes.Skipped, plan.skips.size)
    AdminRoutes.json(200, body(plan, ReplayOutcome(Vector.empty, None)))

  /** A named set that cannot be satisfied in full. Nothing was published and nothing will be. */
  private def refused(plan: ReplayPlan, problem: String): AdminReply =
    metrics.operation(Meters.Outcomes.Failure)
    metrics.records(Meters.Outcomes.Skipped, plan.decisions.size)
    logger.warn(s"a dead-letter replay was refused: $problem")
    AdminRoutes.json(
      422,
      body(plan, ReplayOutcome(Vector.empty, None)).deepMerge(Json.obj("error" -> Json.fromString(problem)))
    )

  /** Publishes the plan, one record at a time, stopping at the first refusal.
    *
    * A `while` and not a `foldLeft` because stopping early is the point: every record after a broker refusal would be
    * another produce at a broker that has just said no, and the operator needs the boundary — "37 published, stopped at
    * this ref" — rather than a list of fifty failures.
    */
  private def commit(plan: ReplayPlan): AdminReply =
    val published = Vector.newBuilder[String]
    var count = 0
    var failure: Option[ReplayFailure] = None
    val pending = plan.replays.iterator
    while failure.isEmpty && pending.hasNext do
      val decision = pending.next()
      try
        store.publish(DeadLetterReplay.producerRecord(decision))
        val _ = published += decision.source.ref
        count += 1
      catch
        case NonFatal(error) => failure = Some(ReplayFailure(decision.source.ref, RecordDecoder.describe(error)))
    val outcome = ReplayOutcome(published.result(), failure)
    metrics.records(Meters.Outcomes.Success, count)
    metrics.records(Meters.Outcomes.Skipped, plan.skips.size)
    failure match
      case None          =>
        metrics.operation(Meters.Outcomes.Success)
        logger.info(s"replayed $count dead letter(s) onto $ownTopic; ${plan.skips.size} skipped")
        AdminRoutes.json(200, body(plan, outcome))
      case Some(problem) =>
        metrics.records(Meters.Outcomes.Failure, 1)
        metrics.operation(Meters.Outcomes.Failure)
        logger.error(
          s"a dead-letter replay stopped after $count record(s) at ${problem.ref}: ${problem.detail}"
        )
        AdminRoutes.json(500, body(plan, outcome))

  /** The response body, identical in shape for every outcome.
    *
    * One shape rather than one per status, so a script that reads `published` does not have to know which of five
    * things happened first. `committed` says whether anything was written at all, which is the field a human checks.
    */
  private def body(plan: ReplayPlan, outcome: ReplayOutcome): Json =
    Json.obj(
      "dryRun" -> Json.fromBoolean(plan.dryRun),
      "committed" -> Json.fromBoolean(outcome.published.nonEmpty),
      "topic" -> Json.fromString(ownTopic),
      "scope" -> (plan.scope match
        case ReplayScope.Recent(limit, reason) =>
          Json.obj(
            "kind" -> Json.fromString("recent"),
            "limit" -> Json.fromInt(limit),
            "reason" -> reason.fold(Json.Null)(Json.fromString)
          )
        case ReplayScope.Named(refs) =>
          Json.obj("kind" -> Json.fromString("named"), "refs" -> Json.arr(refs.map(Json.fromString)*))
      ),
      "selected" -> Json.fromInt(plan.decisions.size),
      "replayable" -> Json.fromInt(plan.replays.size),
      "published" -> Json.fromInt(outcome.published.size),
      "maxAttempts" -> Json.fromInt(config.maxAttempts),
      "plan" -> Json.arr(
        plan.replays.map { decision =>
          Json.obj(
            "ref" -> Json.fromString(decision.source.ref),
            "attempt" -> Json.fromInt(decision.attempt),
            "reason" -> Json.fromString(decision.deadLetter.reason),
            "event" -> Json.fromFields(
              DeadLetterReplay
                .identity(decision.deadLetter)
                .toVector
                .sortBy((name, _) => name)
                .map((name, value) => name -> Json.fromString(value))
            )
          )
        }*
      ),
      "skipped" -> Json.arr(
        plan.skips.map { skip =>
          Json.obj("ref" -> Json.fromString(skip.source.ref), "reason" -> Json.fromString(skip.reason.tag))
        }*
      ),
      "notFound" -> Json.arr(plan.missing.map(Json.fromString)*),
      "failure" -> outcome.failure.fold(Json.Null) { failure =>
        Json.obj("ref" -> Json.fromString(failure.ref), "detail" -> Json.fromString(failure.detail))
      }
    )

  /** Turns an unreachable broker into a 503 rather than a stack trace.
    *
    * 503 and not 500 because the distinction is the one the caller acts on: 500 says this endpoint is broken and there
    * is nothing to do but file a bug, 503 says the dependency is down and the same request will work when it is back.
    * On an admin surface consulted during an incident, telling those apart is most of the value.
    */
  private def guard(what: String)(answer: => AdminReply): AdminReply =
    try answer
    catch
      case NonFatal(error) =>
        val cause = RecordDecoder.describe(error)
        logger.warn(s"could not $what: $cause")
        AdminRoutes.json(503, Json.obj("error" -> Json.fromString(s"could not $what: $cause")))
