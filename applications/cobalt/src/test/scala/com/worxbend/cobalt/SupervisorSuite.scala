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

import com.worxbend.kernel.event.Topics
import java.util.concurrent.TimeoutException
import munit.FunSuite
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** The lifecycle state machine and the operator input that drives it.
  *
  * Driven against a handle that needs no broker, which is the only way these transitions get covered at all: the
  * interesting ones are *failure* and *repeated commands*, and neither is reachable through a real Kafka consumer in a
  * fast test. `DlqReplayIT` and `AdminServerIT` cover the wiring against a real broker; this covers the decisions.
  */
final class SupervisorSuite extends FunSuite:

  // --- operator input: the seek grammar ---------------------------------------------------------------------------

  test("a seek coordinate parses into its three parts"):
    assertEquals(SeekOffset.parse("events.v1/3/912"), Right(SeekOffset("events.v1", 3, 912L)))

  test("every malformed coordinate is reported, not just the first"):
    // An operator pasting eight coordinates during an incident should be told about all eight mistakes at once.
    // Failing on the first turns one round trip into eight.
    val problem = SeekOffset.parseAll("good/1/2, bad, worse/x/3, also/4/nope").swap.getOrElse(fail("expected errors"))
    assert(problem.contains("bad"), problem)
    assert(problem.contains("worse/x/3"), problem)
    assert(problem.contains("also/4/nope"), problem)

  test("a negative partition or offset is refused"):
    assert(SeekOffset.parse("t/-1/0").isLeft)
    assert(SeekOffset.parse("t/0/-5").isLeft)

  test("an empty list is a refusal, not an empty seek"):
    // Silently seeking nothing would report success for a restart that moved no offsets, which is the one outcome an
    // operator issuing an explicit seek must not be told.
    assert(SeekOffset.parseAll("   ").isLeft)

  test("whitespace around entries is tolerated, because this is pasted by hand"):
    assertEquals(
      SeekOffset.parseAll(" a/0/1 , b/1/2 "),
      Right(Vector(SeekOffset("a", 0, 1L), SeekOffset("b", 1, 2L)))
    )

  // --- the state vocabulary ---------------------------------------------------------------------------------------

  test("every state names whether it is consuming, and only the consuming ones say so"):
    assertEquals(RunState.values.filter(_.consuming).toSet, Set(RunState.Starting, RunState.Running))

  test("stopped and failed are different states, because only one of them is somebody's fault"):
    // A supervisor that reported a crashed stream as `stopped` would look exactly like one an operator had paused on
    // purpose, and lag would grow while the dashboard said everything was fine.
    assertNotEquals(RunState.Stopped, RunState.Failed)
    assert(!RunState.Stopped.consuming && !RunState.Failed.consuming)

  test("state and target names round-trip through their wire form"):
    RunState.values.foreach(state => assertEquals(RunState.parse(state.name), Some(state)))
    SeekTarget.values.foreach(target => assertEquals(SeekTarget.parse(target.name), Some(target)))

  test("parsing is case-insensitive and trims, because these arrive as query parameters"):
    assertEquals(SeekTarget.parse("  EARLIEST "), Some(SeekTarget.Earliest))
    assertEquals(RunState.parse("Paused"), Some(RunState.Paused))

  test("an unknown name is None rather than a default"):
    // Defaulting an unrecognised target to `committed` would turn a typo into a silently different operation.
    assertEquals(SeekTarget.parse("yesterday"), None)
    assertEquals(RunState.parse(""), None)

  // --- what the wire looks like -----------------------------------------------------------------------------------

  test("status renders state first and lag as null when it is unknown"):
    val status = ConsumerStatus(
      state = RunState.Paused,
      since = java.time.Instant.parse("2026-08-01T00:00:00Z"),
      generation = 2,
      groupId = "g",
      topic = "t",
      consuming = false,
      lastError = None,
      restarts = 1,
      positions = List(PartitionPosition("t", 0, Some(10L), Some(12L), None, None)),
      totalLag = None
    )
    val json = io.circe.Encoder[ConsumerStatus].apply(status)
    assertEquals(json.hcursor.get[String]("state").toOption, Some("paused"))
    assertEquals(json.hcursor.get[Boolean]("consuming").toOption, Some(false))
    // Unknown lag must be null and never zero: a partition with no committed offset has *unknown* lag, and reporting
    // zero is how a stalled group looks healthy.
    assert(json.hcursor.downField("totalLag").focus.exists(_.isNull))
    val position = json.hcursor.downField("positions").downArray
    assertEquals(position.get[Long]("committed").toOption, Some(10L))
    assertEquals(position.get[Long]("stored").toOption, Some(12L))
    assert(position.downField("lag").focus.exists(_.isNull))

  test("a lifecycle result reports the state on both sides of the command"):
    // "pause" on an already-paused consumer and "pause" on a running one are both successes, and an operator who
    // issued the command twice because the first response was slow needs to know which one happened.
    val status = ConsumerStatus(
      RunState.Paused,
      java.time.Instant.EPOCH,
      1,
      "g",
      "t",
      consuming = false,
      None,
      0,
      Nil,
      None
    )
    val json = io.circe.Encoder[LifecycleResult].apply(LifecycleResult("pause", RunState.Running, status, true))
    assertEquals(json.hcursor.get[String]("from").toOption, Some("running"))
    assertEquals(json.hcursor.get[Boolean]("changed").toOption, Some(true))
    assertEquals(json.hcursor.downField("status").get[String]("state").toOption, Some("paused"))

  test("the stored position appears beside the committed one, which is the whole diagnostic"):
    // A disagreement between the two means one of the commits did not happen, and which one tells an operator
    // whether events will be replayed or were lost. Two endpoints would make that comparison an exercise.
    val position = PartitionPosition(
      "t",
      0,
      committed = Some(100L),
      stored = Some(140L),
      endOffset = Some(200L),
      lag = Some(100L)
    )
    val json = io.circe.Encoder[PartitionPosition].apply(position)
    assertEquals(json.hcursor.get[Long]("committed").toOption, Some(100L))
    assertEquals(json.hcursor.get[Long]("stored").toOption, Some(140L))
    assertEquals(json.hcursor.get[Long]("lag").toOption, Some(100L))

  // --- the lifecycle, driven against a scripted handle --------------------------------------------------------

  /** Inline, so every transition and every completion callback runs on the test's own thread and the assertions need no
    * polling. The supervisor performs its transition synchronously inside the lock before it returns a future, so
    * `state` is safe to read the moment a command returns.
    */
  private given ExecutionContext = ExecutionContext.parasitic

  private def statusOf(supervisor: ConsumerSupervisor): ConsumerStatus = Await.result(supervisor.status, 10.seconds)

  test("a stream abandoned by a drain that did not finish cannot fail the consumer that replaced it"):
    // The sequence: `pause` drains, the drain does not finish cleanly, so the supervisor logs it, clears the handle
    // and reports `paused` while the old stream is still alive. `resume` materialises generation 2. The abandoned
    // stream finally dies and its callback is still armed — and without a guard it stamps `failed` and a restart over
    // a consumer that is committing normally. /admin/consumer and consume_running then report an outage that only
    // another restart can clear, while lag is shrinking.
    val abandoned = Fixtures.ScriptedHandle(() => Future.failed(TimeoutException("the drain did not finish")))
    val live = Fixtures.ScriptedHandle()
    val supervisor = Fixtures.supervisorOver(Fixtures.factoryOf(abandoned, live))

    val _ = supervisor.start()
    val _ = supervisor.pause()
    assertEquals(supervisor.state, RunState.Paused)
    val _ = supervisor.resume()
    assertEquals(supervisor.state, RunState.Running)

    abandoned.die(RuntimeException("the abandoned stream finally died"))

    val status = statusOf(supervisor)
    assertEquals(status.state, RunState.Running, "a superseded generation must not be able to fail the live one")
    assertEquals(status.generation, 2)
    assertEquals(status.restarts, 0)
    assertEquals(status.lastError, None)

  test("the live stream's own failure is still a failure"):
    // The guard must be a guard and not a mute button. When the restart policy gives up on the stream the supervisor
    // is actually running, `failed` with the cause is the honest state and the entire reason the callback exists.
    val handle = Fixtures.ScriptedHandle()
    val supervisor = Fixtures.supervisorOver(Fixtures.factoryOf(handle))
    val _ = supervisor.start()

    handle.die(IllegalStateException("the broker went away for good"))

    val status = statusOf(supervisor)
    assertEquals(status.state, RunState.Failed)
    assertEquals(status.restarts, 1)
    assert(status.lastError.exists(_.contains("the broker went away for good")), status.lastError)

  test("an explicit seek naming a topic this consumer does not own is refused before anything stops"):
    // Kafka alters offsets for any topic that exists, so without this the operator gets 200 and "committed": true
    // while the group's offsets for its own topic are untouched — the consumer resumes exactly where it was, onto the
    // poison record the seek was meant to skip, and the pipeline wedges again while the seek is believed to have
    // worked. `ReplaySkip.ForeignTopic` is the same check on the DLQ's side; this path shipped without it.
    val supervisor = Fixtures.supervisorOver(Fixtures.factoryOf(Fixtures.ScriptedHandle()))
    val _ = supervisor.start()

    val refused = Await.result(
      supervisor.restart(SeekTarget.Explicit, Vector(SeekOffset(Topics.CloudEventsDlq, 0, 5000L))),
      10.seconds
    )

    assert(refused.swap.exists(_.contains(ReplaySkip.ForeignTopic.tag)), refused)
    assert(refused.swap.exists(_.contains(Topics.CloudEventsDlq)), refused)
    assertEquals(supervisor.state, RunState.Running, "a coordinate this consumer does not own must not cost a drain")

  test("the status budget contains the calls it wraps, and still fits inside a poller tick"):
    // Two properties, and the defect was that they cannot both be satisfied by the same arithmetic if the reads run
    // one after the other. The budget must exceed what `status` can take, or the probe's await expires every tick
    // under a slow broker and the supervisor gauges freeze — a paused consumer looking exactly like a crashed one on
    // the dashboard, precisely when somebody is watching. And it must not exceed the interval between ticks, or a slow
    // tick overlaps the next. Summing a 10s checkpoint read onto 3x5s of broker calls gave 25s against a 20s
    // interval; running the two concurrently makes the honest number the larger, not the total.
    val requestTimeout = 5.seconds
    val budget = ConsumerSupervisor.statusBudget(requestTimeout)

    assert(budget >= requestTimeout * 3, s"a budget that cannot contain three sequential admin calls: $budget")
    assert(budget >= ConsumerSupervisor.StoreTimeout, s"a budget that cannot contain the checkpoint read: $budget")
    assertEquals(budget, 15.seconds)
    // The default `lag.refresh-interval`, which is what the probe ticks on.
    assert(budget < 20.seconds, s"a tick that can outlast the gap to the next tick: $budget")

  test("a dry run refuses the coordinate the commit would refuse"):
    // `dryRun` defaults to true so an operator checks before a seek costs a rebalance. That is worth nothing if the
    // check approves what the real call rejects: the guard used to live only in `resolve`, which a dry run never
    // reaches — it previews from the status it already holds — so `dryRun=true` echoed a foreign coordinate under
    // `wouldSeek` and the operator learned the answer only from the call that was supposed to be safe. Worse than a
    // missing preview, because it teaches them to skip the preview.
    val telemetry = Fixtures.telemetry()
    try
      val admin = SupervisorAdmin(Fixtures.idleSupervisor, 5.seconds, SupervisorMetrics(telemetry.registry))
      val foreign = s"${Topics.CloudEventsDlq}/0/5000"

      val previewed = admin.restart(SeekTarget.Explicit.name, foreign, dryRun = true)
      val committed = admin.restart(SeekTarget.Explicit.name, foreign, dryRun = false)

      // Both refuse, both name the same rule, and neither reports a position it would move to.
      assertEquals(previewed.status, committed.status, "a preview that disagrees with the commit is the whole defect")
      assert(previewed.body.contains(ReplaySkip.ForeignTopic.tag), previewed.body)
      assert(committed.body.contains(ReplaySkip.ForeignTopic.tag), committed.body)
      assert(!previewed.body.contains("wouldSeek"), previewed.body)

      // And the consumer's own topic still gets a preview, so the guard refuses only what it is for.
      val own = admin.restart(SeekTarget.Explicit.name, s"${Fixtures.Topic}/0/7", dryRun = true)
      assert(!own.body.contains(ReplaySkip.ForeignTopic.tag), own.body)
    finally telemetry.close()

  test("an explicit seek on the consumer's own topic still reaches the broker"):
    // The other half of the guard: it must refuse the foreign coordinate and only the foreign coordinate. Here the
    // resolution succeeds, the consumer is drained, and the failure that comes back is the unreachable broker's.
    val supervisor = Fixtures.supervisorOver(Fixtures.factoryOf(Fixtures.ScriptedHandle()))
    val _ = supervisor.start()

    val own = Vector(SeekOffset(Fixtures.Topic, 0, 7L))
    val attempted = Await.result(supervisor.restart(SeekTarget.Explicit, own), 10.seconds)

    assert(attempted.swap.exists(_.startsWith("could not move offsets")), attempted)
    assert(!supervisor.state.consuming, "the seek got as far as draining, which is what makes an alter take effect")

  // --- what a batch checkpoints -------------------------------------------------------------------------------

  private def accounted(coordinates: (Int, Long)*): BatchProcessor.Accounted =
    BatchProcessor.Accounted.of(coordinates.toVector.map { case (partition, offset) =>
      Fixtures.pendingWrite("e", partition, offset).record
    })

  test("a batch checkpoints the highest offset per partition, plus one"):
    // Plus one is Kafka's commit convention — the offset of the *next* record. Storing the last processed offset
    // reads identically and is off by one at every seek.
    val commit = BatchProcessor.Checkpointing("g", Some("replica-1")).commit(accounted(0 -> 4L, 0 -> 9L, 3 -> 2L))
    assertEquals(commit.groupId, "g")
    assertEquals(commit.owner, Some("replica-1"))
    val byPartition = commit.positions.map(p => p.partition -> p.nextOffset).toMap
    assertEquals(byPartition, Map(0 -> 10L, 3 -> 3L))

  test("the highest offset wins, not the last one in the batch"):
    // `groupedWithin` assembles a batch across partitions and does not order it by offset within one, so taking the
    // final element would checkpoint whichever record happened to arrive last — and rewind the position if that
    // record was an earlier offset.
    val commit = BatchProcessor.Checkpointing("g", None).commit(accounted(0 -> 9L, 0 -> 4L))
    assertEquals(commit.positions.map(_.nextOffset), Vector(10L))

  test("records counts the batch's own contribution, which the store then accumulates"):
    val commit = BatchProcessor.Checkpointing("g", None).commit(accounted(0 -> 1L, 0 -> 2L))
    assertEquals(commit.positions.map(_.records), Vector(2L))

  test("bisecting a batch keeps its offset and counts each record once"):
    // Both halves of a bisection write the same partition, and the store ADDS `records` on conflict, so the second
    // write cannot carry the whole batch's count — it would add the first half's records a second time, compounding
    // at every level of the recursion. It also cannot carry only its own half's offsets: the right half need not
    // hold the highest one, and the store's `WHERE EXCLUDED.next_offset > …` guard would then leave the stored
    // position short of what was committed to Kafka, which is the shortfall `isolate` exists to close.
    val whole = accounted(0 -> 1L, 0 -> 2L, 0 -> 3L, 0 -> 4L)
    val leftRecords = Vector(1L, 2L).map(offset => Fixtures.pendingWrite("e", 0, offset).record)

    val left = BatchProcessor.Accounted.of(leftRecords)
    val right = whole.less(leftRecords)

    // The batch's own position survives on the write that lands last...
    assertEquals(right.positions.map(_.nextOffset), Vector(5L))
    // ...and the two writes sum to the batch, not to the batch plus its left half.
    assertEquals(left.positions.map(_.records).sum + right.positions.map(_.records).sum, 4L)

  test("subtracting more than a partition contributed floors at zero rather than failing the write"):
    // `records` is `CHECK (records >= 0)`. A drifted diagnostic must not roll back the transaction that carries the
    // offset — that would trade a wrong number for a stalled consumer.
    val whole = accounted(0 -> 1L)
    val overspent = whole.less(Vector(1L, 2L, 3L).map(offset => Fixtures.pendingWrite("e", 0, offset).record))
    assertEquals(overspent.positions.map(_.records), Vector(0L))
    // A partition the earlier write never touched is untouched here too.
    val twoPartitions = accounted(0 -> 1L, 1 -> 1L)
    val onlyZeroSpent = twoPartitions.less(Vector(Fixtures.pendingWrite("e", 0, 1L).record))
    assertEquals(onlyZeroSpent.positions.map(p => p.partition -> p.records).toMap, Map(0 -> 0L, 1 -> 1L))

  // --- the supervisor's own metrics -------------------------------------------------------------------------------

  private def status(positions: List[PartitionPosition], consuming: Boolean = true): ConsumerStatus =
    ConsumerStatus(
      state = if consuming then RunState.Running else RunState.Paused,
      since = java.time.Instant.EPOCH,
      generation = 1,
      groupId = "g",
      topic = "t",
      consuming = consuming,
      lastError = None,
      restarts = 0,
      positions = positions,
      totalLag = None
    )

  test("checkpoint divergence is the largest gap between Kafka and the external store"):
    // Zero is the only healthy value: the two are written in one transaction, so a persistent gap means one of the
    // commits is failing — and which side is ahead says whether events replay or their durability is unprovable.
    val positions = List(
      PartitionPosition("t", 0, committed = Some(100L), stored = Some(100L), endOffset = Some(100L), lag = Some(0L)),
      PartitionPosition("t", 1, committed = Some(50L), stored = Some(63L), endOffset = Some(63L), lag = Some(13L)),
      PartitionPosition("t", 2, committed = Some(90L), stored = Some(88L), endOffset = Some(90L), lag = Some(0L))
    )
    assertEquals(SupervisorMetrics.divergenceOf(status(positions)), 13L)

  test("a partition with either side unknown contributes nothing, not its whole offset"):
    // Every group has committed-but-not-yet-checkpointed partitions for the first seconds after a deploy. Counting
    // those would report a divergence the size of the log and page somebody on every rollout.
    val positions = List(
      PartitionPosition("t", 0, committed = Some(9_000_000L), stored = None, endOffset = Some(9_000_000L), lag = None),
      PartitionPosition("t", 1, committed = None, stored = Some(42L), endOffset = None, lag = None)
    )
    assertEquals(SupervisorMetrics.divergenceOf(status(positions)), 0L)

  test("divergence is absolute — either side being ahead is a defect"):
    val ahead = List(PartitionPosition("t", 0, Some(10L), Some(4L), Some(10L), Some(0L)))
    val behind = List(PartitionPosition("t", 0, Some(4L), Some(10L), Some(10L), Some(6L)))
    assertEquals(SupervisorMetrics.divergenceOf(status(ahead)), 6L)
    assertEquals(SupervisorMetrics.divergenceOf(status(behind)), 6L)

  test("the running gauge is what separates a deliberate pause from an outage"):
    // Lag rising with this at 1 is a consumer that cannot keep up; at 0 it is somebody's maintenance window.
    // Alerting on lag alone cannot tell them apart, which is how a planned pause pages the on-call.
    val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
    val metrics = SupervisorMetrics(registry)
    def gauge(name: String): Double = Option(registry.find(name).gauge()).map(_.value()).getOrElse(Double.NaN)

    metrics.observe(status(Nil, consuming = true))
    assertEquals(gauge(com.worxbend.observability.Meters.ConsumeRunning), 1.0)
    metrics.observe(status(Nil, consuming = false))
    assertEquals(gauge(com.worxbend.observability.Meters.ConsumeRunning), 0.0)

  test("lifecycle commands record whether they changed anything"):
    // "pause" on an already-paused consumer is a success that changed nothing, and an operator who issued it twice
    // needs the counter to say so. `skipped` is the same convention the maintenance jobs use for a lost lock race.
    val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
    val metrics = SupervisorMetrics(registry)
    def counted(command: String, outcome: String): Double =
      Option(
        registry
          .find(com.worxbend.observability.Meters.ConsumeLifecycle)
          .tag(com.worxbend.observability.Meters.TagKeys.Command, command)
          .tag(com.worxbend.observability.Meters.TagKeys.Outcome, outcome)
          .counter()
      ).map(_.count()).getOrElse(0.0)

    metrics.command(com.worxbend.observability.Meters.Commands.Pause, changed = true)
    metrics.command(com.worxbend.observability.Meters.Commands.Pause, changed = false)
    metrics.commandFailed(com.worxbend.observability.Meters.Commands.Restart)
    assertEquals(counted(com.worxbend.observability.Meters.Commands.Pause, "success"), 1.0)
    assertEquals(counted(com.worxbend.observability.Meters.Commands.Pause, "skipped"), 1.0)
    assertEquals(counted(com.worxbend.observability.Meters.Commands.Restart, "failure"), 1.0)
