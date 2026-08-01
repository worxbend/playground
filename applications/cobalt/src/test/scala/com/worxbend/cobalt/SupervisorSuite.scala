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

import munit.FunSuite

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

  // --- what a batch checkpoints -------------------------------------------------------------------------------

  test("a batch checkpoints the highest offset per partition, plus one"):
    // Plus one is Kafka's commit convention — the offset of the *next* record. Storing the last processed offset
    // reads identically and is off by one at every seek.
    val records = Vector(
      Fixtures.pendingWrite("a", partition = 0, offset = 4L),
      Fixtures.pendingWrite("b", partition = 0, offset = 9L),
      Fixtures.pendingWrite("c", partition = 3, offset = 2L)
    )
    val commit = BatchProcessor.Checkpointing("g", Some("replica-1")).commit(records)
    assertEquals(commit.groupId, "g")
    assertEquals(commit.owner, Some("replica-1"))
    val byPartition = commit.positions.map(p => p.partition -> p.nextOffset).toMap
    assertEquals(byPartition, Map(0 -> 10L, 3 -> 3L))

  test("the highest offset wins, not the last one in the batch"):
    // `groupedWithin` assembles a batch across partitions and does not order it by offset within one, so taking the
    // final element would checkpoint whichever record happened to arrive last — and rewind the position if that
    // record was an earlier offset.
    val outOfOrder = Vector(
      Fixtures.pendingWrite("a", partition = 0, offset = 9L),
      Fixtures.pendingWrite("b", partition = 0, offset = 4L)
    )
    val commit = BatchProcessor.Checkpointing("g", None).commit(outOfOrder)
    assertEquals(commit.positions.map(_.nextOffset), Vector(10L))

  test("records counts the batch's own contribution, which the store then accumulates"):
    val records = Vector(
      Fixtures.pendingWrite("a", partition = 0, offset = 1L),
      Fixtures.pendingWrite("b", partition = 0, offset = 2L)
    )
    assertEquals(BatchProcessor.Checkpointing("g", None).commit(records).positions.map(_.records), Vector(2L))
