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
import com.worxbend.eventing.DecodeFailure
import com.worxbend.observability.Meters
import io.circe.Json
import io.circe.parser
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import scala.concurrent.duration.DurationInt

/** The dead-letter endpoints, asserted as status codes and bodies without a socket or a broker.
  *
  * The interesting cases are all failure cases — over-limit, refused, disabled, half-published, broker down — because a
  * replay tool is used at most a handful of times a year and every one of those times is an incident. A path that has
  * only ever been exercised by its happy case is a path nobody has tested.
  */
final class DeadLetterAdminSuite extends munit.FunSuite:

  private val config: ReplayConfig = ReplayConfig(enabled = true, maxRecords = 10, maxAttempts = 3, 1.second)

  private def admin(
    store: DeadLetterStore,
    registry: MeterRegistry,
    settings: ReplayConfig = config
  ): DeadLetterAdmin =
    DeadLetterAdmin(store, ReplayMetrics(registry), settings, Fixtures.Topic, "dlq")

  private def json(reply: AdminReply): io.circe.HCursor =
    parser.parse(reply.body).fold(error => fail(s"the reply is not JSON: $error"), _.hcursor)

  private def counter(registry: MeterRegistry, name: String, outcome: String): Double =
    Option(registry.find(name).tags(Tags.of(Meters.TagKeys.Outcome, outcome)).counter())
      .map(_.count())
      .getOrElse(0.0d)

  private def letter(id: String, offset: Long) =
    Fixtures.deadLetter(
      Fixtures.record(Fixtures.envelope(id), offset = offset),
      DecodeFailure.Unconvertible("the database refused it")
    )

  private def dlq(id: String, offset: Long, timestamp: Long): DlqRecord =
    Fixtures.dlqRecord(letter(id, offset), offset = offset, timestamp = timestamp)

  // --- inspection ---------------------------------------------------------------------------------------------

  test("the summary reports the depth as a sum and says it is an upper bound"):
    // An exact count is not available from a keyed topic that may be compacted, and reporting one would be the more
    // comfortable lie. What an operator needs is "empty, a handful, or a flood".
    val store = Fixtures.StubDeadLetterStore(
      partitions = Vector(DlqPartitionDepth(0, 10L, 14L), DlqPartitionDepth(1, 0L, 3L))
    )
    val reply = admin(store, SimpleMeterRegistry()).summary()
    assertEquals(reply.status, 200)
    val body = json(reply)
    assertEquals(body.get[Long]("outstanding").toOption, Some(7L))
    assertEquals(body.get[Boolean]("outstandingIsUpperBound").toOption, Some(true))
    assertEquals(body.downField("replay").get[Int]("maxRecords").toOption, Some(10))

  test("the summary answers 503 when the broker is unreachable, not 500"):
    // 500 says this endpoint is broken and there is nothing to do; 503 says the dependency is down and the same
    // request will work when it is back. On an admin surface consulted during an incident that is most of the value.
    val reply = admin(Fixtures.StubDeadLetterStore(unreachable = true), SimpleMeterRegistry()).summary()
    assertEquals(reply.status, 503)
    assert(json(reply).get[String]("error").toOption.exists(_.contains("unreachable")))

  test("the listing is bounded by the configured ceiling, and the bound reaches the store"):
    val store = Fixtures.StubDeadLetterStore(records = (1 to 20).toVector.map(i => dlq(s"e$i", i.toLong, i.toLong)))
    val reply = admin(store, SimpleMeterRegistry()).records(5, "")
    assertEquals(reply.status, 200)
    assertEquals(json(reply).get[Int]("returned").toOption, Some(5))
    assertEquals(store.limits.peek(), 5, "an unbounded fetch is how the listing endpoint OOMs the service")

  test("a listing over the ceiling is refused rather than trimmed"):
    val reply = admin(Fixtures.StubDeadLetterStore(), SimpleMeterRegistry()).records(11, "")
    assertEquals(reply.status, 400)
    assert(json(reply).get[String]("error").toOption.exists(_.contains("10")))

  test("the listing filters by reason and reports both counts"):
    val store = Fixtures.StubDeadLetterStore(records =
      Vector(
        dlq("a", 1L, 1L),
        Fixtures.dlqRecord(
          Fixtures.deadLetter(Fixtures.record(Fixtures.envelope("b"), offset = 2L)),
          offset = 2L,
          timestamp = 2L
        )
      )
    )
    val body = json(admin(store, SimpleMeterRegistry()).records(10, "unconvertible"))
    assertEquals(body.get[Int]("fetched").toOption, Some(2))
    assertEquals(body.get[Int]("returned").toOption, Some(1))

  // --- the dry-run / commit split -----------------------------------------------------------------------------

  test("a dry run publishes nothing, shows the whole plan, and counts as an operation that did not act"):
    val store = Fixtures.StubDeadLetterStore(records = Vector(dlq("a", 1L, 1L), dlq("b", 2L, 2L)))
    val registry = SimpleMeterRegistry()
    val reply = admin(store, registry).replay(10, "", "", dryRun = true)
    assertEquals(reply.status, 200)
    val body = json(reply)
    assertEquals(body.get[Boolean]("dryRun").toOption, Some(true))
    assertEquals(body.get[Boolean]("committed").toOption, Some(false))
    assertEquals(body.get[Int]("replayable").toOption, Some(2))
    assertEquals(body.get[Int]("published").toOption, Some(0))
    assertEquals(store.published, Vector.empty)
    assertEquals(counter(registry, Meters.DlqReplayOperations, Meters.Outcomes.Skipped), 1.0d)
    assertEquals(counter(registry, Meters.DlqReplayOperations, Meters.Outcomes.Success), 0.0d)

  test("the plan a dry run shows is the plan a commit executes"):
    // The dry run is only worth running if it is the same computation. Asserting the two plans field for field is what
    // stops them drifting into "approximately what would happen".
    val store = Fixtures.StubDeadLetterStore(records = Vector(dlq("a", 1L, 1L), dlq("b", 2L, 2L)))
    val planned = json(admin(store, SimpleMeterRegistry()).replay(10, "", "", dryRun = true))
    val committed = json(admin(store, SimpleMeterRegistry()).replay(10, "", "", dryRun = false))
    assertEquals(planned.get[Json]("plan").toOption, committed.get[Json]("plan").toOption)

  test("a commit publishes every replayable record, oldest first, and counts them"):
    val store = Fixtures.StubDeadLetterStore(records = Vector(dlq("c", 3L, 300L), dlq("a", 1L, 100L)))
    val registry = SimpleMeterRegistry()
    val reply = admin(store, registry).replay(10, "", "", dryRun = false)
    assertEquals(reply.status, 200)
    assertEquals(json(reply).get[Int]("published").toOption, Some(2))
    assertEquals(store.published.map(_.topic).distinct, Vector(Fixtures.Topic))
    assertEquals(
      store.published.map(record => CloudEventHeaders.get(record.headers, CloudEventHeaders.attribute("id"))),
      Vector(Some("a"), Some("c")),
      "the oldest dead letter must reach the topic first"
    )
    assertEquals(counter(registry, Meters.DlqReplayRecords, Meters.Outcomes.Success), 2.0d)
    assertEquals(counter(registry, Meters.DlqReplayOperations, Meters.Outcomes.Success), 1.0d)

  test("a skipped record is reported with its reason and counted, and does not stop the rest"):
    val store = Fixtures.StubDeadLetterStore(records = Vector(Fixtures.unreadableDlqRecord(offset = 9L), dlq("a", 1L, 1L)))
    val registry = SimpleMeterRegistry()
    val body = json(admin(store, registry).replay(10, "", "", dryRun = false))
    assertEquals(body.get[Int]("published").toOption, Some(1))
    assertEquals(body.downField("skipped").downArray.get[String]("reason").toOption, Some("undecodable"))
    assertEquals(counter(registry, Meters.DlqReplayRecords, Meters.Outcomes.Skipped), 1.0d)

  // --- refusing rather than half-succeeding -------------------------------------------------------------------

  test("a named set with one absent ref publishes nothing at all"):
    val present = dlq("a", 1L, 1L)
    val store = Fixtures.StubDeadLetterStore(records = Vector(present))
    val registry = SimpleMeterRegistry()
    val reply = admin(store, registry).replay(10, "", s"${present.ref},gone/0/0", dryRun = false)
    assertEquals(reply.status, 422)
    assertEquals(store.published, Vector.empty, "publishing the found half is the half-success this endpoint refuses")
    assertEquals(json(reply).downField("notFound").downArray.as[String].toOption, Some("gone/0/0"))
    assertEquals(counter(registry, Meters.DlqReplayOperations, Meters.Outcomes.Failure), 1.0d)

  test("a produce that the broker refuses stops the replay and reports the exact boundary"):
    // Retrying from here is safe precisely because the insert is idempotent, so the response has to say how far it got
    // rather than how many failed.
    val store = Fixtures.StubDeadLetterStore(
      records = Vector(dlq("a", 1L, 100L), dlq("b", 2L, 200L), dlq("c", 3L, 300L)),
      failFrom = 1
    )
    val registry = SimpleMeterRegistry()
    val reply = admin(store, registry).replay(10, "", "", dryRun = false)
    assertEquals(reply.status, 500)
    val body = json(reply)
    assertEquals(body.get[Int]("published").toOption, Some(1))
    assertEquals(body.downField("failure").get[String]("ref").toOption.map(_.endsWith("/2")), Some(true))
    assertEquals(store.published.size, 1, "nothing may be produced after the broker has refused")
    assertEquals(counter(registry, Meters.DlqReplayRecords, Meters.Outcomes.Failure), 1.0d)
    assertEquals(counter(registry, Meters.DlqReplayOperations, Meters.Outcomes.Failure), 1.0d)

  test("an over-limit replay is a 400 and never reaches the broker"):
    val store = Fixtures.StubDeadLetterStore(records = Vector(dlq("a", 1L, 1L)))
    val reply = admin(store, SimpleMeterRegistry()).replay(999, "", "", dryRun = false)
    assertEquals(reply.status, 400)
    assertEquals(store.limits.size, 0, "a request that will be refused must not cost a fetch")

  // --- the off switch ------------------------------------------------------------------------------------------

  test("with replay disabled a commit is refused but a dry run still works"):
    // Switching replay off must not also blind the operator; that would only send them back to the console consumer
    // this surface exists to replace.
    val store = Fixtures.StubDeadLetterStore(records = Vector(dlq("a", 1L, 1L)))
    val disabled = admin(store, SimpleMeterRegistry(), config.copy(enabled = false))
    assertEquals(disabled.replay(10, "", "", dryRun = false).status, 403)
    assertEquals(store.published, Vector.empty)
    assertEquals(disabled.replay(10, "", "", dryRun = true).status, 200)

  test("a broker that cannot be read is 503 on the replay path too"):
    val reply =
      admin(Fixtures.StubDeadLetterStore(unreachable = true), SimpleMeterRegistry()).replay(10, "", "", dryRun = true)
    assertEquals(reply.status, 503)
