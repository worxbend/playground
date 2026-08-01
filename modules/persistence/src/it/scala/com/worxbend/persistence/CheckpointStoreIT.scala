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

package com.worxbend.persistence

import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.persistence.repository.CheckpointCommit
import com.worxbend.persistence.repository.CheckpointWrite
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.PostgresCheckpointStore
import com.worxbend.persistence.repository.PostgresEventRepository
import io.circe.Json
import java.time.OffsetDateTime
import scala.util.Using

/** The externalised offset store, against a real schema.
  *
  * **The property worth an integration test is atomicity**, and it is not observable in a unit test: the whole reason
  * this table exists rather than a Redis key is that the offset and the rows it accounts for commit together. A test
  * that wrote them through two calls would pass against any implementation, including the broken one.
  */
final class CheckpointStoreIT extends PostgresSuite:

  private lazy val repository = PostgresEventRepository(database.read.transactor, database.write.transactor)
  private lazy val store = PostgresCheckpointStore(database.read.transactor, database.write.transactor)

  private val group = "checkpoint-it"
  private val topic = "events.cloudevents.v1"

  override def beforeAll(): Unit =
    super.beforeAll()
    truncateEvents()
    val _ = await(store.clear(group))

  private def force[A](result: Either[String, A]): A = result.fold(message => fail(message), identity)

  private def event(id: String, at: OffsetDateTime): NewEvent =
    force(
      NewEvent.from(
        Envelope(
          id = force(EventId(id)),
          source = force(Source("/gateways/checkpoint")),
          eventType = force(EventType("com.worxbend.iot.telemetry")),
          time = Some(at),
          subject = None,
          dataContentType = None,
          schema = None,
          extensions = Map.empty,
          payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("cp-1")))
        )
      )
    )

  private def positions(offsets: (Int, Long)*): Vector[CheckpointWrite] =
    offsets.toVector.map((partition, next) => CheckpointWrite(topic, partition, next, 1L))

  private def stored: Map[Int, Long] =
    await(store.load(group)).map(row => row.partition -> row.nextOffset).toMap

  /** How many rows carry this CloudEvents id. The other half of the atomicity assertion: a checkpoint that rolled back
    * while its rows stayed would be just as broken as the reverse, and only this catches it.
    */
  private def countOf(id: String): Long =
    withConnection: connection =>
      Using.resource(connection.prepareStatement("SELECT count(*) FROM events.cloud_event WHERE ce_id = ?")):
        statement =>
          statement.setString(1, id)
          Using.resource(statement.executeQuery())(rs => if rs.next() then rs.getLong(1) else -1L)

  test("an insert and its checkpoint land together"):
    val at = OffsetDateTime.parse("2026-08-01T00:00:00Z")
    val written = await(repository.insertAllCheckpointed(
      Vector(event("cp-a", at)),
      CheckpointCommit(group, Some("r1"), positions(0 -> 10L))
    ))
    assertEquals(written, 1L)
    assertEquals(stored.get(0), Some(10L))

  test("an empty batch still checkpoints"):
    // A poll that yielded only duplicates, or only dead letters, has still moved the consumer forward. Not recording
    // that leaves a position the next start would rewind to, and the rewind replays events that were never a problem.
    val _ =
      await(repository.insertAllCheckpointed(Vector.empty, CheckpointCommit(group, Some("r1"), positions(1 -> 5L))))
    assertEquals(stored.get(1), Some(5L))

  test("a failed checkpoint rolls the insert back with it — the whole point of the table"):
    val at = OffsetDateTime.parse("2026-08-01T01:00:00Z")
    val before = stored.get(0)
    // The failure is forced from the checkpoint side rather than the insert side, because the insert side cannot be
    // made to fail cheaply: the fact table has a DEFAULT partition, so even an absurd `occurred_at` is accepted, and
    // a duplicate id is absorbed by `ON CONFLICT DO NOTHING`. A negative offset violates the table's own CHECK, which
    // is a constraint the schema states and this test therefore also covers.
    val doomed = event("cp-doomed", at)
    val outcome = scala.util.Try(
      await(repository.insertAllCheckpointed(Vector(doomed), CheckpointCommit(group, Some("r1"), positions(0 -> -1L))))
    )
    assert(outcome.isFailure, "a negative offset should have been refused by the CHECK constraint")
    assertEquals(stored.get(0), before, "the checkpoint moved despite the transaction failing")
    assertEquals(countOf("cp-doomed"), 0L, "the event was written even though its checkpoint failed — not atomic")
    // And the good path still works afterwards, so the failure did not poison the pool.
    val _ = await(
      repository.insertAllCheckpointed(
        Vector(event("cp-b", at)),
        CheckpointCommit(group, Some("r1"), positions(0 -> 11L))
      )
    )
    assertEquals(stored.get(0), Some(11L))
    assertEquals(countOf("cp-b"), 1L)

  test("a stale write cannot rewind a position"):
    // A rebalance can hand the same partition to another replica mid-flight. Without the monotonicity guard the older
    // position would overwrite the newer one and manufacture a replay — enforced by the database, not by call order.
    val _ =
      await(repository.insertAllCheckpointed(Vector.empty, CheckpointCommit(group, Some("r2"), positions(0 -> 5L))))
    assertEquals(stored.get(0), Some(11L))

  test("records accumulate rather than being overwritten"):
    val counted = await(store.load(group)).find(_.partition == 0).map(_.records).getOrElse(0L)
    assert(counted >= 2L, s"expected the counter to have accumulated across writes, got $counted")

  test("clearing forgets the group and reports how much it forgot"):
    val cleared = await(store.clear(group))
    assert(cleared >= 1, s"expected to clear at least one checkpoint, cleared $cleared")
    assertEquals(await(store.load(group)), Vector.empty)
    assertEquals(await(store.clear(group)), 0, "clearing an empty group is zero, not an error")

  test("the migration created the table with its primary key"):
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        Using.resource(
          statement.executeQuery(
            "SELECT count(*) FROM pg_constraint WHERE conname = 'consumer_checkpoint_pkey'"
          )
        ): rs =>
          assert(rs.next() && rs.getInt(1) == 1, "consumer_checkpoint has no primary key")
