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

import com.worxbend.observability.Meters
import com.worxbend.observability.Telemetry
import com.worxbend.observability.Tracing
import com.worxbend.persistence.repository.NewEvent
import java.sql.SQLException
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** The batch path: what is written, what is dead-lettered, and — the interesting one — what is *not* dead-lettered.
  *
  * The poison-isolation tests are the reason this class exists. Bisecting a failed batch is only safe if the failure is
  * one the database will deterministically repeat; getting that wrong turns a five-second outage into a DLQ full of
  * perfectly good events, and it does so silently.
  */
final class BatchProcessorSuite extends munit.FunSuite:

  given ExecutionContext = ExecutionContext.global

  private val offsets = java.util.concurrent.atomic.AtomicLong(0L)

  private def decoded(record: ConsumerRecord[String, Array[Byte]]): DecodedRecord =
    RecordDecoder(Fixtures.source, Tracing.noop.tracer)
      .decode(Fixtures.committableMessage(record, Fixtures.offsetFor(0, offsets.getAndIncrement())))

  private def good(id: String): DecodedRecord = decoded(Fixtures.record(Fixtures.envelope(id)))

  /** A record whose committable offset is the record's own, so a checkpoint assertion means something. */
  private def at(id: String, partition: Int, offset: Long): DecodedRecord =
    RecordDecoder(Fixtures.source, Tracing.noop.tracer).decode(
      Fixtures.committableMessage(
        Fixtures.record(Fixtures.envelope(id), partition, offset),
        Fixtures.offsetFor(partition, offset)
      )
    )

  private def poisonAt(partition: Int, offset: Long): DecodedRecord =
    RecordDecoder(Fixtures.source, Tracing.noop.tracer).decode(
      Fixtures.committableMessage(Fixtures.malformedRecord(partition, offset), Fixtures.offsetFor(partition, offset))
    )

  private def processor(
    repository: Fixtures.RecordingRepository,
    deadLetters: Fixtures.RecordingDeadLetters,
    metrics: ConsumerMetrics,
    attempts: Int = 3
  ): BatchProcessor =
    BatchProcessor(repository, deadLetters, metrics, Fixtures.source, attempts, () => Future.unit)

  /** The same processor, externalising its offsets — which is how cobalt actually runs it. */
  private def checkpointing(
    repository: Fixtures.RecordingRepository,
    deadLetters: Fixtures.RecordingDeadLetters,
    metrics: ConsumerMetrics,
    attempts: Int = 3
  ): BatchProcessor =
    BatchProcessor(
      repository,
      deadLetters,
      metrics,
      Fixtures.source,
      attempts,
      () => Future.unit,
      checkpoint = Some(BatchProcessor.Checkpointing(Fixtures.GroupId, Some("replica-1")))
    )

  private def idOf(event: NewEvent): String = event.raw.hcursor.get[String]("id").getOrElse("?")

  /** Telemetry that outlives the `Future` under test.
    *
    * A `try`/`finally` around a body that *returns* a `Future` closes the registry before the assertions inside it ever
    * run, so the meters read back empty and the test passes or fails for reasons unrelated to the code. Closing in
    * `transform` is the only correct shape here.
    */
  private def withTelemetry[A](body: Telemetry => Future[A]): Future[A] =
    val telemetry = Fixtures.telemetry()
    body(telemetry).transform: result =>
      telemetry.close()
      result

  test("a clean batch is written once and every offset comes back"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val deadLetters = Fixtures.RecordingDeadLetters()
      val batch = Vector(good("a"), good("b"), good("c"))
      processor(repository, deadLetters, ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(committables.size, 3)
          assertEquals(repository.calls.get(), 1)
          assertEquals(repository.batches.head.map(idOf), Vector("a", "b", "c"))
          assertEquals(deadLetters.published, Vector.empty)
          assert(telemetry.scrape().contains(Meters.ConsumePersisted.replace('.', '_')))

  test("a malformed record is dead-lettered and its offset is still returned"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val deadLetters = Fixtures.RecordingDeadLetters()
      val batch = Vector(good("a"), decoded(Fixtures.malformedRecord(offset = 9L)))
      processor(repository, deadLetters, ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(committables.size, 2, "the poison record's offset must move so the stream does not wedge")
          assertEquals(deadLetters.published.map(_.origin.offset), Vector(9L))
          assertEquals(repository.batches.head.map(idOf), Vector("a"), "the poison record must not reach the database")

  test("duplicates are counted as the shortfall between the batch size and the rows written"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(_ => Future.successful(1L))
      val metrics = ConsumerMetrics(telemetry.registry)
      processor(repository, Fixtures.RecordingDeadLetters(), metrics)
        .process(Vector(good("a"), good("b"), good("c")))
        .map: _ =>
          val duplicate = telemetry.registry.find(Meters.ConsumeDuplicate).counter()
          assertEquals(Option(duplicate).map(_.count()), Some(2.0d))

  test("an empty batch touches nothing"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(_ => Future.successful(0L))
      processor(repository, Fixtures.RecordingDeadLetters(), ConsumerMetrics(telemetry.registry))
        .process(Vector.empty)
        .map: committables =>
          assertEquals(committables, Vector.empty)
          assertEquals(repository.calls.get(), 0)

  test("a transient failure is retried whole rather than bisected"):
    withTelemetry: telemetry =>
      val firstAttempt = java.util.concurrent.atomic.AtomicBoolean(true)
      val repository = Fixtures.RecordingRepository: events =>
        if firstAttempt.getAndSet(false) then Future.failed(SQLException("connection reset", "08006"))
        else Future.successful(events.size.toLong)
      val deadLetters = Fixtures.RecordingDeadLetters()
      processor(repository, deadLetters, ConsumerMetrics(telemetry.registry))
        .process(Vector(good("a"), good("b")))
        .map: _ =>
          assertEquals(repository.calls.get(), 2, "the second attempt must be the whole batch, not a half")
          assertEquals(repository.batches.map(_.size), Vector(2, 2))
          assertEquals(deadLetters.published, Vector.empty, "a transient failure must never dead-letter anything")

  test("a database that stays unreachable fails the batch instead of emptying it into the DLQ"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(_ => Future.failed(SQLException("no route to host", "08001")))
      val deadLetters = Fixtures.RecordingDeadLetters()
      processor(repository, deadLetters, ConsumerMetrics(telemetry.registry), attempts = 2)
        .process(Vector(good("a"), good("b"), good("c")))
        .failed
        .map: error =>
          assert(error.isInstanceOf[SQLException])
          assertEquals(deadLetters.published, Vector.empty, "an outage must not be mistaken for poison")

  test("one unpersistable record is isolated by bisection and the rest of the batch still lands"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository: events =>
        if events.exists(event => idOf(event) == "bad") then
          Future.failed(SQLException("no partition of relation found for row", "23514"))
        else Future.successful(events.size.toLong)
      val deadLetters = Fixtures.RecordingDeadLetters()
      val batch = Vector(good("a"), good("bad"), good("c"), good("d"))
      processor(repository, deadLetters, ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(committables.size, 4, "every offset moves: three written, one dead-lettered")
          assertEquals(deadLetters.published.size, 1)
          assertEquals(deadLetters.published.head.reason, "unconvertible")
          val landed = repository.batches.filter(batch => !batch.exists(event => idOf(event) == "bad"))
          assertEquals(landed.flatMap(_.map(idOf)).toSet, Set("a", "c", "d"))
          val poison = telemetry.registry
            .find(Meters.ConsumePoison)
            .tag(Meters.TagKeys.Reason, Meters.Reasons.Unpersistable)
            .counter()
          assertEquals(Option(poison).map(_.count()), Some(1.0d))

  test("a DLQ that refuses the record leaves the offset uncommitted"):
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      processor(repository, Fixtures.RecordingDeadLetters(fail = true), ConsumerMetrics(telemetry.registry))
        .process(Vector(decoded(Fixtures.malformedRecord())))
        .failed
        .map(error => assertEquals(error.getMessage, "the DLQ is unavailable"))

  // --- the checkpoint and the Kafka commit must describe the same position ------------------------------------------
  //
  // Every test below asserts one invariant: whatever offsets `process` hands to the committer, the externalised
  // checkpoint says the same thing. Break it and `consume.checkpoint.divergence` — the gauge whose documented healthy
  // value is zero — reports a permanent gap, and `POST /admin/consumer:restart?target=stored` rewinds the group onto
  // records it has already dead-lettered.

  test("a batch of nothing but poison still checkpoints past it"):
    // The whole poll dead-lettered, so there is no insert to hang the offsets off — and that is exactly the batch
    // whose position must still be recorded, because a `target=stored` restart otherwise re-consumes and
    // re-dead-letters every one of these records, forever.
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val deadLetters = Fixtures.RecordingDeadLetters()
      val batch = Vector(poisonAt(0, 7L), poisonAt(0, 8L))
      checkpointing(repository, deadLetters, ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(deadLetters.published.size, 2)
          assertEquals(repository.stored, Fixtures.committedPositions(committables))
          assertEquals(repository.stored, Map((Fixtures.Topic, 0) -> 9L))

  test("a poison record at the top of a batch does not hold the checkpoint below it"):
    // `max` over the writable records alone stops at the last one that happened to decode. The offsets in between
    // are committed to Kafka regardless, so the stored position is left permanently behind the real one.
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val batch = Vector(at("a", 0, 0L), at("b", 0, 1L), poisonAt(0, 2L))
      checkpointing(repository, Fixtures.RecordingDeadLetters(), ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(repository.stored, Fixtures.committedPositions(committables))
          assertEquals(repository.stored, Map((Fixtures.Topic, 0) -> 3L))

  test("a partition whose entire slice was poison still gets a checkpoint row"):
    // Grouping the writable records by partition cannot invent a partition none of them came from, so the partition
    // that produced only dead letters disappears from the checkpoint altogether — and never reappears until a
    // decodable record happens to arrive on it.
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val batch = Vector(at("a", 0, 4L), poisonAt(1, 11L), poisonAt(1, 12L))
      checkpointing(repository, Fixtures.RecordingDeadLetters(), ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(repository.stored, Fixtures.committedPositions(committables))
          assertEquals(repository.stored, Map((Fixtures.Topic, 0) -> 5L, (Fixtures.Topic, 1) -> 13L))

  test("bisection still ends at the batch's high-water mark"):
    // The isolation path writes each half separately, so the last durable write of a bisected batch has to carry the
    // whole batch's position — including the offsets of the record it just dead-lettered and of the poison at the top.
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository: events =>
        if events.exists(event => idOf(event) == "bad") then Future.failed(SQLException("check violation", "23514"))
        else Future.successful(events.size.toLong)
      val deadLetters = Fixtures.RecordingDeadLetters()
      val batch = Vector(at("a", 0, 0L), at("bad", 0, 1L), at("c", 0, 2L), poisonAt(0, 3L))
      checkpointing(repository, deadLetters, ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(committables.size, 4)
          assertEquals(deadLetters.published.size, 2, "one undecodable, one the database refused")
          assertEquals(repository.stored, Fixtures.committedPositions(committables))
          assertEquals(repository.stored, Map((Fixtures.Topic, 0) -> 4L))

  test("an unpersistable record at the very top of a batch still moves the stored position"):
    // The singleton dead-letter path returns without writing anything at all, so a batch that ends on a record the
    // database refuses leaves the checkpoint one short — and the next `target=stored` restart lands straight back on
    // the record that cannot be stored.
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository: events =>
        if events.exists(event => idOf(event) == "bad") then Future.failed(SQLException("bad json", "22P02"))
        else Future.successful(events.size.toLong)
      val batch = Vector(at("bad", 0, 5L))
      checkpointing(repository, Fixtures.RecordingDeadLetters(), ConsumerMetrics(telemetry.registry))
        .process(batch)
        .map: committables =>
          assertEquals(repository.stored, Fixtures.committedPositions(committables))
          assertEquals(repository.stored, Map((Fixtures.Topic, 0) -> 6L))

  test("a processor with no checkpoint store still writes nothing for an all-poison batch"):
    // The suites that have no checkpoint store must not gain a round trip: with nothing to externalise, a batch that
    // produced no rows has no reason to touch the database at all.
    withTelemetry: telemetry =>
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      processor(repository, Fixtures.RecordingDeadLetters(), ConsumerMetrics(telemetry.registry))
        .process(Vector(poisonAt(0, 1L)))
        .map: _ =>
          assertEquals(repository.calls.get(), 0)
          assertEquals(repository.checkpoints, Vector.empty)

  test("SQLSTATE classes 22 and 23 are data errors; connection and resource classes are not"):
    assert(BatchProcessor.isDataError(SQLException("bad timestamp", "22007")))
    assert(BatchProcessor.isDataError(SQLException("check violation", "23514")))
    assert(!BatchProcessor.isDataError(SQLException("connection failure", "08006")))
    assert(!BatchProcessor.isDataError(SQLException("out of memory", "53200")))
    assert(!BatchProcessor.isDataError(SQLException("syntax error", "42601")), "a build defect must page, not drain")
    assert(!BatchProcessor.isDataError(RuntimeException("nothing to do with SQL")))

  test("a data error wrapped several layers deep is still recognised"):
    val wrapped = RuntimeException("magnum", IllegalStateException("hikari", SQLException("bad json", "22P02")))
    assert(BatchProcessor.isDataError(wrapped))

  test("a self-referential cause chain terminates instead of hanging"):
    val looping = SQLException("no state at all")
    looping.setNextException(looping)
    assert(!BatchProcessor.isDataError(looping))
