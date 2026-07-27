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

package io.kzonix.cobalt

import io.kzonix.observability.Meters
import io.kzonix.observability.Telemetry
import io.kzonix.observability.Tracing
import io.kzonix.persistence.repository.NewEvent
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

  private def processor(
    repository: Fixtures.RecordingRepository,
    deadLetters: Fixtures.RecordingDeadLetters,
    metrics: ConsumerMetrics,
    attempts: Int = 3
  ): BatchProcessor =
    BatchProcessor(repository, deadLetters, metrics, Fixtures.source, attempts, () => Future.unit)

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
