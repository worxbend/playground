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

import io.kzonix.observability.Tracing
import io.kzonix.persistence.repository.NewEvent
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.pekko.Done
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.ConsumerMessage.Committable
import org.apache.pekko.kafka.ConsumerMessage.CommittableMessage
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.stream.testkit.scaladsl.TestSource
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** The graph, driven from memory: no broker, no database, no Docker.
  *
  * Everything asserted here is a *topology* property, and topology properties are exactly the ones that survive review
  * unnoticed. In particular "the committer is downstream of the write" is one line of code and one production incident,
  * so it is asserted directly, on an ordered journal, rather than inferred from the shape of the source file.
  */
final class ConsumerStreamSuite extends munit.FunSuite:

  given system: ActorSystem = ActorSystem("cobalt-stream-suite")
  given ExecutionContext = system.dispatcher

  override def afterAll(): Unit =
    val _ = Await.result(system.terminate(), 30.seconds)

  private val Timeout = 10.seconds

  private def idOf(event: NewEvent): String = event.raw.hcursor.get[String]("id").getOrElse("?")

  /** The committer seam, instrumented.
    *
    * Production passes `Committer.flow`; a test passes this, which appends to the same journal the write path writes
    * to. `ConsumerMessage.CommittableOffset` is sealed, so the offset cannot be a recording double — instrumenting the
    * *flow* instead is both possible and a better test, because the flow is the production seam whose position in the
    * graph is the property under test.
    */
  private def committer(journal: ConcurrentLinkedQueue[String]): Flow[Committable, Done, NotUsed] =
    Flow[Committable].map: committable =>
      val _ = journal.add(s"commit:${Fixtures.offsetOf(committable)}")
      Done

  private def message(id: String, offset: Long): CommittableMessage[String, Array[Byte]] =
    Fixtures.committableMessage(Fixtures.record(Fixtures.envelope(id), offset = offset), Fixtures.offsetFor(0, offset))

  private def malformed(offset: Long): CommittableMessage[String, Array[Byte]] =
    Fixtures.committableMessage(Fixtures.malformedRecord(offset = offset), Fixtures.offsetFor(0, offset))

  private def graph(
    processor: BatchProcessor,
    batchSize: Int,
    journal: ConcurrentLinkedQueue[String]
  ): Flow[CommittableMessage[String, Array[Byte]], Done, NotUsed] =
    ConsumerStream.processing(
      decode = RecordDecoder(Fixtures.source, Tracing.noop.tracer).decode,
      processor = processor,
      batchSize = batchSize,
      batchWindow = 100.millis,
      committer = committer(journal)
    )

  private def processor(
    repository: Fixtures.RecordingRepository,
    deadLetters: Fixtures.RecordingDeadLetters,
    telemetry: io.kzonix.observability.Telemetry
  ): BatchProcessor =
    BatchProcessor(
      repository,
      deadLetters,
      ConsumerMetrics(telemetry.registry),
      Fixtures.source,
      attempts = 1,
      backoff = () => Future.unit
    )

  test("records are grouped into batches no larger than the configured size"):
    val telemetry = Fixtures.telemetry()
    try
      val journal = ConcurrentLinkedQueue[String]()
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val messages = (0 until 5).map(i => message(s"e$i", i.toLong)).toVector
      val done = Source(messages)
        .via(graph(processor(repository, Fixtures.RecordingDeadLetters(), telemetry), batchSize = 2, journal))
        .runWith(Sink.seq)
      val _ = Await.result(done, Timeout)
      assertEquals(repository.batches.map(_.size), Vector(2, 2, 1))
      assertEquals(repository.batches.flatMap(_.map(idOf)), Vector("e0", "e1", "e2", "e3", "e4"))
    finally telemetry.close()

  test("an offset is never committed before the batch it belongs to is durable"):
    val telemetry = Fixtures.telemetry()
    try
      val journal = ConcurrentLinkedQueue[String]()
      val repository = Fixtures.RecordingRepository: events =>
        val _ = journal.add(s"write:${events.map(idOf).mkString(",")}")
        Future.successful(events.size.toLong)
      val messages = (0 until 4).map(i => message(s"e$i", i.toLong)).toVector
      val done = Source(messages)
        .via(graph(processor(repository, Fixtures.RecordingDeadLetters(), telemetry), batchSize = 2, journal))
        .runWith(Sink.seq)
      val _ = Await.result(done, Timeout)
      assertEquals(
        journal.asScala.toVector,
        Vector("write:e0,e1", "commit:0", "commit:1", "write:e2,e3", "commit:2", "commit:3")
      )
    finally telemetry.close()

  test("a malformed record goes to the DLQ and its offset is still committed"):
    val telemetry = Fixtures.telemetry()
    try
      val journal = ConcurrentLinkedQueue[String]()
      val repository = Fixtures.RecordingRepository(events => Future.successful(events.size.toLong))
      val deadLetters = Fixtures.RecordingDeadLetters()
      val messages = Vector(message("e0", 0L), malformed(1L), message("e2", 2L))
      val done = Source(messages)
        .via(graph(processor(repository, deadLetters, telemetry), batchSize = 10, journal))
        .runWith(Sink.seq)
      val _ = Await.result(done, Timeout)
      assertEquals(deadLetters.published.map(_.origin.offset), Vector(1L))
      assertEquals(journal.asScala.toVector.filter(_.startsWith("commit:")), Vector("commit:0", "commit:1", "commit:2"))
      assertEquals(repository.batches.flatMap(_.map(idOf)), Vector("e0", "e2"))
    finally telemetry.close()

  test("a failing write fails the stream with the offsets uncommitted"):
    val telemetry = Fixtures.telemetry()
    try
      val journal = ConcurrentLinkedQueue[String]()
      val repository = Fixtures.RecordingRepository(_ => Future.failed(java.sql.SQLException("down", "08006")))
      val (source, sink) = TestSource[CommittableMessage[String, Array[Byte]]]()
        .via(graph(processor(repository, Fixtures.RecordingDeadLetters(), telemetry), batchSize = 1, journal))
        .toMat(TestSink[Done]())(Keep.both)
        .run()
      sink.request(1)
      source.sendNext(message("e0", 0L))
      val _ = sink.expectError()
      assertEquals(journal.asScala.toVector, Vector.empty, "nothing may be committed when the write failed")
    finally telemetry.close()

  test("the restart source re-captures the consumer control on every attempt"):
    val attempts = AtomicInteger(0)
    val controls = Vector(FakeControl("first"), FakeControl("second"))
    val captured = AtomicReference[Consumer.Control](Consumer.NoopControl)
    val attempt = () =>
      val n = attempts.getAndIncrement()
      val inner = if n == 0 then Source.failed[Done](RuntimeException("broker restarted")) else Source.single(Done)
      inner.mapMaterializedValue(_ => controls(math.min(n, controls.size - 1)))

    val settings = RestartSettings(10.millis, 50.millis, 0.0d).withMaxRestarts(3, 1.minute)
    val done = ConsumerStream.restarting(settings, captured)(attempt).runWith(Sink.seq)
    val emitted = Await.result(done, Timeout)

    assertEquals(emitted, Seq(Done), "the second attempt must produce the element the first one failed to")
    assertEquals(attempts.get(), 2)
    assertEquals(
      captured.get().asInstanceOf[FakeControl].name,
      "second",
      "a stale control would be drained instead of the live consumer"
    )

  test("restart settings carry the ADR's bounds through unchanged"):
    val settings = ConsumerStream.restartSettings(RestartConfig(1.second, 30.seconds, 0.2d, 50, 10.minutes))
    assertEquals(settings.minBackoff, 1.second)
    assertEquals(settings.maxBackoff, 30.seconds)
    assertEquals(settings.maxRestarts, 50)
    assertEquals(settings.maxRestartsWithin, 10.minutes)

  /** A `Consumer.Control` that does nothing but be identifiable. Nothing calls it; the test asserts *which one* the
    * restart wrapper captured.
    */
  final case class FakeControl(name: String) extends Consumer.Control:
    def stop(): Future[Done] = Future.successful(Done)
    def shutdown(): Future[Done] = Future.successful(Done)
    def isShutdown: Future[Done] = Future.successful(Done)
    def metrics: Future[Map[MetricName, Metric]] = Future.successful(Map.empty)
