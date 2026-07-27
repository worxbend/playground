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

package io.kzonix.wolfram

import io.kzonix.eventing.ContentMode
import io.kzonix.kernel.Rfc3339
import io.kzonix.observability.Meters
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import munit.FunSuite
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** The application service: validation order, the partition key, and the metric a rejection produces.
  *
  * The metric assertions are as load-bearing as the behavioural ones. `ingest.events.rejected{reason}` is what an
  * operator alerts on, and a rejection path that returns the right status while incrementing nothing is invisible — the
  * API looks healthy and events silently do not arrive.
  */
final class IngestionServiceSuite extends FunSuite:

  private given ExecutionContext = ExecutionContext.parasitic

  private def bytes(text: String): Array[Byte] = text.getBytes(UTF_8)

  final private case class Harness(
    service: IngestionService,
    registry: MeterRegistry,
    publisher: Fixtures.StubPublisher
  ):
    def rejectedCount(reason: String): Double =
      Option(registry.find(Meters.IngestRejected).tag(Meters.TagKeys.Reason, reason).counter())
        .map(_.count())
        .getOrElse(0.0)

    def receivedCount(mode: String): Double =
      Option(registry.find(Meters.IngestReceived).tag(Meters.TagKeys.Mode, mode).counter())
        .map(_.count())
        .getOrElse(0.0)

  private def harness(publisher: Fixtures.StubPublisher = Fixtures.StubPublisher()): Harness =
    val registry = SimpleMeterRegistry()
    val service =
      IngestionService(
        publisher,
        TimeClamp.from(Fixtures.ingest),
        Fixtures.ingest,
        IngestMetrics(registry),
        () => Fixtures.now
      )
    Harness(service, registry, publisher)

  private def ingest(harness: Harness, headers: Map[String, String], body: String) =
    Await.result(harness.service.ingest(headers, bytes(body)), 5.seconds)

  test("a structured event is published and its receipt reports where it landed"):
    val h = harness()
    val result = ingest(h, Fixtures.structuredHeaders, Fixtures.structuredBody())
    assertEquals(result.map(_.ack), Right(Fixtures.ack))
    assertEquals(h.publisher.published.size, 1)
    assertEquals(h.receivedCount(Meters.Modes.Structured), 1.0)

  test("a binary event is published, and is metered as binary"):
    val h = harness()
    val result = Await.result(
      h.service.ingest(HttpBinding.normalise(Fixtures.binaryHeaders()), bytes(Fixtures.binaryBody)),
      5.seconds
    )
    assert(result.isRight, result.toString)
    assertEquals(h.receivedCount(Meters.Modes.Binary), 1.0)

  test("the partition key is kernel's, never recomputed here"):
    val h = harness()
    val withSubject = ingest(h, Fixtures.structuredHeaders, Fixtures.structuredBody())
    val withoutSubject = ingest(h, Fixtures.structuredHeaders, Fixtures.structuredBody(subject = None))

    assertEquals(withSubject.map(_.accepted.partitionKey), Right("/gateway/kitchen#kitchen-thermostat"))
    assertEquals(withoutSubject.map(_.accepted.partitionKey), Right("/gateway/kitchen"))
    // The delegation itself: whatever kernel says for the envelope the service decoded is what the service reports.
    h.publisher.published.zip(Vector(withSubject, withoutSubject)).foreach: (envelope, result) =>
      assertEquals(result.map(_.accepted.partitionKey), Right(envelope.partitionKey))

  test("an unparseable body is rejected as malformed and metered as malformed"):
    val h = harness()
    val result = ingest(h, Fixtures.structuredHeaders, "{ not json")
    assert(result.left.exists(_.reason == Meters.Reasons.Malformed), result.toString)
    assertEquals(h.rejectedCount(Meters.Reasons.Malformed), 1.0)
    assertEquals(h.publisher.published.size, 0)

  test("a missing attribute is rejected as invalid-attributes and metered as such"):
    val h = harness()
    val result = ingest(h, Fixtures.structuredHeaders, """{"specversion":"1.0","id":"x"}""")
    assert(result.left.exists(_.reason == Meters.Reasons.InvalidAttributes), result.toString)
    assertEquals(h.rejectedCount(Meters.Reasons.InvalidAttributes), 1.0)

  test("an implausible time is rejected, and shares the invalid-attributes tag with other attribute failures"):
    val h = harness()
    val stale = Rfc3339.render(Fixtures.at(Duration.ofDays(-200)))
    val result = ingest(h, Fixtures.structuredHeaders, Fixtures.structuredBody(time = Some(stale)))
    assert(result.left.exists(_.detail.contains("in the past")), result.toString)
    assertEquals(h.rejectedCount(Meters.Reasons.InvalidAttributes), 1.0)

  test("an oversize body is rejected before it is decoded, and metered as too-large"):
    val h = harness()
    val padding = "x" * (Fixtures.ingest.maxEventBytes.toInt + 1)
    val result = ingest(h, Fixtures.structuredHeaders, padding)
    assert(result.left.exists(_.reason == Meters.Reasons.TooLarge), result.toString)
    assertEquals(h.rejectedCount(Meters.Reasons.TooLarge), 1.0)

  test("a broker refusal is metered as unpersistable and never counted as received"):
    val h = harness(Fixtures.unavailable)
    val result = ingest(h, Fixtures.structuredHeaders, Fixtures.structuredBody())
    assert(result.left.exists(_.reason == Meters.Reasons.Unpersistable), result.toString)
    assertEquals(h.rejectedCount(Meters.Reasons.Unpersistable), 1.0)
    assertEquals(h.receivedCount(Meters.Modes.Structured), 0.0)

  test("validate is pure and decides mode, time and partition key together"):
    val h = harness()
    val accepted = h.service
      .validate(Fixtures.structuredHeaders, bytes(Fixtures.structuredBody()))
      .fold(rejection => fail(rejection.message), identity)
    assertEquals(accepted.mode, ContentMode.Structured)
    assertEquals(Rfc3339.render(accepted.occurredAt), Fixtures.eventTime)
    assertEquals(accepted.partitionKey, accepted.envelope.partitionKey)

  test("a batch publishes the good elements and reports the bad ones, in request order"):
    val h = harness()
    val body = Fixtures.batchBody(
      Fixtures.structuredBody(id = "a"),
      """{"specversion":"1.0","id":"b"}""",
      Fixtures.structuredBody(id = "c")
    )
    val outcome = Await
      .result(h.service.ingestBatch(bytes(body)), 5.seconds)
      .fold(rejection => fail(rejection.message), identity)

    assertEquals(outcome.accepted, 2)
    assertEquals(outcome.rejected, 1)
    assert(outcome.partial)
    assert(outcome.results(0).isRight)
    assert(outcome.results(1).isLeft)
    assert(outcome.results(2).isRight)
    assertEquals(h.publisher.published.map(e => e.id: String), Vector("a", "c"))

  test("a batch element with an implausible time is refused without taking the rest of the batch down"):
    val h = harness()
    val stale = Rfc3339.render(Fixtures.at(Duration.ofDays(-200)))
    val body =
      Fixtures.batchBody(Fixtures.structuredBody(id = "a", time = Some(stale)), Fixtures.structuredBody(id = "b"))
    val outcome = Await
      .result(h.service.ingestBatch(bytes(body)), 5.seconds)
      .fold(rejection => fail(rejection.message), identity)
    assertEquals(outcome.accepted, 1)
    assertEquals(outcome.rejected, 1)

  test("a batch with too many elements is refused as a whole"):
    val h = harness()
    val body = Fixtures.batchBody(Seq.fill(Fixtures.ingest.maxBatchEvents + 1)(Fixtures.structuredBody())*)
    val result = Await.result(h.service.ingestBatch(bytes(body)), 5.seconds)
    assert(result.left.exists(_.reason == Meters.Reasons.TooLarge), result.toString)
    assertEquals(h.publisher.published.size, 0)
