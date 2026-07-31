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

import com.worxbend.eventing.KafkaTrace
import com.worxbend.observability.Tracing
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.header.internals.RecordHeaders
import scala.jdk.CollectionConverters.*

/** Decoding must be total and it must continue the trace. Those are the two properties this suite exists for.
  *
  * The trace assertion uses a real SDK tracer with a hand-rolled collecting exporter rather than
  * `opentelemetry-sdk-testing`, which is not on cobalt's classpath (ADR §3.7 does not list it) — and adding a
  * dependency to make a test easier is exactly the kind of build change this work is not allowed to make.
  */
final class RecordDecoderSuite extends munit.FunSuite:

  /** Collects exported spans in memory. Four methods, all trivial; `SpanExporter` is in `opentelemetry-sdk`, which is
    * already here via `modules/observability`.
    */
  final class CollectingExporter extends SpanExporter:
    private val collected = java.util.concurrent.ConcurrentLinkedQueue[SpanData]()
    def `export`(spans: java.util.Collection[SpanData]): CompletableResultCode =
      val _ = collected.addAll(spans)
      CompletableResultCode.ofSuccess()
    def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
    def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
    def spans: Vector[SpanData] = collected.asScala.toVector

  private def decoder(): RecordDecoder = RecordDecoder(Fixtures.source, Tracing.noop.tracer)

  test("a binary-mode record decodes to the envelope that was published"):
    val envelope = Fixtures.envelope("evt-1")
    val outcome = decoder().decodeRecord(Fixtures.record(envelope))
    assertEquals(outcome.map(_.envelope.id), Right(envelope.id))
    assertEquals(outcome.map(_.event.occurredAt), Right(Fixtures.at))

  test("a record no content mode can read becomes a dead letter, not an exception"):
    val outcome = decoder().decodeRecord(Fixtures.malformedRecord(partition = 2, offset = 17L))
    assert(outcome.isLeft, "a malformed record must decode to a Left")
    val deadLetter = outcome.swap.toOption.get
    assertEquals(deadLetter.origin.partition, 2)
    assertEquals(deadLetter.origin.offset, 17L)
    assertEquals(deadLetter.source, Fixtures.source)
    assert(deadLetter.payload.isDefined, "the original bytes must survive so the record is replayable")

  test("a spec-valid event with no time is dead-lettered rather than given an invented timestamp"):
    val outcome = decoder().decodeRecord(Fixtures.record(Fixtures.envelope("evt-2", time = None)))
    assert(outcome.isLeft, "an event with no time has no partition to land in")
    assertEquals(outcome.swap.toOption.map(_.reason), Some("unconvertible"))

  test("a record with a null value is a dead letter and never a NullPointerException"):
    val record = Fixtures.consumerRecord("k", null, RecordHeaders(), 0, 0L)
    assert(decoder().decodeRecord(record).isLeft)

  test("the consumer span continues the trace the producer injected"):
    val exporter = CollectingExporter()
    val provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
    try
      val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
      val spanId = "00f067aa0ba902b7"
      val remote =
        Span.wrap(SpanContext.createFromRemoteParent(traceId, spanId, TraceFlags.getSampled, TraceState.getDefault))
      val record = Fixtures.record(Fixtures.envelope("evt-3"))
      val _ = KafkaTrace.inject(remote.storeInContext(io.opentelemetry.context.Context.root()), record.headers)

      val message = Fixtures.committableMessage(record, Fixtures.offsetFor(0, 0L))
      val decoded = RecordDecoder(Fixtures.source, provider.get("test")).decode(message)
      assert(decoded.outcome.isRight)

      val _ = provider.forceFlush().join(5, TimeUnit.SECONDS)
      val span = exporter.spans.headOption.getOrElse(fail("the decoder opened no span"))
      assertEquals(span.getTraceId, traceId, "the consumer span must join the producer's trace")
      assertEquals(span.getParentSpanId, spanId, "the consumer span must be a child of the ingest span")
      assertEquals(span.getKind, SpanKind.CONSUMER)
    finally provider.close()
