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

package io.kzonix.eventing

import io.kzonix.kernel.event.Topics
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import java.util.Optional
import munit.FunSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

/** Trace propagation over Kafka headers: inject on produce, extract on consume, and the consumer span landing in the
  * producer's trace.
  *
  * These assertions are made against the OpenTelemetry **API** — a remote `SpanContext`, the W3C propagator and a no-op
  * tracer — rather than against an SDK `InMemorySpanExporter`. That is not the preferred shape: an exporter would let
  * the test assert on the recorded span's *parent span id* directly. It is what this module's classpath allows, because
  * `opentelemetry-sdk-testing` is scoped to `modules/observability` only (ADR §3.5) and adding it here is a build
  * change outside this change's scope.
  *
  * What is asserted is nevertheless the property that matters end to end: the traceparent survives the header round
  * trip byte for byte, and a span opened by [[KafkaTrace.withConsumerSpan]] on the consuming side reports the
  * producer's trace id and the producer's span id as its parent — which is exactly what makes one trace span HTTP
  * ingestion, the broker and the consumer. The `applications/cobalt` integration tier is where the exporter-based
  * assertion belongs anyway, since only there is a real SDK configured.
  */
class KafkaTraceSuite extends FunSuite:

  private val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  private val spanId = "00f067aa0ba902b7"

  private def remoteContext: Context =
    val spanContext = SpanContext.createFromRemoteParent(traceId, spanId, TraceFlags.getSampled, TraceState.getDefault)
    Context.root().`with`(Span.wrap(spanContext))

  test("inject then extract preserves the trace id and the span id"):
    val headers = RecordHeaders()
    val _ = KafkaTrace.inject(remoteContext, headers)
    val extracted = Span.fromContext(KafkaTrace.extract(headers)).getSpanContext
    assertEquals(extracted.getTraceId, traceId)
    assertEquals(extracted.getSpanId, spanId)
    assert(extracted.isValid)
    assert(extracted.isRemote)

  test("the header written is a W3C traceparent, in the format any other vendor's SDK will read"):
    val headers = RecordHeaders()
    val _ = KafkaTrace.inject(remoteContext, headers)
    assertEquals(CloudEventHeaders.get(headers, KafkaTrace.TraceParent), Some(s"00-$traceId-$spanId-01"))

  test("re-injecting replaces the traceparent instead of appending a second one"):
    val headers = RecordHeaders()
    val _ = KafkaTrace.inject(remoteContext, headers)
    val other = SpanContext.createFromRemoteParent(
      "0af7651916cd43dd8448eb211c80319c",
      "b7ad6b7169203331",
      TraceFlags.getSampled,
      TraceState.getDefault
    )
    val _ = KafkaTrace.inject(Context.root().`with`(Span.wrap(other)), headers)
    val occurrences = headers.toArray.count(_.key == KafkaTrace.TraceParent)
    assertEquals(occurrences, 1)
    assertEquals(Span.fromContext(KafkaTrace.extract(headers)).getSpanContext.getSpanId, "b7ad6b7169203331")

  test("extracting from headers that carry no trace context yields root, not an error"):
    val extracted = Span.fromContext(KafkaTrace.extract(RecordHeaders())).getSpanContext
    assert(!extracted.isValid)

  test("a garbled traceparent is ignored rather than propagated as a corrupt parent"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, KafkaTrace.TraceParent, "not-a-traceparent")
    assert(!Span.fromContext(KafkaTrace.extract(headers)).getSpanContext.isValid)

  test("the consumer span continues the producer's trace, which is the whole cross-service story"):
    val headers = RecordHeaders()
    val _ = KafkaTrace.inject(remoteContext, headers)
    val record = consumerRecord(headers)
    val tracer = OpenTelemetry.noop().getTracer("test")

    val observed = KafkaTrace.withConsumerSpan(record, tracer): _ =>
      val current = Span.current().getSpanContext
      (current.getTraceId, current.getSpanId)

    assertEquals(observed, (traceId, spanId))

  test("withConsumerSpan returns the body's value and rethrows its failure"):
    val record = consumerRecord(RecordHeaders())
    val tracer = OpenTelemetry.noop().getTracer("test")
    assertEquals(KafkaTrace.withConsumerSpan(record, tracer)(_ => 7), 7)
    intercept[IllegalStateException]:
      KafkaTrace.withConsumerSpan(record, tracer)(_ => throw IllegalStateException("boom"))

  test("trace context travels alongside the CloudEvents headers without disturbing them"):
    val envelope = WireGenerators.force(
      for
        id <- io.kzonix.kernel.event.EventId("event-1")
        source <- io.kzonix.kernel.event.Source("/sensors/kitchen")
        eventType <- io.kzonix.kernel.event.EventType("io.kzonix.iot.telemetry")
      yield io.kzonix.kernel.event.Envelope(
        id,
        source,
        eventType,
        None,
        None,
        None,
        None,
        Map.empty,
        io.kzonix.kernel.event.Payload.Empty
      )
    )
    val record = KafkaCodecs.producerRecord(Topics.CloudEvents, envelope).getOrElse(fail("could not build the record"))
    val _ = KafkaTrace.inject(record, remoteContext)

    assertEquals(ContentMode.read(record.headers, Option(record.value)), Right(envelope))
    assertEquals(
      Span.fromContext(KafkaTrace.extract(record.headers)).getSpanContext.getTraceId,
      traceId
    )

  test("the carrier operations are the three a TextCarrier binding needs, and agree with the header helpers"):
    val headers = RecordHeaders()
    val _ = KafkaHeaderCarrier.put(headers, "k", "v")
    assertEquals(KafkaHeaderCarrier.get(headers, "k"), Some("v"))
    assertEquals(KafkaHeaderCarrier.keys(headers).toSeq, Seq("k"))
    assertEquals(KafkaHeaderCarrier.get(headers, "absent"), None)

  private def consumerRecord(headers: RecordHeaders): ConsumerRecord[String, Array[Byte]] =
    ConsumerRecord[String, Array[Byte]](
      Topics.CloudEvents,
      3,
      99L,
      1700000000000L,
      TimestampType.CREATE_TIME,
      1,
      0,
      "device-1",
      Array.emptyByteArray,
      headers,
      Optional.empty[Integer]
    )
