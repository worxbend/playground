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

import io.kzonix.eventing.CloudEventHeaders
import io.kzonix.eventing.ContentMode
import io.kzonix.eventing.KafkaTrace
import io.kzonix.kernel.event.Envelope
import io.kzonix.observability.Meters
import io.kzonix.observability.Tracing
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executor
import java.util.concurrent.RejectedExecutionException
import munit.FunSuite
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.RoundRobinPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** The Kafka publisher: the record it builds, the trace context it carries, and how it fails.
  *
  * A `MockProducer` rather than a container, because everything asserted here is about what wolfram *hands* the
  * producer — headers, key, trace context — and about the failure paths, which a healthy broker cannot produce on
  * demand. That the record survives a real broker is `src/it`'s job.
  *
  * The tracer is a real SDK one and not [[Tracing.noop]]: a no-op tracer produces an invalid span context, which the
  * W3C propagator correctly declines to inject — so a traceparent assertion against it would pass or fail for reasons
  * unrelated to this class.
  */
final class KafkaEventPublisherSuite extends FunSuite:

  private val topic = "events.cloudevents.v1"

  private val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  private val spanId = "00f067aa0ba902b7"

  /** Runs the hand-off on the calling thread, so an assertion follows a completed future without sleeping. */
  private val runInline: Executor = runnable => runnable.run()

  /** A one-node, one-partition cluster: `MockProducer` needs real metadata for the partitioner, and `probe()` reads
    * exactly this to decide readiness.
    */
  private val cluster: Cluster =
    val node = Node(0, "localhost", 9092)
    Cluster(
      "wolfram-test",
      List(node).asJava,
      List(PartitionInfo(topic, 0, node, Array(node), Array(node))).asJava,
      java.util.Set.of[String](),
      java.util.Set.of[String]()
    )

  private def envelope: Envelope =
    Envelope
      .parse(Fixtures.structuredBody())
      .fold(message => fail(s"fixture should parse: $message"), identity)

  private def recordingTracing: Tracing =
    val provider = SdkTracerProvider.builder().build()
    val sdk = OpenTelemetrySdk
      .builder()
      .setTracerProvider(provider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build()
    Tracing(sdk, Tracing.ScopeName, () => provider.close())

  final private case class Harness(
    publisher: KafkaEventPublisher,
    producer: MockProducer[String, Array[Byte]],
    health: BrokerHealth,
    registry: SimpleMeterRegistry
  )

  private def harness(autoComplete: Boolean = true, executor: Executor = runInline): Harness =
    val producer =
      MockProducer[String, Array[Byte]](
        cluster,
        autoComplete,
        RoundRobinPartitioner(),
        StringSerializer(),
        ByteArraySerializer()
      )
    val registry = SimpleMeterRegistry()
    val health = BrokerHealth()
    val publisher = new KafkaEventPublisher(
      producer = producer,
      topic = topic,
      sender = executor,
      health = health,
      metrics = IngestMetrics(registry),
      tracing = recordingTracing,
      closeTimeout = 1.second,
      now = () => Fixtures.now
    )
    Harness(publisher, producer, health, registry)

  private def publish(h: Harness): Either[Rejection, PublishAck] =
    Await.result(h.publisher.publish(envelope), 5.seconds)

  test("the record is binary content mode, keyed by kernel's partition key"):
    val h = harness()
    assert(publish(h).isRight)

    val record = h.producer.history.asScala.head
    assertEquals(record.topic, topic)
    assertEquals(record.key, envelope.partitionKey)
    assertEquals(ContentMode.of(record.headers), Some(ContentMode.Binary))
    assertEquals(CloudEventHeaders.get(record.headers, "ce_id"), Some("evt-1"))
    assertEquals(CloudEventHeaders.get(record.headers, "ce_type"), Some("io.kzonix.iot.telemetry"))
    assertEquals(String(record.value, UTF_8), """{"celsius":21.5}""")

  test("the ambient trace context is injected as a W3C traceparent, so the ingest span continues in the consumer"):
    val h = harness()
    val parent = Span.wrap(SpanContext.create(traceId, spanId, TraceFlags.getSampled, TraceState.getDefault))
    val scope = Context.root().`with`(parent).makeCurrent()
    try assert(publish(h).isRight)
    finally scope.close()

    val headers = h.producer.history.asScala.head.headers
    assert(CloudEventHeaders.get(headers, KafkaTrace.TraceParent).isDefined, CloudEventHeaders.toMap(headers).toString)
    // Extracted with the module cobalt uses, because that is the actual cross-service contract: the produce span is
    // the parent, and it belongs to the trace the request arrived on.
    val extracted = Span.fromContext(KafkaTrace.extract(headers)).getSpanContext
    assertEquals(extracted.getTraceId, traceId)
    assertNotEquals(extracted.getSpanId, spanId)

  test("a successful publish is timed as a success and marks the broker reachable"):
    val h = harness()
    assertEquals(publish(h).map(_.topic), Right(topic))
    assert(h.publisher.brokerReachable)
    assertEquals(timerCount(h, Meters.Outcomes.Success), 1L)

  test("a broker refusal becomes a BrokerUnavailable value, is timed as a failure, and fails readiness"):
    val h = harness(autoComplete = false)
    val pending = h.publisher.publish(envelope)
    val _ = h.producer.errorNext(TimeoutException("no leader for partition"))

    val result = Await.result(pending, 5.seconds)
    assert(result.left.exists(_.reason == Meters.Reasons.Unpersistable), result.toString)
    assert(result.left.exists(_.detail.contains("no leader")), result.toString)
    assert(!h.publisher.brokerReachable)
    assertEquals(timerCount(h, Meters.Outcomes.Failure), 1L)

  test("a full sender queue sheds the request rather than buffering in front of the broker"):
    val rejecting: Executor = _ => throw RejectedExecutionException("queue full")
    val h = harness(executor = rejecting)
    val result = publish(h)
    assert(result.left.exists(_.reason == Meters.Reasons.Unpersistable), result.toString)
    assert(result.left.exists(_.detail.contains("queue is full")), result.toString)
    assert(!h.publisher.brokerReachable)
    assertEquals(h.producer.history.size, 0)

  test("readiness starts pessimistic; only real evidence makes it positive"):
    val h = harness()
    assertEquals(h.health.evidence, BrokerHealth.Evidence.Unknown)
    assert(!h.publisher.brokerReachable)
    h.publisher.probe()
    assertEquals(h.health.evidence, BrokerHealth.Evidence.Reachable(Fixtures.now))
    assert(h.publisher.brokerReachable)

  test("shutdown flushes before closing, so records a client was already told were accepted still land"):
    val h = harness(autoComplete = false)
    val pending = h.publisher.publish(envelope)
    h.publisher.close()
    assert(h.producer.closed())
    assertEquals(Await.result(pending, 5.seconds).map(_.topic), Right(topic))

  private def timerCount(h: Harness, outcome: String): Long =
    Option(h.registry.find(Meters.KafkaProduceLatency).tag(Meters.TagKeys.Outcome, outcome).timer())
      .map(_.count())
      .getOrElse(0L)
