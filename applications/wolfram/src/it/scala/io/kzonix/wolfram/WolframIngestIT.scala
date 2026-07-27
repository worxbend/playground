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

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration as JavaDuration
import java.time.Instant
import java.time.ZoneOffset
import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

import io.kzonix.eventing.CloudEventHeaders
import io.kzonix.eventing.ContentMode
import io.kzonix.eventing.KafkaTrace
import io.kzonix.kernel.Rfc3339
import io.kzonix.kernel.event.Topics
import io.kzonix.observability.Telemetry
import io.kzonix.observability.TelemetryConfig
import io.kzonix.observability.Tracing
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import munit.FunSuite
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import sttp.client3.*
import sttp.model.StatusCode

/** wolfram end-to-end against a **real broker**: HTTP in, a CloudEvents record out.
  *
  * Everything in `src/test` proves that the ingestion service builds the right record. That is necessary and not
  * sufficient — the things that break in production are the ones the broker and the HTTP server are in the middle of:
  * whether the `ce_*` headers survive the Kafka wire protocol, whether the key really lands on the partition kernel's
  * function chose, and above all whether the `traceparent` a client sent arrives in the consumer's headers on the same
  * trace. None of those is observable without both ends running.
  *
  * **Broker discovery.** `KAFKA_BOOTSTRAP_SERVERS` from the environment, skipping when it is absent, so `IT/testFull`
  * is green on a machine with no Docker. ADR §9.2 asks for a Testcontainers `KafkaContainer` behind a shared lazy
  * singleton, and that is the intended shape — but `testcontainers-scala` is wired to `modules/eventing` and
  * `modules/persistence` only, and this module's `build.sbt` entry declares no Testcontainers dependency. Adding
  * `libraryDependencies ++= testContainers.map(_ % IT)` to the `wolfram` project is the whole change; every test below
  * already takes the broker address as a parameter, exactly as `modules/eventing`'s `KafkaWireIT` does.
  */
final class WolframIngestIT extends FunSuite:

  /** The single point a Testcontainers `KafkaContainer` replaces. */
  private def bootstrapServers: Option[String] =
    sys.env.get("KAFKA_BOOTSTRAP_SERVERS").filter(_.trim.nonEmpty)

  private val incomingTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
  private val incomingTraceId = "0af7651916cd43dd8448eb211c80319c"

  private def eventTime: String = Rfc3339.render(Instant.now().atOffset(ZoneOffset.UTC))

  test("a binary-mode POST becomes a binary-mode Kafka record, keyed and traced"):
    withApp: (app, topic, servers) =>
      val backend = HttpClientSyncBackend()
      val response = basicRequest
        .post(uri"http://localhost:${app.port}/events")
        .header("ce-specversion", "1.0")
        .header("ce-id", "it-1")
        .header("ce-source", "/gateway/it")
        .header("ce-type", "io.kzonix.iot.telemetry")
        .header("ce-subject", "kitchen-thermostat")
        .header("ce-time", eventTime)
        .header("content-type", "application/json")
        .header(KafkaTrace.TraceParent, incomingTraceParent)
        .body("""{"celsius":21.5}""".getBytes(UTF_8))
        .send(backend)

      assertEquals(response.code, StatusCode.Accepted)

      val record = consumeOne(servers, topic)
      assertEquals(ContentMode.of(record.headers), Some(ContentMode.Binary))
      assertEquals(CloudEventHeaders.get(record.headers, "ce_id"), Some("it-1"))
      assertEquals(CloudEventHeaders.get(record.headers, "ce_type"), Some("io.kzonix.iot.telemetry"))
      assertEquals(record.key, "/gateway/it#kitchen-thermostat")
      assertEquals(String(record.value, UTF_8), """{"celsius":21.5}""")

      // The whole point of the produce boundary: the trace the client started continues into the consumer.
      assert(CloudEventHeaders.get(record.headers, KafkaTrace.TraceParent).isDefined)
      val extracted = Span.fromContext(KafkaTrace.extract(record.headers)).getSpanContext
      assertEquals(extracted.getTraceId, incomingTraceId)
      assert(extracted.isRemote)

  test("a structured-mode POST produces the identical record, so the content mode is a client's choice only"):
    withApp: (app, topic, servers) =>
      val backend = HttpClientSyncBackend()
      val body =
        s"""{"specversion":"1.0","id":"it-2","source":"/gateway/it","type":"io.kzonix.iot.telemetry",
           |"subject":"kitchen-thermostat","time":"$eventTime","datacontenttype":"application/json",
           |"data":{"celsius":21.5}}""".stripMargin

      val response = basicRequest
        .post(uri"http://localhost:${app.port}/events")
        .header("content-type", HttpBinding.StructuredMediaType)
        .body(body.getBytes(UTF_8))
        .send(backend)

      assertEquals(response.code, StatusCode.Accepted)

      val record = consumeOne(servers, topic)
      assertEquals(ContentMode.of(record.headers), Some(ContentMode.Binary))
      assertEquals(CloudEventHeaders.get(record.headers, "ce_id"), Some("it-2"))
      assertEquals(record.key, "/gateway/it#kitchen-thermostat")

  test("readiness reflects the broker and the exposition carries the ingest meters"):
    withApp: (app, _, _) =>
      val backend = HttpClientSyncBackend()
      val ready = basicRequest.get(uri"http://localhost:${app.port}/health/ready").send(backend)
      assertEquals(ready.code, StatusCode.Ok)

      val live = basicRequest.get(uri"http://localhost:${app.port}/health/live").send(backend)
      assertEquals(live.code, StatusCode.Ok)

      val metrics = basicRequest.get(uri"http://localhost:${app.port}/metrics").send(backend)
      assert(metrics.body.exists(_.contains("jvm_memory_used_bytes")), metrics.body.toString)

      val openApi = basicRequest.get(uri"http://localhost:${app.port}/openapi.json").send(backend)
      assert(openApi.body.exists(_.contains("\"/events\"")), openApi.body.toString)

  /** Boots the real service on an ephemeral port against a freshly created topic. */
  private def withApp(body: (WolframApp, String, String) => Unit): Unit =
    bootstrapServers match
      case None =>
        // Skipped rather than failed: the fast tier must stay runnable without a broker (ADR §9.2).
        ()
      case Some(servers) =>
        val topic = newTopic(servers)
        val telemetry = Telemetry.start(TelemetryConfig("wolfram", "it", "it-0"), recordingTracing)
        val app = WolframApp.start(config(servers, topic), telemetry)
        try body(app, topic, servers)
        finally app.close()

  /** A real SDK tracer: [[Tracing.noop]] would produce an invalid span context, which the W3C propagator declines to
    * inject — the traceparent assertion would then pass or fail for the wrong reason.
    */
  private def recordingTracing: Tracing =
    val provider = SdkTracerProvider.builder().build()
    val sdk = OpenTelemetrySdk
      .builder()
      .setTracerProvider(provider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build()
    Tracing(sdk, Tracing.ScopeName, () => provider.close())

  private def config(servers: String, topic: String): WolframConfig =
    WolframConfig(
      server = ServerConfig("127.0.0.1", 0),
      ingest = IngestConfig(1048576L, 256, 24.hours, 90.days),
      publisher = PublisherConfig(
        bootstrapServers = servers,
        topic = topic,
        maxBlock = 5.seconds,
        deliveryTimeout = 20.seconds,
        requestTimeout = 10.seconds,
        closeTimeout = 10.seconds,
        queueCapacity = 128,
        properties = Map.empty
      )
    )

  /** A topic per test, so one test's records can never be read by another. */
  private def newTopic(servers: String): String =
    val name = s"wolfram-it-${java.util.UUID.randomUUID()}"
    val admin = Admin.create(properties(servers))
    try
      val _ = admin
        .createTopics(List(NewTopic(name, Topics.CloudEventsPartitions, 1.toShort)).asJava)
        .all()
        .get()
      name
    finally admin.close()

  private def consumeOne(servers: String, topic: String): ConsumerRecord[String, Array[Byte]] =
    val settings = properties(servers)
    settings.put(ConsumerConfig.GROUP_ID_CONFIG, s"wolfram-it-${java.util.UUID.randomUUID()}")
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = KafkaConsumer[String, Array[Byte]](settings, StringDeserializer(), ByteArrayDeserializer())
    try
      consumer.subscribe(List(topic).asJava)
      val deadline = System.nanoTime() + 30_000_000_000L
      var found: Option[ConsumerRecord[String, Array[Byte]]] = None
      while found.isEmpty && System.nanoTime() < deadline do
        found = consumer.poll(JavaDuration.ofMillis(500)).records(topic).asScala.headOption
      found.getOrElse(fail(s"no record arrived on $topic within 30s"))
    finally consumer.close()

  private def properties(servers: String): Properties =
    val settings = Properties()
    settings.put("bootstrap.servers", servers)
    settings
