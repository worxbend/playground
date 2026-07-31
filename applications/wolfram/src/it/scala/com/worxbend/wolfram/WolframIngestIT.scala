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

package com.worxbend.wolfram

import com.dimafeng.testcontainers.KafkaContainer
import com.worxbend.eventing.CloudEventHeaders
import com.worxbend.eventing.ContentMode
import com.worxbend.eventing.KafkaTrace
import com.worxbend.kernel.Rfc3339
import com.worxbend.kernel.event.Topics
import com.worxbend.observability.Telemetry
import com.worxbend.observability.TelemetryConfig
import com.worxbend.observability.Tracing
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration as JavaDuration
import java.time.Instant
import java.time.ZoneOffset
import java.util.Properties
import munit.FunSuite
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.testcontainers.utility.DockerImageName
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.util.Try
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
  * **Broker discovery.** ADR §9.2's shape: a Testcontainers `KafkaContainer` behind a lazy singleton in the companion
  * object, started once per forked JVM and reaped by Ryuk. `KAFKA_BOOTSTRAP_SERVERS` still wins when it is set — that
  * is how a CI job points every module's slow tier at one broker rather than starting three.
  *
  * The suite is skipped only when Docker itself is unreachable. It is worth saying what this replaced: `withApp`
  * previously returned unit when the environment variable was absent, so all three tests reported **success** having
  * executed no assertion and contacted no broker. Green is the one result nobody investigates, and the suite had never
  * run once since it was written.
  */
final class WolframIngestIT extends FunSuite:

  override def munitIgnore: Boolean = WolframIngestIT.bootstrapServers.isEmpty

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
        .header("ce-type", "com.worxbend.iot.telemetry")
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
      assertEquals(CloudEventHeaders.get(record.headers, "ce_type"), Some("com.worxbend.iot.telemetry"))
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
        s"""{"specversion":"1.0","id":"it-2","source":"/gateway/it","type":"com.worxbend.iot.telemetry",
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

  test("a real request produces histogram buckets in the real exposition"):
    withApp: (app, _, _) =>
      val backend = HttpClientSyncBackend()
      // One request through the running server, so `http.server.requests` exists with the interceptor's real tags —
      // the bucket filter in `Telemetry` is keyed on the meter *name*, and a unit test that registers the timer by
      // hand cannot prove the name the interceptor actually uses is the one the filter matches.
      val posted = basicRequest
        .post(uri"http://localhost:${app.port}/events")
        .header("ce-specversion", "1.0")
        .header("ce-id", "it-buckets")
        .header("ce-source", "/gateway/it")
        .header("ce-type", "com.worxbend.iot.telemetry")
        // `ce-subject` is not decoration here: refinement resolves the device from `subject` first, and without either
        // it or a `data.deviceId` the event is Unrecognised for lack of an identity, not for its payload.
        .header("ce-subject", "kitchen-thermostat")
        .header("ce-time", eventTime)
        .header("content-type", "application/json")
        .body("""{"metric":"temperature","value":21.5,"unit":"C"}""".getBytes(UTF_8))
        .send(backend)
      assertEquals(posted.code, StatusCode.Accepted)

      val exposition = basicRequest
        .get(uri"http://localhost:${app.port}/metrics")
        .send(backend)
        .body
        .getOrElse(fail("no exposition"))

      assert(
        exposition.contains("http_server_requests_seconds_bucket"),
        "the HTTP timer reached Prometheus without buckets, so no latency percentile panel can work"
      )
      assert(
        exposition.contains("kafka_produce_latency_seconds_bucket"),
        "the produce timer reached Prometheus without buckets"
      )
      // A well-formed telemetry payload: refinement succeeded, so nothing was counted as unrecognised.
      assert(!exposition.contains("event_unrecognised_total"), "a decodable event was counted as unrecognised")

  /** Boots the real service on an ephemeral port against a freshly created topic. */
  private def withApp(body: (WolframApp, String, String) => Unit): Unit =
    val servers = WolframIngestIT.bootstrapServers.getOrElse(fail("no broker"))
    val topic = newTopic(servers)
    val telemetry = Telemetry.start(TelemetryConfig("wolfram", "it", "it-0"), recordingTracing)
    val app = WolframApp.start(config(servers, topic), telemetry)
    // `app.close()` closes telemetry too — a second close here would be the JFR-stream leak Telemetry.close guards
    // against, from the other direction.
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

/** The broker, started once per forked JVM.
  *
  * A companion object rather than a field: munit constructs a fresh suite instance per test, so a `lazy val` on the
  * suite would start one broker per test rather than one per run. The image is the one ADR §3.10 pins — `apache/kafka`,
  * KRaft, no ZooKeeper and no Confluent image.
  */
object WolframIngestIT:

  val BootstrapEnv: String = "KAFKA_BOOTSTRAP_SERVERS"

  val Image: String = "apache/kafka:4.3.1"

  /** `Try`, so an unreachable Docker daemon skips the suite instead of failing every test in it with the same stack
    * trace. A missing *broker* is a defect; a missing Docker is a laptop.
    */
  private lazy val container: Option[KafkaContainer] =
    Option.when(sys.env.get(BootstrapEnv).forall(_.trim.isEmpty))(KafkaContainer(DockerImageName.parse(Image)))
      .flatMap(started => Try(started.start()).toOption.map(_ => started))

  lazy val bootstrapServers: Option[String] =
    sys.env.get(BootstrapEnv).filter(_.trim.nonEmpty).orElse(container.map(_.bootstrapServers))
