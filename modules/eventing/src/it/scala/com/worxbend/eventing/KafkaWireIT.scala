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

package com.worxbend.eventing

import com.dimafeng.testcontainers.KafkaContainer
import com.worxbend.kernel.event.AttrValue
import com.worxbend.kernel.event.ContentType
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.kernel.event.Subject
import io.circe.Json
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import java.time.Duration as JDuration
import java.util.UUID
import munit.FunSuite
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.utility.DockerImageName
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

/** The wire contract against a **real broker**.
  *
  * Everything in `src/test` proves that this module's encoder and decoder are inverses. That is necessary and not
  * sufficient: the things that actually break in production are the ones a broker is in the middle of — header survival
  * across the wire protocol, a `null` value versus an empty one, key-based partitioning, and trace context arriving
  * intact on a different JVM. Those are only observable here.
  *
  * **Broker discovery.** ADR §9.2 calls for Testcontainers behind a shared lazy singleton per forked JVM, which is what
  * [[KafkaWireIT.bootstrapServers]] provides: `IT / fork := true` gives this module its own JVM, `IT /
  * parallelExecution := false` means nothing races for the broker, and Ryuk reaps the container when the JVM exits.
  * `KAFKA_BOOTSTRAP_SERVERS` still wins when set, so a CI job that already runs a broker can point the suite at it.
  */
class KafkaWireIT extends FunSuite:

  private def bootstrapServers: String = KafkaWireIT.bootstrapServers

  private val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  private val spanId = "00f067aa0ba902b7"

  test("a binary record survives a real broker with its headers, key and payload intact"):
    withBroker: servers =>
      val topic = newTopic(servers, "eventing-binary")
      val envelope = telemetry

      val record = KafkaCodecs
        .producerRecord(topic, envelope)
        .getOrElse(fail("the envelope should be encodable"))
      val _ = KafkaTrace.inject(record, producerContext)
      publish(servers, record)

      val consumed = consumeOne(servers, topic)
      assertEquals(KafkaCodecs.decode(consumed), Right(CloudEventAdapter.binaryCanonical(envelope)))
      assertEquals(consumed.key, envelope.partitionKey)
      assertEquals(ContentMode.of(consumed.headers), Some(ContentMode.Binary))
      assertEquals(CloudEventHeaders.get(consumed.headers, "ce_type"), Some(envelope.eventType: String))
      assertEquals(CloudEventHeaders.get(consumed.headers, "ce_source"), Some(envelope.source: String))

  test("the trace context survives the broker, so producer and consumer share one trace"):
    withBroker: servers =>
      val topic = newTopic(servers, "eventing-trace")
      val record = KafkaCodecs
        .producerRecord(topic, telemetry)
        .getOrElse(fail("the envelope should be encodable"))
      val _ = KafkaTrace.inject(record, producerContext)
      publish(servers, record)

      val consumed = consumeOne(servers, topic)
      val extracted = Span.fromContext(KafkaTrace.extract(consumed.headers)).getSpanContext
      assertEquals(extracted.getTraceId, traceId)
      assertEquals(extracted.getSpanId, spanId)
      assert(extracted.isRemote)

  test("a poison record is decoded into a dead letter and replayed through the DLQ unchanged"):
    withBroker: servers =>
      val topic = newTopic(servers, "eventing-poison")
      val dlq = newTopic(servers, "eventing-poison-dlq")

      publish(
        servers,
        ProducerRecord[String, Array[Byte]](topic, "device-1", Array[Byte](3, 1, 4, 1, 5))
      )
      val consumed = consumeOne(servers, topic)
      val outcome = KafkaCodecs.decodeOrDeadLetter(consumed)
      assert(outcome.isLeft, outcome)
      val deadLetter = outcome.swap.getOrElse(fail("expected a dead letter"))
      assertEquals(deadLetter.reason, "unknown-encoding")
      assertEquals(deadLetter.origin.topic, topic)

      publish(
        servers,
        KafkaCodecs.deadLetterRecord(deadLetter, dlq).getOrElse(fail("the dead letter should be encodable"))
      )
      val fromDlq = consumeOne(servers, dlq)
      assertEquals(ContentMode.of(fromDlq.headers), Some(ContentMode.Structured))
      assertEquals(fromDlq.key, deadLetter.origin.dlqKey)
      assertEquals(
        KafkaCodecs.decode(fromDlq).flatMap(DeadLetter.fromEnvelope(_).left.map(DecodeFailure.Unconvertible.apply)),
        Right(deadLetter)
      )

  test("an event with no data crosses the broker as a null value, not as zero bytes"):
    withBroker: servers =>
      val topic = newTopic(servers, "eventing-nodata")
      val envelope = telemetry.copy(dataContentType = None, payload = Payload.Empty)
      publish(
        servers,
        KafkaCodecs.producerRecord(topic, envelope).getOrElse(fail("the envelope should be encodable"))
      )
      val consumed = consumeOne(servers, topic)
      assertEquals(Option(consumed.value), None)
      assertEquals(KafkaCodecs.decode(consumed).map(_.payload), Right(Payload.Empty))

  test("structured mode crosses the broker as one self-contained JSON value"):
    withBroker: servers =>
      val topic = newTopic(servers, "eventing-structured")
      val envelope = telemetry
      publish(
        servers,
        KafkaCodecs
          .producerRecord(topic, envelope, ContentMode.Structured)
          .getOrElse(fail("the envelope should be encodable"))
      )
      val consumed = consumeOne(servers, topic)
      assertEquals(ContentMode.of(consumed.headers), Some(ContentMode.Structured))
      assertEquals(KafkaCodecs.decode(consumed), Right(envelope))

  // -------------------------------------------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------------------------------------------

  private def withBroker(body: String => Unit): Unit = body(bootstrapServers)

  private def producerContext: Context =
    val spanContext = SpanContext.createFromRemoteParent(traceId, spanId, TraceFlags.getSampled, TraceState.getDefault)
    Context.root().`with`(Span.wrap(spanContext))

  private def telemetry: Envelope =
    val built =
      for
        id <- EventId(UUID.randomUUID().toString)
        source <- Source("https://home.example/gateway/1")
        eventType <- EventType("com.worxbend.iot.telemetry")
        subject <- Subject("kitchen-1")
        mediaType <- ContentType("application/json")
      yield Envelope(
        id = id,
        source = source,
        eventType = eventType,
        time = Some(java.time.OffsetDateTime.parse("2024-01-01T17:31:00+02:00")),
        subject = Some(subject),
        dataContentType = Some(mediaType),
        schema = None,
        extensions = Map("tenantid" -> AttrValue.Text("acme"), "sequence" -> AttrValue.Num(7)),
        payload = Payload.Structured(
          Json.obj(
            "metric" -> Json.fromString("temperature"),
            "value" -> Json.fromDoubleOrNull(21.5),
            "unit" -> Json.fromString("celsius")
          )
        )
      )
    built.fold(message => fail(message), identity)

  /** A topic name unique to this run, created and waited for: an auto-created topic has one partition and different
    * defaults, which would quietly weaken every assertion below.
    */
  private def newTopic(servers: String, prefix: String): String =
    val name = s"$prefix-${UUID.randomUUID()}"
    val admin = Admin.create(Map[String, AnyRef]("bootstrap.servers" -> servers).asJava)
    try
      val _ = admin.createTopics(List(NewTopic(name, 3, 1.toShort)).asJava).all().get()
      name
    finally admin.close()

  private def publish(servers: String, record: ProducerRecord[String, Array[Byte]]): Unit =
    val producer = KafkaProducer[String, Array[Byte]](
      KafkaCodecs.producerConfig(Map("bootstrap.servers" -> servers)),
      StringSerializer(),
      ByteArraySerializer()
    )
    try
      val _ = producer.send(record).get()
      producer.flush()
    finally producer.close()

  /** Polls until one record arrives or the deadline passes. Deliberately not a `while (records.isEmpty)` with no bound:
    * a broken assertion should fail the suite, not hang CI.
    */
  private def consumeOne(servers: String, topic: String): ConsumerRecord[String, Array[Byte]] =
    val config = Map[String, AnyRef](
      "bootstrap.servers" -> servers,
      "group.id" -> s"eventing-it-${UUID.randomUUID()}",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false"
    )
    val consumer = KafkaConsumer[String, Array[Byte]](config.asJava, StringDeserializer(), ByteArrayDeserializer())
    try
      consumer.subscribe(List(topic).asJava)
      poll(consumer, System.nanoTime() + JDuration.ofSeconds(30).toNanos)
    finally consumer.close()

  @tailrec
  private def poll(
    consumer: KafkaConsumer[String, Array[Byte]],
    deadlineNanos: Long
  ): ConsumerRecord[String, Array[Byte]] =
    if System.nanoTime() > deadlineNanos then fail("no record arrived within the deadline")
    else
      val records = consumer.poll(JDuration.ofMillis(500)).iterator.asScala.toVector
      records.headOption match
        case Some(record) => record
        case None         => poll(consumer, deadlineNanos)

/** The broker, started once per forked JVM.
  *
  * A companion object rather than a field: a `lazy val` on the suite instance would start one broker per *test*, since
  * munit constructs a fresh suite instance for each. The image is the one ADR §3.10 pins — `apache/kafka`, KRaft, no
  * ZooKeeper and no Confluent image.
  */
object KafkaWireIT:

  val Image: String = "apache/kafka:4.3.1"

  private lazy val container: Option[KafkaContainer] =
    Option.when(sys.env.get("KAFKA_BOOTSTRAP_SERVERS").forall(_.trim.isEmpty)):
      val started = KafkaContainer(DockerImageName.parse(Image))
      started.start()
      started

  lazy val bootstrapServers: String =
    sys.env
      .get("KAFKA_BOOTSTRAP_SERVERS")
      .filter(_.trim.nonEmpty)
      .getOrElse(container.map(_.bootstrapServers).getOrElse(sys.error("no broker")))
