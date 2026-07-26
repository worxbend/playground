package io.kzonix.eventing

import java.time.Duration as JDuration
import java.util.UUID

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

import io.circe.Json
import io.kzonix.kernel.event.AttrValue
import io.kzonix.kernel.event.ContentType
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.EventId
import io.kzonix.kernel.event.EventType
import io.kzonix.kernel.event.Payload
import io.kzonix.kernel.event.Source
import io.kzonix.kernel.event.Subject
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
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

/** The wire contract against a **real broker**.
  *
  * Everything in `src/test` proves that this module's encoder and decoder are inverses. That is necessary and not
  * sufficient: the things that actually break in production are the ones a broker is in the middle of — header
  * survival across the wire protocol, a `null` value versus an empty one, key-based partitioning, and trace context
  * arriving intact on a different JVM. Those are only observable here.
  *
  * **Broker discovery.** The suite takes `KAFKA_BOOTSTRAP_SERVERS` from the environment and skips when it is absent,
  * so `IT/testFull` is green on a machine with no Docker. ADR §9.2 calls for Testcontainers with a shared lazy
  * singleton per forked JVM, and that is the intended shape — but `testcontainers-scala` is declared in
  * `project/Dependencies.scala` and wired to no module, so it is not on any classpath yet. Swapping it in is a change
  * to [[bootstrapServers]] and nothing else: every test below already takes the address as a parameter.
  */
class KafkaWireIT extends FunSuite:

  /** The single point a Testcontainers `KafkaContainer` replaces. */
  private def bootstrapServers: Option[String] =
    sys.env.get("KAFKA_BOOTSTRAP_SERVERS").filter(_.trim.nonEmpty)

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

  private def withBroker(body: String => Unit): Unit =
    bootstrapServers match
      case Some(servers) => body(servers)
      case None          =>
        assume(false, "KAFKA_BOOTSTRAP_SERVERS is not set; skipping the broker round trip")

  private def producerContext: Context =
    val spanContext = SpanContext.createFromRemoteParent(traceId, spanId, TraceFlags.getSampled, TraceState.getDefault)
    Context.root().`with`(Span.wrap(spanContext))

  private def telemetry: Envelope =
    val built =
      for
        id        <- EventId(UUID.randomUUID().toString)
        source    <- Source("https://home.example/gateway/1")
        eventType <- EventType("io.kzonix.iot.telemetry")
        subject   <- Subject("kitchen-1")
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
            "value"  -> Json.fromDoubleOrNull(21.5),
            "unit"   -> Json.fromString("celsius")
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

  /** Polls until one record arrives or the deadline passes. Deliberately not a `while (records.isEmpty)` with no
    * bound: a broken assertion should fail the suite, not hang CI.
    */
  private def consumeOne(servers: String, topic: String): ConsumerRecord[String, Array[Byte]] =
    val config = Map[String, AnyRef](
      "bootstrap.servers"  -> servers,
      "group.id"           -> s"eventing-it-${UUID.randomUUID()}",
      "auto.offset.reset"  -> "earliest",
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
