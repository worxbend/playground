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

import com.worxbend.eventing.ContentMode
import com.worxbend.eventing.DeadLetter
import com.worxbend.eventing.KafkaCodecs
import com.worxbend.kernel.event.ContentType
import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.EventId
import com.worxbend.kernel.event.EventType
import com.worxbend.kernel.event.Payload
import com.worxbend.kernel.event.Source
import com.worxbend.kernel.event.Subject
import com.worxbend.observability.Telemetry
import com.worxbend.observability.TelemetryConfig
import com.worxbend.observability.Tracing
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.PostgresEventRepository
import io.circe.Json
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.Subscriptions
import org.apache.pekko.kafka.scaladsl.Committer
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** The at-least-once contract, end to end, against a real broker and a real PostgreSQL.
  *
  * Everything in `src/test` proves the graph's *shape*: batching, ordering, isolation. What only this tier can prove is
  * that the shape survives the two systems it was designed around — that a record produced in binary mode is decodable
  * on the other side of the wire, that the row lands in the partitioned fact table with its generated columns
  * populated, and above all that **replaying the same record is a no-op**, which is the property the whole design of
  * ADR §4.3 exists to buy: at-least-once delivery plus `(occurred_at, ce_source, ce_id)` uniqueness equals
  * observationally exactly-once at the database.
  */
final class CobaltIngestIT extends CobaltIT:

  private val at: OffsetDateTime = OffsetDateTime.of(2026, 7, 22, 9, 30, 0, 0, ZoneOffset.UTC)

  private def force[A](result: Either[String, A]): A = result.fold(reason => fail(reason), identity)

  private def envelope(id: String): Envelope =
    Envelope(
      id = force(EventId(id)),
      source = force(Source("urn:worxbend:it:gateway")),
      eventType = force(EventType("com.worxbend.it.telemetry")),
      time = Some(at),
      subject = Some(force(Subject("device-it"))),
      dataContentType = Some(force(ContentType("application/json"))),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(
        Json.obj(
          "deviceId" -> Json.fromString("device-it"),
          "value" -> Json.fromDoubleOrNull(21.5d),
          "unit" -> Json.fromString("celsius")
        )
      )
    )

  private def consumerConfig(topic: String, dlq: String, group: String): ConsumerConfig =
    ConsumerConfig(
      bootstrapServers = servers,
      topic = topic,
      dlqTopic = dlq,
      groupId = group,
      batchSize = 50,
      batchWindow = 200.millis,
      writeAttempts = 2,
      retryDelay = 50.millis,
      commitMaxBatch = 10L,
      commitMaxInterval = 1.second,
      commitParallelism = 1,
      drainTimeout = 10.seconds,
      properties = Map.empty
    )

  test("a produced CloudEvent lands as a row, and replaying it is a no-op"):
    truncateEvents()
    val topic = newTopic("cobalt-ingest")
    val dlq = newTopic("cobalt-ingest-dlq")
    val group = s"cobalt-it-${java.util.UUID.randomUUID()}"
    val config = consumerConfig(topic, dlq, group)

    val telemetry = Telemetry.start(TelemetryConfig("cobalt-it", "0.0.0-it", "it"), Tracing.noop)
    given system: ActorSystem = ActorSystem("cobalt-ingest-it")
    val deadLetters = KafkaDeadLetterPublisher.start(config)
    try
      val repository = PostgresEventRepository(database.read.transactor, database.write.transactor)
      val processor = BatchProcessor(
        repository = repository,
        deadLetters = deadLetters,
        metrics = ConsumerMetrics(telemetry.registry),
        source = CobaltApp.DeadLetterSource,
        attempts = config.writeAttempts,
        backoff = () => Future.unit
      )
      val decoder = RecordDecoder(CobaltApp.DeadLetterSource, telemetry.tracing.tracer)
      val processing = ConsumerStream.processing(
        decode = decoder.decode,
        processor = processor,
        batchSize = config.batchSize,
        batchWindow = config.batchWindow,
        committer = Committer.flow(EventConsumer.committerSettings(config))
      )

      // A record, then the *same* record again: at-least-once redelivery, provoked deliberately rather than waited for.
      val record = force(KafkaCodecs.producerRecord(topic, envelope("evt-it-1"), ContentMode.Binary))
      publish(Vector(record, record))
      // A record nothing can decode, to prove the stream does not wedge on it.
      publish(Vector(ProducerRecord[String, Array[Byte]](topic, "junk", Array[Byte](3, 1, 4))))
      publish(Vector(force(KafkaCodecs.producerRecord(topic, envelope("evt-it-2"), ContentMode.Binary))))

      val (control, completion) = ConsumerStream
        .attempt(EventConsumer.consumerSettings(config), Subscriptions.topics(topic), processing)
        .toMat(Sink.ignore)(Keep.both)
        .run()
      try
        eventually("both good events to be persisted")(countById("evt-it-1") == 1L && countById("evt-it-2") == 1L)
        assertEquals(countById("evt-it-1"), 1L, "the duplicate must be absorbed by the identity index, not stored")
      finally
        val _ = await(control.drainAndShutdown(completion))
    finally
      deadLetters.close()
      val _ = await(system.terminate())
      telemetry.close()

  test("the repository absorbs a replayed batch without writing a second row"):
    truncateEvents()
    val repository = PostgresEventRepository(database.read.transactor, database.write.transactor)
    val events = Vector(force(NewEvent.from(envelope("evt-replay"))))
    assertEquals(await(repository.insertAll(events)), 1L)
    assertEquals(await(repository.insertAll(events)), 0L, "ON CONFLICT DO NOTHING is what makes redelivery free")
    assertEquals(countById("evt-replay"), 1L)

  test("the consumer control captured by the restart wrapper is the live one after a failure"):
    val topic = newTopic("cobalt-restart")
    val group = s"cobalt-it-${java.util.UUID.randomUUID()}"
    val config = consumerConfig(topic, topic + "-dlq", group)
    given system: ActorSystem = ActorSystem("cobalt-restart-it")
    try
      val captured = AtomicReference[Consumer.Control](Consumer.NoopControl)
      val processing = ConsumerStream.processing(
        decode = RecordDecoder(CobaltApp.DeadLetterSource, Tracing.noop.tracer).decode,
        processor = BatchProcessor(
          repository = PostgresEventRepository(database.read.transactor, database.write.transactor),
          deadLetters = NoopDeadLetters,
          metrics = ConsumerMetrics(Telemetry.start(TelemetryConfig("x", "0", "0"), Tracing.noop).registry),
          source = CobaltApp.DeadLetterSource,
          attempts = 1,
          backoff = () => Future.unit
        ),
        batchSize = config.batchSize,
        batchWindow = config.batchWindow,
        committer = Committer.flow(EventConsumer.committerSettings(config))
      )
      val settings = EventConsumer.consumerSettings(config)
      val attempt = () => ConsumerStream.attempt(settings, Subscriptions.topics(topic), processing)
      val restarted = ConsumerStream.restarting(ConsumerStream.restartSettings(restartConfig), captured)(attempt)
      val completion = restarted.runWith(Sink.ignore)
      eventually("the consumer control to be captured")(captured.get() != Consumer.NoopControl)
      val _ = await(captured.get().shutdown())
      val _ = await(completion)
    finally
      val _ = await(system.terminate())

  private val restartConfig: RestartConfig = RestartConfig(50.millis, 200.millis, 0.0d, 3, 1.minute)

  private object NoopDeadLetters extends DeadLetterPublisher:
    def publish(deadLetter: DeadLetter): Future[Unit] = Future.unit
    def close(): Unit = ()
