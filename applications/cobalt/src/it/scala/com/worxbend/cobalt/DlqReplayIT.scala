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
import com.worxbend.eventing.DecodeFailure
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
import com.worxbend.persistence.repository.PostgresEventRepository
import io.circe.HCursor
import io.circe.Json
import io.circe.parser
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.Subscriptions
import org.apache.pekko.kafka.scaladsl.Committer
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** The dead-letter round trip, against a real broker and a real PostgreSQL.
  *
  * **A replay tool that has never replayed anything is not a tool.** Everything in `src/test` proves the *decisions* —
  * selection, bounding, refusal, the reconstructed record's shape — and none of it can prove the two things that only
  * fail against a real cluster: that a record rebuilt from a dead letter is one the consumer will actually accept, and
  * that a record which fails again comes back with its replay budget intact so the loop terminates.
  *
  * Both directions are exercised here, because they are the two situations an operator meets. A dead letter caused by a
  * transient database failure replays into a row; a dead letter caused by a permanently malformed record replays into
  * another dead letter, one generation on, and is then refused.
  */
final class DlqReplayIT extends CobaltIT:

  private val at: OffsetDateTime = OffsetDateTime.of(2026, 7, 24, 8, 15, 0, 0, ZoneOffset.UTC)

  private def force[A](result: Either[String, A]): A = result.fold(reason => fail(reason), identity)

  private def envelope(id: String): Envelope =
    Envelope(
      id = force(EventId(id)),
      source = force(Source("urn:worxbend:it:gateway")),
      eventType = force(EventType("com.worxbend.it.telemetry")),
      time = Some(at),
      subject = Some(force(Subject("device-replay"))),
      dataContentType = Some(force(ContentType("application/json"))),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString("device-replay")))
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

  private def body(reply: AdminReply): HCursor =
    parser.parse(reply.body).fold(error => fail(s"the reply is not JSON: $error"), _.hcursor)

  /** The dead letter cobalt would write for a record the *database* refused — a good event that could not be stored.
    *
    * Built rather than provoked, because provoking it means breaking PostgreSQL mid-test. The shape is identical:
    * `BatchProcessor` calls `DeadLetter.of` with the same original record and `reason=unpersistable`.
    */
  private def unpersistable(topic: String, envelope: Envelope, offset: Long): DeadLetter =
    val produced = force(KafkaCodecs.producerRecord(topic, envelope, ContentMode.Binary))
    val consumed = ConsumerRecord(
      topic,
      0,
      offset,
      at.toInstant.toEpochMilli,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      produced.key,
      produced.value,
      produced.headers,
      Optional.empty[java.lang.Integer]
    )
    DeadLetter.of(consumed, DecodeFailure.Unconvertible("the database refused it"), CobaltApp.DeadLetterSource, at)

  test("a dead letter is listed, planned without publishing, and then replayed into a row"):
    truncateEvents()
    val topic = newTopic("cobalt-replay")
    val dlq = newTopic("cobalt-replay-dlq")
    val group = s"cobalt-it-${java.util.UUID.randomUUID()}"
    val config = consumerConfig(topic, dlq, group)
    val replayConfig = ReplayConfig(enabled = true, maxRecords = 20, maxAttempts = 3, 5.seconds)

    val telemetry = Telemetry.start(TelemetryConfig("cobalt-it", "0.0.0-it", "it"), Tracing.noop)
    given system: ActorSystem = ActorSystem("cobalt-replay-it")
    val publisher = KafkaDeadLetterPublisher.start(config)
    val store = KafkaDeadLetterStore.start(config, replayConfig)
    val admin = DeadLetterAdmin(store, ReplayMetrics(telemetry.registry), replayConfig, topic, dlq)
    try
      // A good event that the database refused, dead-lettered exactly as BatchProcessor would have done.
      val event = envelope("evt-replay-1")
      await(publisher.publish(unpersistable(topic, event, offset = 7L)))

      eventually("the dead letter to reach the DLQ")(store.depth().outstanding == 1L)

      val listed = body(admin.records(10, ""))
      assertEquals(listed.get[Int]("returned").toOption, Some(1))
      val entry = listed.downField("records").downArray
      assertEquals(entry.downField("event").get[String]("id").toOption, Some("evt-replay-1"))
      assertEquals(entry.get[String]("reason").toOption, Some("unconvertible"))
      assertEquals(entry.downField("origin").get[Long]("offset").toOption, Some(7L))
      assertEquals(entry.get[Int]("replayAttempts").toOption, Some(0))
      val ref = entry.get[String]("ref").toOption.getOrElse(fail("the listing must give a replayable ref"))

      // The consumer, running against the main topic, so a replayed record is picked up the way a live one is.
      val repository = PostgresEventRepository(database.read.transactor, database.write.transactor)
      val processing = ConsumerStream.processing(
        decode = RecordDecoder(CobaltApp.DeadLetterSource, Tracing.noop.tracer).decode,
        processor = BatchProcessor(
          repository = repository,
          deadLetters = publisher,
          metrics = ConsumerMetrics(telemetry.registry),
          source = CobaltApp.DeadLetterSource,
          attempts = config.writeAttempts,
          backoff = () => Future.unit
        ),
        batchSize = config.batchSize,
        batchWindow = config.batchWindow,
        committer = Committer.flow(EventConsumer.committerSettings(config))
      )
      val (control, completion) = ConsumerStream
        .attempt(EventConsumer.consumerSettings(config), Subscriptions.topics(topic), processing)
        .toMat(Sink.ignore)(Keep.both)
        .run()
      try
        // The dry run must be exact and must publish nothing: an operator who cannot see what a replay would do will
        // not run one during an incident.
        val dry = body(admin.replay(10, "", ref, dryRun = true))
        assertEquals(dry.get[Boolean]("committed").toOption, Some(false))
        assertEquals(dry.get[Int]("replayable").toOption, Some(1))
        assertEquals(dry.downField("plan").downArray.get[Int]("attempt").toOption, Some(1))
        assertEquals(countById("evt-replay-1"), 0L, "a dry run must not put anything on the topic")

        val committed = body(admin.replay(10, "", ref, dryRun = false))
        assertEquals(committed.get[Int]("published").toOption, Some(1))

        eventually("the replayed event to be persisted")(countById("evt-replay-1") == 1L)

        // The second replay is the property the whole design rests on: the CloudEvents id survived the round trip, so
        // ON CONFLICT DO NOTHING absorbs it and the row count does not move.
        assertEquals(body(admin.replay(10, "", ref, dryRun = false)).get[Int]("published").toOption, Some(1))
        eventually("the duplicate to be absorbed")(countById("evt-replay-1") == 1L)
        assertEquals(countById("evt-replay-1"), 1L, "replaying an event that DID land must be a no-op at the database")
      finally
        val _ = await(control.drainAndShutdown(completion))
    finally
      store.close()
      publisher.close()
      val _ = await(system.terminate())
      telemetry.close()

  test("a record that fails again comes back one generation on, and is then refused"):
    val topic = newTopic("cobalt-poison")
    val dlq = newTopic("cobalt-poison-dlq")
    val group = s"cobalt-it-${java.util.UUID.randomUUID()}"
    val config = consumerConfig(topic, dlq, group)
    // maxAttempts = 1, so exactly one generation is allowed and the bound is reached inside one test.
    val replayConfig = ReplayConfig(enabled = true, maxRecords = 20, maxAttempts = 1, 5.seconds)

    val telemetry = Telemetry.start(TelemetryConfig("cobalt-it", "0.0.0-it", "it"), Tracing.noop)
    given system: ActorSystem = ActorSystem("cobalt-poison-it")
    val publisher = KafkaDeadLetterPublisher.start(config)
    val store = KafkaDeadLetterStore.start(config, replayConfig)
    val admin = DeadLetterAdmin(store, ReplayMetrics(telemetry.registry), replayConfig, topic, dlq)
    try
      val processing = ConsumerStream.processing(
        decode = RecordDecoder(CobaltApp.DeadLetterSource, Tracing.noop.tracer).decode,
        processor = BatchProcessor(
          repository = PostgresEventRepository(database.read.transactor, database.write.transactor),
          deadLetters = publisher,
          metrics = ConsumerMetrics(telemetry.registry),
          source = CobaltApp.DeadLetterSource,
          attempts = config.writeAttempts,
          backoff = () => Future.unit
        ),
        batchSize = config.batchSize,
        batchWindow = config.batchWindow,
        committer = Committer.flow(EventConsumer.committerSettings(config))
      )
      val (control, completion) = ConsumerStream
        .attempt(EventConsumer.consumerSettings(config), Subscriptions.topics(topic), processing)
        .toMat(Sink.ignore)(Keep.both)
        .run()
      try
        publish(Vector(ProducerRecord[String, Array[Byte]](topic, "poison", Array[Byte](3, 1, 4))))
        eventually("the poison record to be dead-lettered")(store.depth().outstanding == 1L)

        val first = store.recent(10).headOption.getOrElse(fail("the dead letter must be listed"))
        assertEquals(first.entry.map(_.reason), Right("unknown-encoding"))
        assertEquals(first.entry.map(letter => ReplayHeaders.attemptsOf(letter.headers)), Right(Right(0)))

        assertEquals(body(admin.replay(10, "", first.ref, dryRun = false)).get[Int]("published").toOption, Some(1))

        // The replayed record is still poison, so it comes back — under a NEW key, because its origin coordinates are
        // the coordinates of the record that was just produced. Nothing in Kafka bounds that; the header counter does.
        eventually("the replayed record to be dead-lettered again")(store.depth().outstanding == 2L)
        val second = store
          .recent(10)
          .find(record => record.entry.exists(letter => ReplayHeaders.attemptsOf(letter.headers) == Right(1)))
          .getOrElse(fail("the second generation must record that it has been replayed once"))
        assertNotEquals(second.ref, first.ref, "a replayed record returns under new origin coordinates")
        assertEquals(
          second.entry.toOption.flatMap(_.headers.get(ReplayHeaders.Of)),
          Some(first.entry.map(_.origin.dlqKey).getOrElse("")),
          "each generation must name its predecessor, so a loop reads as a chain"
        )

        // Budget spent. Naming it explicitly refuses the whole operation rather than quietly skipping it.
        val refused = admin.replay(10, "", second.ref, dryRun = false)
        assertEquals(refused.status, 422)
        assert(body(refused).get[String]("error").toOption.exists(_.contains("budget-exhausted")), refused.body)
        assertEquals(store.depth().outstanding, 2L, "a refused replay must publish nothing")
      finally
        val _ = await(control.drainAndShutdown(completion))
    finally
      store.close()
      publisher.close()
      val _ = await(system.terminate())
      telemetry.close()
