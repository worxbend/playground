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

import com.augustnagro.magnum.DbTx
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
import com.worxbend.kernel.event.Topics
import com.worxbend.kernel.search.Filter
import com.worxbend.observability.Telemetry
import com.worxbend.observability.TelemetryConfig
import com.worxbend.observability.Tracing
import com.worxbend.persistence.repository.CheckpointWrite
import com.worxbend.persistence.repository.EventDetail
import com.worxbend.persistence.repository.EventRef
import com.worxbend.persistence.repository.EventRepository
import com.worxbend.persistence.repository.FacetRequest
import com.worxbend.persistence.repository.Facets
import com.worxbend.persistence.repository.HistogramBucket
import com.worxbend.persistence.repository.HistogramRequest
import com.worxbend.persistence.repository.NewEvent
import com.worxbend.persistence.repository.SearchPage
import com.worxbend.persistence.repository.SearchRequest
import io.circe.Json
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.pekko.kafka.ConsumerMessage.Committable
import org.apache.pekko.kafka.ConsumerMessage.CommittableMessage
import org.apache.pekko.kafka.ConsumerMessage.CommittableOffset
import org.apache.pekko.kafka.testkit.ConsumerResultFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** Hand-built doubles for everything the consumer talks to.
  *
  * Deliberately not a mocking framework: every assertion in these suites is about *ordering* — which record went where,
  * and whether a commit happened after its write — and a recorded sequence of plain values cannot lie about the order
  * it was appended in the way a verification DSL can.
  */
object Fixtures:

  val Topic: String = Topics.CloudEvents

  val GroupId: String = "cobalt-test"

  /** Test-only escape hatch for the `Either`-returning smart constructors. Throwing is right here: a fixture that built
    * an invalid value would be a bug in the fixture, not a case under test.
    */
  def force[A](result: Either[String, A]): A =
    result.fold(message => throw IllegalArgumentException(message), identity)

  val source: Source = force(Source("urn:worxbend:test"))

  val at: OffsetDateTime = OffsetDateTime.of(2026, 7, 20, 12, 0, 0, 0, ZoneOffset.UTC)

  /** A minimal, spec-valid envelope. `time` is present by default; its absence is a separate case under test. */
  def envelope(id: String, subject: String = "device-1", time: Option[OffsetDateTime] = Some(at)): Envelope =
    Envelope(
      id = force(EventId(id)),
      source = force(Source("urn:worxbend:test:gateway")),
      eventType = force(EventType("com.worxbend.test.telemetry")),
      time = time,
      subject = Some(force(Subject(subject))),
      dataContentType = Some(force(ContentType("application/json"))),
      schema = None,
      extensions = Map.empty,
      payload = Payload.Structured(Json.obj("deviceId" -> Json.fromString(subject)))
    )

  /** Encodes an envelope exactly as wolfram publishes it (binary mode) and hands it back as a consumed record. */
  def record(envelope: Envelope, partition: Int = 0, offset: Long = 0L): ConsumerRecord[String, Array[Byte]] =
    val produced = force(KafkaCodecs.producerRecord(Topic, envelope, ContentMode.Binary))
    consumerRecord(produced.key, produced.value, produced.headers, partition, offset)

  /** A record no content mode can read: no `ce_*` headers, no structured content type, arbitrary bytes. */
  def malformedRecord(partition: Int = 0, offset: Long = 0L): ConsumerRecord[String, Array[Byte]] =
    consumerRecord("junk", Array[Byte](1, 2, 3), RecordHeaders(), partition, offset)

  def consumerRecord(
    key: String,
    value: Array[Byte],
    headers: Headers,
    partition: Int,
    offset: Long
  ): ConsumerRecord[String, Array[Byte]] =
    ConsumerRecord(
      Topic,
      partition,
      offset,
      at.toInstant.toEpochMilli,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      key,
      value,
      headers,
      Optional.empty[java.lang.Integer]
    )

  /** A real `CommittableOffset` from the connector's own testkit.
    *
    * `ConsumerMessage.CommittableOffset` is **sealed**, so a hand-written recording double is not an option — the
    * testkit's factory is the only way to obtain one outside the connector. Its committer is a stub, so nothing here
    * may actually call `commit`; commit *ordering* is instead observed by the committer flow the graph is given (see
    * [[ConsumerStreamSuite]]), which is a better place to observe it anyway: that flow is the production seam.
    */
  def offsetFor(partition: Int, offset: Long): CommittableOffset =
    ConsumerResultFactory.committableOffset(GroupId, Topic, partition, offset, "")

  /** The offset an arbitrary `Committable` carries, or `-1` for a shape that has none. Used by tests to label a commit
    * without reaching into the connector's internals.
    */
  def offsetOf(committable: Committable): Long = committable match
    case offset: CommittableOffset => offset.partitionOffset.offset
    case _                         => -1L

  def committableMessage(
    record: ConsumerRecord[String, Array[Byte]],
    offset: CommittableOffset
  ): CommittableMessage[String, Array[Byte]] =
    CommittableMessage(record, offset)

  /** A dead letter for `record`, exactly as [[RecordDecoder]] would build one. */
  def deadLetter(
    record: ConsumerRecord[String, Array[Byte]],
    failure: DecodeFailure = DecodeFailure.MalformedBinary("the headers are not a CloudEvent")
  ): DeadLetter =
    DeadLetter.of(record, failure, source, at)

  /** One dead letter as it would sit on the DLQ topic, keyed the way `KafkaCodecs.deadLetterRecord` keys it. */
  def dlqRecord(letter: DeadLetter, partition: Int = 0, offset: Long = 0L, timestamp: Long = 0L): DlqRecord =
    DlqRecord(partition, offset, Some(timestamp), Some(letter.origin.dlqKey), Right(letter))

  /** A DLQ record nothing can read — a foreign writer's, or an older build's. */
  def unreadableDlqRecord(partition: Int = 0, offset: Long = 0L): DlqRecord =
    DlqRecord(partition, offset, Some(0L), Some("someone-elses-key"), Left("not a dead letter"))

  /** A [[DeadLetterStore]] over a fixed set of records, which records what was published and can be told to refuse.
    *
    * Hand-written rather than mocked for the same reason [[RecordingDeadLetters]] is: the assertions are about *order*
    * and about exactly where a partial failure stopped, and a recorded sequence of plain values cannot misreport
    * either.
    */
  final class StubDeadLetterStore(
    val records: Vector[DlqRecord] = Vector.empty,
    val partitions: Vector[DlqPartitionDepth] = Vector.empty,
    failFrom: Int = Int.MaxValue,
    unreachable: Boolean = false
  ) extends DeadLetterStore:

    private val sent: ConcurrentLinkedQueue[ProducerRecord[String, Array[Byte]]] =
      ConcurrentLinkedQueue[ProducerRecord[String, Array[Byte]]]()

    /** The `limit` each `recent` call was asked for — the only way to assert that the bound reached the broker. */
    val limits: ConcurrentLinkedQueue[Int] = ConcurrentLinkedQueue[Int]()

    def depth(): DlqDepth =
      if unreachable then throw IllegalStateException("the broker is unreachable")
      else DlqDepth(Topics.CloudEventsDlq, partitions)

    def recent(limit: Int): Vector[DlqRecord] =
      if unreachable then throw IllegalStateException("the broker is unreachable")
      val _ = limits.add(limit)
      records.take(limit)

    def publish(record: ProducerRecord[String, Array[Byte]]): Unit =
      if sent.size >= failFrom then throw IllegalStateException("the broker refused the record")
      val _ = sent.add(record)

    def close(): Unit = ()

    def published: Vector[ProducerRecord[String, Array[Byte]]] = sent.asScala.toVector

  /** Records every dead letter, and can be told to fail — a DLQ that cannot accept a record must not let the offset
    * move past it, and that is a property worth being able to provoke.
    */
  final class RecordingDeadLetters(fail: Boolean = false) extends DeadLetterPublisher:

    private val queue: ConcurrentLinkedQueue[DeadLetter] = ConcurrentLinkedQueue[DeadLetter]()

    def publish(deadLetter: DeadLetter): Future[Unit] =
      if fail then Future.failed(IllegalStateException("the DLQ is unavailable"))
      else
        val _ = queue.add(deadLetter)
        Future.unit

    def close(): Unit = ()

    def published: Vector[DeadLetter] = queue.asScala.toVector

  /** An [[EventRepository]] whose only implemented method is the one cobalt uses.
    *
    * The read half throws rather than answering politely: cobalt calling `search` would be a layering mistake, and a
    * stub that returned an empty page would hide it.
    */
  abstract class WriteOnlyRepository extends EventRepository:
    private def unused: Nothing = throw UnsupportedOperationException("cobalt does not read")
    def search(request: SearchRequest): Future[SearchPage] = unused
    def facets(request: FacetRequest): Future[Facets] = unused
    def histogram(request: HistogramRequest): Future[Vector[HistogramBucket]] = unused
    def find(ref: EventRef): Future[Option[EventDetail]] = unused
    def countAtMost(filter: Option[Filter], cap: Int): Future[Long] = unused

  /** Records every batch it was handed and answers with `respond`. */
  final class RecordingRepository(respond: Vector[NewEvent] => Future[Long]) extends WriteOnlyRepository:

    private val queue: ConcurrentLinkedQueue[Vector[NewEvent]] = ConcurrentLinkedQueue[Vector[NewEvent]]()
    val calls: AtomicInteger = AtomicInteger(0)

    def insertAll(events: Vector[NewEvent]): Future[Long] =
      val _ = queue.add(events)
      val _ = calls.incrementAndGet()
      respond(events)

    def batches: Vector[Vector[NewEvent]] = queue.asScala.toVector

  /** A `Telemetry` with a no-op tracer and a real Prometheus registry, so meters can be asserted on without exporting
    * anything anywhere.
    */
  def telemetry(): Telemetry =
    Telemetry.start(TelemetryConfig("cobalt-test", "0.0.0-test", "test"), Tracing.noop)

  /** A decoded record ready to write, at a chosen Kafka coordinate.
    *
    * The coordinate is what the checkpoint tests are about, so it is the parameter; the payload is incidental and comes
    * from the standard envelope.
    */
  def pendingWrite(id: String, partition: Int, offset: Long): PendingWrite =
    val message = envelope(id)
    PendingWrite(
      record(message, partition = partition, offset = offset),
      message,
      com.worxbend.persistence.repository.NewEvent.from(message).fold(problem => sys.error(problem), identity)
    )

  /** A supervisor whose stream never actually starts, and whose Kafka and checkpoint lookups answer nothing.
    *
    * For suites that need a supervisor to *exist* rather than to run — the admin-path constants, the route wiring. The
    * lifecycle transitions themselves are covered in `SupervisorSuite` against the same seam, and against a real broker
    * in `CobaltIngestIT`.
    */
  def idleSupervisor(using ExecutionContext): ConsumerSupervisor =
    val handle = new ConsumerHandle:
      def drain(): Future[org.apache.pekko.Done] = Future.successful(org.apache.pekko.Done)
      def completion: Future[org.apache.pekko.Done] = Promise[org.apache.pekko.Done]().future
    val checkpoints = new com.worxbend.persistence.repository.CheckpointStore:
      def record(groupId: String, owner: Option[String], positions: Vector[CheckpointWrite])(using DbTx): Unit = ()
      def load(groupId: String): Future[Vector[com.worxbend.persistence.repository.Checkpoint]] =
        Future.successful(Vector.empty)
      def clear(groupId: String): Future[Int] = Future.successful(0)
    ConsumerSupervisor(
      factory = () => handle,
      offsets = AdminOffsets(
        org.apache.kafka.clients.admin.Admin.create(java.util.Map.of("bootstrap.servers", "127.0.0.1:1")),
        1.second
      ),
      checkpoints = checkpoints,
      groupId = "test-group",
      topic = Topic
    )
