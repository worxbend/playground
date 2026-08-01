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

import com.typesafe.scalalogging.StrictLogging
import com.worxbend.eventing.DeadLetter
import com.worxbend.eventing.DecodeFailure
import com.worxbend.kernel.event.Source
import com.worxbend.observability.Meters
import com.worxbend.persistence.repository.CheckpointCommit
import com.worxbend.persistence.repository.CheckpointingWriter
import com.worxbend.persistence.repository.CheckpointWrite
import com.worxbend.persistence.repository.EventRepository
import java.sql.SQLException
import org.apache.pekko.kafka.ConsumerMessage.Committable
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

/** Turns one `groupedWithin` batch into a durable effect, and returns the offsets that effect earned.
  *
  * **This class is the at-least-once guarantee.** It returns `Committable`s, and it returns them only on the paths
  * where the batch is genuinely accounted for — every record either written to PostgreSQL, or published to the DLQ.
  * When neither is true the returned `Future` fails, the stream fails with it, and the offsets are never handed to the
  * committer. [[ConsumerStream]] then puts `Committer.flow` strictly downstream of this stage, so an offset is only
  * ever a receipt for work that already happened.
  *
  * **Poison isolation is a bisection, not a retry loop.** ADR §4.3 requires that one bad record cannot wedge the
  * stream. The naïve fix — dead-letter the whole batch on failure — discards up to `batchSize` perfectly good events
  * for one malformed one. So a failed batch is retried whole (a database blip is not a data problem), then split in
  * half and each half written independently, recursively, until the failure is attributed to a single record. That
  * record is dead-lettered and the batch proceeds. `log₂(500) ≈ 9` extra round trips is a cheap price for never losing
  * a good event and never stalling.
  *
  * **The classification in [[BatchProcessor.isDataError]] is what keeps the bisection safe.** Without it, a database
  * that is merely *down* would bisect to singletons and dead-letter the entire batch — converting a recoverable outage
  * into permanent data loss, which is strictly worse than stalling. Only a failure the database will deterministically
  * repeat (SQLSTATE class 22, data exception; class 23, integrity constraint) is allowed to dead-letter a record.
  * Everything else is rethrown so the `RestartSource` backs off and tries again with the offsets uncommitted.
  *
  * **No span is opened around the write.** A batch aggregates records from many unrelated traces, so it cannot be a
  * child of any one of them, and a span with `batchSize` links is not something any backend renders usefully. The
  * per-record CONSUMER span in [[RecordDecoder]] is where trace continuation lives; batch health is a metric
  * ([[ConsumerMetrics.batchWrite]]), which is the shape that question actually has.
  *
  * @param backoff
  *   the pause between whole-batch attempts, as a function so tests do not have to wait. Short on purpose: the real
  *   backoff for a sustained outage is the consumer's `RestartSource`, which also unwinds the Kafka session.
  */
final class BatchProcessor(
  repository: EventRepository,
  deadLetters: DeadLetterPublisher,
  metrics: ConsumerMetrics,
  source: Source,
  attempts: Int,
  backoff: () => Future[Unit],
  checkpoint: Option[BatchProcessor.Checkpointing] = None
)(using ec: ExecutionContext)
    extends StrictLogging:

  /** Processes one batch and yields the offsets it earned, in arrival order. */
  def process(batch: Vector[DecodedRecord]): Future[Vector[Committable]] =
    if batch.isEmpty then Future.successful(Vector.empty)
    else
      metrics.batchSize(batch.size)
      val (poison, pending) = batch.map(_.outcome).partitionMap(identity)
      for
        _ <- deadLetterAll(poison)
        _ <- write(pending)
      yield batch.map(_.committable)

  /** Publishes the records that could not be decoded at all.
    *
    * Sequential rather than parallel: a DLQ burst is by definition a bad moment for the pipeline, and firing an
    * unbounded fan-out of produces at a broker that may itself be the problem is how a poison batch becomes an outage.
    * Order is also preserved, which makes the DLQ readable.
    */
  private def deadLetterAll(poison: Vector[DeadLetter]): Future[Unit] =
    poison.foldLeft(Future.unit): (previous, deadLetter) =>
      previous.flatMap: _ =>
        logger.warn(s"dead-lettering ${deadLetter.origin.dlqKey}: ${deadLetter.reason} — ${deadLetter.detail}")
        deadLetters.publish(deadLetter).map: _ =>
          metrics.poison(deadLetter.reason)

  private def write(pending: Vector[PendingWrite]): Future[Unit] =
    if pending.isEmpty then Future.unit else attempt(pending, attempts)

  /** One whole-batch insert, retried while the failure still looks transient. */
  private def attempt(pending: Vector[PendingWrite], remaining: Int): Future[Unit] =
    insert(pending).recoverWith:
      case NonFatal(error) if remaining > 1 && !BatchProcessor.isDataError(error) =>
        logger.warn(s"batch insert of ${pending.size} records failed, retrying: ${RecordDecoder.describe(error)}")
        backoff().flatMap(_ => attempt(pending, remaining - 1))
      case NonFatal(error) => isolate(pending, error)

  /** Halve, or attribute. See the class Scaladoc for why this is not allowed to run on a transient failure. */
  private def isolate(pending: Vector[PendingWrite], error: Throwable): Future[Unit] =
    if pending.sizeIs > 1 then
      val (left, right) = pending.splitAt(pending.size / 2)
      logger.warn(s"isolating a poison record: splitting a failed batch of ${pending.size}")
      write(left).flatMap(_ => write(right))
    else if BatchProcessor.isDataError(error) then pending.headOption.fold(Future.unit)(unpersistable(_, error))
    else Future.failed(error)

  /** One record the database will never accept. Dead-lettered, counted, and its offset allowed to move past it. */
  private def unpersistable(pending: PendingWrite, error: Throwable): Future[Unit] =
    val detail = s"the database rejected this record: ${RecordDecoder.describe(error)}"
    val deadLetter = DeadLetter.of(pending.record, DecodeFailure.Unconvertible(detail), source)
    logger.error(s"record ${deadLetter.origin.dlqKey} is unpersistable and is being dead-lettered: $detail", error)
    deadLetters.publish(deadLetter).map: _ =>
      metrics.poison(Meters.Reasons.Unpersistable)

  /** The single insert, timed and counted. Never retries — that is [[attempt]]'s job.
    *
    * **The checkpoint goes in with the rows, or not at all.** When a [[BatchProcessor.Checkpointing]] is configured and
    * the repository can honour it, the offsets and the events commit in one transaction — which is the entire reason
    * `events.consumer_checkpoint` exists rather than a Redis key. Falling back to a plain insert when it is absent
    * keeps this class usable by the suites that have no checkpoint store, and the fallback is *silent about offsets*
    * rather than writing them separately: a second transaction would reintroduce exactly the window the table removes,
    * while looking like it worked.
    */
  private def insert(pending: Vector[PendingWrite]): Future[Unit] =
    val started = System.nanoTime()
    val written = (checkpoint, repository) match
      case (Some(checkpointing), writer: CheckpointingWriter) =>
        writer.insertAllCheckpointed(pending.map(_.event), checkpointing.commit(pending))
      case _ => repository.insertAll(pending.map(_.event))
    written
      .transform:
        case Success(written) =>
          metrics.batchWrite(Meters.Outcomes.Success, System.nanoTime() - started)
          count(pending, written)
          Success(())
        case Failure(error) =>
          metrics.batchWrite(Meters.Outcomes.Failure, System.nanoTime() - started)
          Failure(error)

  private def count(pending: Vector[PendingWrite], written: Long): Unit =
    pending
      .groupBy(_.envelope.eventType)
      .foreach((eventType, records) => metrics.persisted(eventType, records.size.toLong))
    metrics.duplicates(math.max(0L, pending.size.toLong - written))

object BatchProcessor:

  /** How to turn a batch into the offsets it earned.
    *
    * A small type rather than two loose parameters because the group id and the owner travel together everywhere and
    * because it makes the *absence* of checkpointing one `None` instead of two — a processor built with a group id and
    * no owner would otherwise be a state that compiles and means nothing.
    */
  final case class Checkpointing(groupId: String, owner: Option[String]):

    /** The positions a batch accounts for: per partition, the highest offset seen **plus one**.
      *
      * Plus one because that is Kafka's commit convention — the offset of the next record to read. Storing the last
      * processed offset instead reads identically and is off by one at every seek, in the direction that reprocesses a
      * record: safe, because the insert deduplicates, and still wrong.
      *
      * `max` and not `last`: a batch is assembled by `groupedWithin` across partitions and is not ordered by offset
      * within one, so taking the final element would checkpoint whichever record happened to arrive last.
      */
    def commit(pending: Vector[PendingWrite]): CheckpointCommit =
      val positions = pending
        .groupBy(write => (write.record.topic, write.record.partition))
        .toVector
        .map { case ((topic, partition), writes) =>
          CheckpointWrite(topic, partition, writes.map(_.record.offset).max + 1L, writes.size.toLong)
        }
      CheckpointCommit(groupId, owner, positions)

  /** SQLSTATE classes that mean "this row, not this database".
    *
    * `22` is a data exception (bad datetime, numeric overflow, invalid text representation); `23` is an integrity
    * constraint violation, which on this schema is how `cloud_event_specversion_ck`, `cloud_event_required_ck` and "no
    * partition of relation found for row" all surface. Both are deterministic properties of the row, so retrying or
    * restarting can only reproduce them.
    *
    * Everything else is treated as transient — including syntax and privilege errors (class `42`), which are a *build*
    * defect rather than a record defect and must page rather than quietly shovel the whole topic into the DLQ.
    */
  val DataErrorClasses: Set[String] = Set("22", "23")

  /** Walks the cause chain looking for a SQLSTATE this build is willing to dead-letter a record for.
    *
    * The chain matters: Magnum wraps, HikariCP wraps, and a `BatchUpdateException`'s real reason is in
    * `getNextException` rather than in `getCause`. Missing the state and treating a data error as transient is the
    * benign direction — the stream restarts and eventually bisects to the same record — so the walk is deliberately
    * conservative rather than clever.
    */
  def isDataError(error: Throwable): Boolean =
    @tailrec def loop(current: Throwable, seen: Int): Boolean =
      if current == null || seen > MaxCauseDepth then false
      else
        val state = current match
          case sql: SQLException => Option(sql.getSQLState)
          case _                 => None
        if state.exists(s => s.length >= 2 && DataErrorClasses(s.take(2))) then true
        else
          val next = current match
            case sql: SQLException if sql.getCause == null => sql.getNextException
            case other                                     => other.getCause
          loop(next, seen + 1)
    loop(error, 0)

  /** A cycle in a cause chain is rare but real (drivers do re-link exceptions), and an infinite loop inside an error
    * handler is the worst possible place for one.
    */
  private val MaxCauseDepth: Int = 32
