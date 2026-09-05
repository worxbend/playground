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
import org.apache.kafka.clients.consumer.ConsumerRecord
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
  * **The externalised checkpoint and the Kafka commit always describe the same position, and that is the invariant
  * [[BatchProcessor.Accounted]] exists to hold.** `process` returns a `Committable` for *every* record in the batch,
  * decoded or not, so the checkpoint has to account for every one of them too. Deriving it from the writable records
  * alone — which is what a `Vector[PendingWrite]` invites — silently drops the offsets of dead letters, and the two
  * consequences are both quiet: `consume.checkpoint.divergence`, whose only healthy value is zero, reports a gap that
  * means nothing and hides the one that does; and `restart?target=stored` rewinds the group onto records it has already
  * dead-lettered, re-consuming and re-dead-lettering them on every recovery. The offsets are therefore computed once,
  * in `process`, from the whole batch, and [[insert]] is the single place they can reach the database.
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

  /** The writer and the group to externalise offsets for, resolved once.
    *
    * `None` means this processor externalises nothing — either it was built without a [[BatchProcessor.Checkpointing]]
    * or its repository cannot honour one. Deciding it here rather than at each write keeps the two conditions from
    * drifting apart, and gives [[write]] a single question to ask about whether a rowless batch has anything to do.
    */
  private val checkpointing: Option[(CheckpointingWriter, BatchProcessor.Checkpointing)] =
    (repository, checkpoint) match
      case (writer: CheckpointingWriter, Some(group)) => Some((writer, group))
      case _                                          => None

  /** Processes one batch and yields the offsets it earned, in arrival order. */
  def process(batch: Vector[DecodedRecord]): Future[Vector[Committable]] =
    if batch.isEmpty then Future.successful(Vector.empty)
    else
      metrics.batchSize(batch.size)
      val (poison, pending) = batch.map(_.outcome).partitionMap(identity)
      // From `batch` and not from `pending`: the committables returned below cover the poison records too, so the
      // checkpoint must as well or the two positions disagree by exactly the dead letters. See the class comment.
      val accounted = BatchProcessor.Accounted.of(batch.map(_.record))
      for
        _ <- deadLetterAll(poison)
        _ <- write(pending, accounted)
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

  /** Writes `pending` and records that `accounted` is now durable.
    *
    * **Rowless is not the same as nothing to do.** A poll whose every record dead-lettered still moved the consumer,
    * and the write is the only place that fact reaches `events.consumer_checkpoint`. The single case with genuinely
    * nothing to do is a rowless write on a processor that externalises no offsets at all — the suites without a
    * checkpoint store — and skipping that spares them a round trip that could only insert zero rows.
    */
  private def write(pending: Vector[PendingWrite], accounted: BatchProcessor.Accounted): Future[Unit] =
    if pending.isEmpty && checkpointing.isEmpty then Future.unit else attempt(pending, accounted, attempts)

  /** One whole-batch insert, retried while the failure still looks transient. */
  private def attempt(
    pending: Vector[PendingWrite],
    accounted: BatchProcessor.Accounted,
    remaining: Int
  ): Future[Unit] =
    insert(pending, accounted).recoverWith:
      case NonFatal(error) if remaining > 1 && !BatchProcessor.isDataError(error) =>
        logger.warn(s"batch insert of ${pending.size} records failed, retrying: ${RecordDecoder.describe(error)}")
        backoff().flatMap(_ => attempt(pending, accounted, remaining - 1))
      case NonFatal(error) => isolate(pending, accounted, error)

  /** Halve, or attribute. See the class Scaladoc for why this is not allowed to run on a transient failure.
    *
    * **The right half inherits the whole scope's offsets and the left half gets only its own.** Within a partition a
    * batch arrives in offset order, so the left half's high-water mark is always below the right's — and making the
    * *last* durable write of a bisection the one that carries the parent's position is what keeps the invariant true on
    * this path too. Give both halves their own and a batch that ends in poison, or in a record the database refuses,
    * checkpoints short of what it committed to Kafka.
    */
  private def isolate(
    pending: Vector[PendingWrite],
    accounted: BatchProcessor.Accounted,
    error: Throwable
  ): Future[Unit] =
    if pending.sizeIs > 1 then
      val (left, right) = pending.splitAt(pending.size / 2)
      logger.warn(s"isolating a poison record: splitting a failed batch of ${pending.size}")
      val spent = left.map(_.record)
      write(left, BatchProcessor.Accounted.of(spent)).flatMap(_ => write(right, accounted.less(spent)))
    else if pending.sizeIs == 1 && BatchProcessor.isDataError(error) then
      unpersistable(pending.head, accounted, error)
    // Nothing left to attribute the failure to — a rowless write whose checkpoint would not commit, or a single
    // record the database refused for a reason it will not repeat. Either way the batch is not accounted for.
    else Future.failed(error)

  /** One record the database will never accept. Dead-lettered, counted, and its offset allowed to move past it.
    *
    * The offset moves by a rowless checkpoint write, not by omission: this record is durable on the DLQ and its
    * committable is about to reach the committer, so the stored position has to say so or the next `target=stored`
    * restart lands straight back on the record that cannot be stored.
    */
  private def unpersistable(
    pending: PendingWrite,
    accounted: BatchProcessor.Accounted,
    error: Throwable
  ): Future[Unit] =
    val detail = s"the database rejected this record: ${RecordDecoder.describe(error)}"
    val deadLetter = DeadLetter.of(pending.record, DecodeFailure.Unconvertible(detail), source)
    logger.error(s"record ${deadLetter.origin.dlqKey} is unpersistable and is being dead-lettered: $detail", error)
    deadLetters.publish(deadLetter).flatMap: _ =>
      metrics.poison(Meters.Reasons.Unpersistable)
      write(Vector.empty, accounted)

  /** The single insert, timed and counted. Never retries — that is [[attempt]]'s job.
    *
    * **The only place an offset reaches the database, which is what makes the invariant checkable in one spot.** When a
    * [[BatchProcessor.Checkpointing]] is configured and the repository can honour it, the offsets and the events commit
    * in one transaction — the entire reason `events.consumer_checkpoint` exists rather than a Redis key — and `pending`
    * may be empty, because a checkpoint with no rows is how a poll that only dead-lettered records still moves. Falling
    * back to a plain insert when checkpointing is absent keeps this class usable by the suites that have no checkpoint
    * store, and the fallback is *silent about offsets* rather than writing them separately: a second transaction would
    * reintroduce exactly the window the table removes, while looking like it worked.
    */
  private def insert(pending: Vector[PendingWrite], accounted: BatchProcessor.Accounted): Future[Unit] =
    val started = System.nanoTime()
    val written = checkpointing match
      case Some((writer, group)) => writer.insertAllCheckpointed(pending.map(_.event), group.commit(accounted))
      case None                  => repository.insertAll(pending.map(_.event))
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

  /** The Kafka positions a durable effect is answerable for: per partition, the highest offset seen **plus one**.
    *
    * **A type, and not a `Vector[PendingWrite]`, because the wrong vector here is invisible and its consequence is
    * silent.** The writable records and the records the consumer is committing are different sets whenever anything
    * dead-letters, and a signature that accepts the first will eventually be handed it — which is exactly the defect
    * this type replaced: dead-lettered offsets never reached `events.consumer_checkpoint`, so the divergence gauge
    * reported a permanent gap and `restart?target=stored` rewound onto records already on the DLQ. Building one takes
    * `ConsumerRecord`s, which the poison path has and the write path has, so the *complete* set is the natural thing to
    * pass and the writable subset is the one you have to go out of your way to construct.
    *
    * Plus one because that is Kafka's commit convention — the offset of the next record to read. Storing the last
    * processed offset instead reads identically and is off by one at every seek, in the direction that reprocesses a
    * record: safe, because the insert deduplicates, and still wrong.
    */
  final case class Accounted private (positions: Vector[CheckpointWrite]):

    /** These offsets, less the records an earlier write already added to the column.
      *
      * **`records` accumulates in the database (`records + EXCLUDED.records`) and bisection writes the same batch
      * twice**, so the second write cannot simply carry the parent's counts. It does have to carry the parent's
      * *offsets*: the right half may not hold the batch's highest one, and the store's `WHERE EXCLUDED.next_offset > …`
      * guard would then leave the stored position short of what was committed to Kafka — the very shortfall `isolate`
      * exists to prevent. Subtracting keeps both properties: the position the whole batch earned, and each record
      * counted once, at every level of the recursion.
      *
      * Floored at zero rather than allowed to go negative. The column is `CHECK (records >= 0)`, and a diagnostic that
      * has drifted must not fail the transaction carrying the offset — that would trade a wrong number for a stalled
      * consumer.
      */
    def less(earlier: Vector[ConsumerRecord[?, ?]]): Accounted =
      val spent = earlier.groupBy(record => (record.topic, record.partition)).view.mapValues(_.size.toLong).toMap
      Accounted(positions.map { position =>
        val already = spent.getOrElse((position.topic, position.partition), 0L)
        position.copy(records = math.max(0L, position.records - already))
      })

  object Accounted:

    /** `max` and not `last`: a batch is assembled by `groupedWithin` across partitions and is not ordered by offset
      * within one, so taking the final element would checkpoint whichever record happened to arrive last.
      */
    def of(records: Vector[ConsumerRecord[?, ?]]): Accounted =
      Accounted(
        records
          .groupBy(record => (record.topic, record.partition))
          .toVector
          .map { case ((topic, partition), group) =>
            CheckpointWrite(topic, partition, group.map(_.offset).max + 1L, group.size.toLong)
          }
      )

  /** How to turn a batch into the offsets it earned.
    *
    * A small type rather than two loose parameters because the group id and the owner travel together everywhere and
    * because it makes the *absence* of checkpointing one `None` instead of two — a processor built with a group id and
    * no owner would otherwise be a state that compiles and means nothing.
    */
  final case class Checkpointing(groupId: String, owner: Option[String]):

    /** Names the group and the replica behind an [[Accounted]] position. Nothing more: what the position *is* belongs
      * to [[Accounted]], so there is one place that can compute it and one place that can get it wrong.
      */
    def commit(accounted: Accounted): CheckpointCommit =
      CheckpointCommit(groupId, owner, accounted.positions)

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
