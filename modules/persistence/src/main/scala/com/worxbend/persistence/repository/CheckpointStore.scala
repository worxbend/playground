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

package com.worxbend.persistence.repository

import com.augustnagro.magnum.DbCodec
import com.augustnagro.magnum.DbTx
import com.augustnagro.magnum.Transactor
import com.augustnagro.magnum.connect
import com.augustnagro.magnum.sql
import com.augustnagro.magnum.transact
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** One partition's durable position.
  *
  * @param nextOffset
  *   the offset of the **next** record to read, which is Kafka's own commit convention (`last processed + 1`). Storing
  *   "last processed" instead reads identically and is off by one at every seek, in the direction that reprocesses a
  *   record — safe here, because the insert is idempotent, and still wrong.
  * @param records
  *   how many records this checkpoint accounts for, cumulative. The cheapest way to tell an *idle* partition from a
  *   *stuck* one: both have a static `nextOffset`, only one has a static count.
  */
final case class Checkpoint(
  groupId: String,
  topic: String,
  partition: Int,
  nextOffset: Long,
  records: Long,
  owner: Option[String],
  updatedAt: OffsetDateTime
) derives DbCodec

/** A position to write, without the bookkeeping the store fills in. */
final case class CheckpointWrite(topic: String, partition: Int, nextOffset: Long, records: Long)

/** Externalised consumer offsets.
  *
  * **This does not replace Kafka's `__consumer_offsets`,** which stays authoritative for fetching. It answers the two
  * questions Kafka's own storage cannot:
  *
  *   1. **Retention.** `offsets.retention.minutes` defaults to seven days. A group that stops committing for longer
  *      has its offsets deleted, and the next start resolves `auto.offset.reset=earliest` and replays the whole
  *      retained log. Survivable, because the insert deduplicates — and completely silent, which is the problem.
  *   2. **Transactionality.** [[record]] is called *inside* the same transaction as the batch insert, so the offset
  *      and the rows it accounts for commit or roll back together. That is the only arrangement in which "the last
  *      durably stored offset" has an exact answer; Kafka's commit is a separate round trip afterwards, and the window
  *      between them is precisely the replay window.
  *
  * **Why Postgres and not Redis or Cassandra.** The store has to be external to the broker, and the honest answer for
  * this system is the database that is already a hard dependency of the write path. Redis is a second datastore to
  * run, back up and monitor, and — not being transactional with respect to the insert — gives up property 2, which is
  * the only reason worth doing this at all. Cassandra adds an operational surface larger than the rest of the stack
  * for a table holding one row per partition. If Postgres is down, cobalt is not consuming; no new failure mode.
  */
trait CheckpointStore:

  /** Records positions **inside an existing transaction**.
    *
    * Takes a `DbTx` rather than opening its own, and that is the whole design: called from within the batch insert's
    * transaction, the checkpoint is atomic with the rows. A version of this that opened its own connection would be a
    * second commit that can fail on its own, which is the state this table exists to make impossible.
    */
  def record(groupId: String, owner: Option[String], positions: Vector[CheckpointWrite])(using DbTx): Unit

  /** Every checkpoint for a group, most recently active first. Admin-path only. */
  def load(groupId: String): Future[Vector[Checkpoint]]

  /** Forgets a group's checkpoints.
    *
    * The operator escape hatch for "this stored position is wrong and I want the broker's answer instead". Returns the
    * number of rows removed so a caller can tell "cleared three partitions" from "there was nothing there", which are
    * different things to report back from an admin endpoint.
    */
  def clear(groupId: String): Future[Int]

object CheckpointStore:

  /** The table, qualified. One constant because it appears in four statements. */
  val Table: String = "events.consumer_checkpoint"

/** The Magnum implementation.
  *
  * Two transactors, as everywhere else in this module: reads go to the read pool, writes to the write pool. [[record]]
  * takes neither, because it deliberately borrows the caller's transaction.
  */
final class PostgresCheckpointStore(read: Transactor, write: Transactor)(using ExecutionContext)
    extends CheckpointStore:

  def record(groupId: String, owner: Option[String], positions: Vector[CheckpointWrite])(using DbTx): Unit =
    positions.foreach: position =>
      // `records` accumulates rather than being overwritten, so the column answers "how much has this partition ever
      // moved" — a monotone counter an operator can watch — instead of "how big was the last batch", which the batch
      // meters already report.
      //
      // The `WHERE` on the update is what makes a late or duplicated write harmless: a rebalance can hand the same
      // partition to another replica mid-flight, and without the guard the older position would overwrite the newer
      // one and manufacture a replay. Monotonicity is enforced by the database, not by the caller's ordering.
      val _ = sql"""
        INSERT INTO events.consumer_checkpoint (group_id, topic, partition, next_offset, records, owner, updated_at)
        VALUES (${groupId}, ${position.topic}, ${position.partition}, ${position.nextOffset}, ${position.records},
                ${owner}, now())
        ON CONFLICT (group_id, topic, partition) DO UPDATE
          SET next_offset = EXCLUDED.next_offset,
              records     = events.consumer_checkpoint.records + EXCLUDED.records,
              owner       = EXCLUDED.owner,
              updated_at  = now()
          WHERE EXCLUDED.next_offset > events.consumer_checkpoint.next_offset
      """.update.run()

  def load(groupId: String): Future[Vector[Checkpoint]] =
    Future:
      connect(read):
        sql"""
          SELECT group_id, topic, partition, next_offset, records, owner, updated_at
          FROM events.consumer_checkpoint
          WHERE group_id = ${groupId}
          ORDER BY updated_at DESC, topic, partition
        """.query[Checkpoint].run()

  def clear(groupId: String): Future[Int] =
    Future:
      transact(write):
        sql"DELETE FROM events.consumer_checkpoint WHERE group_id = ${groupId}".update.run()

/** What to checkpoint alongside a batch of events. */
final case class CheckpointCommit(groupId: String, owner: Option[String], positions: Vector[CheckpointWrite])

/** Insert-and-checkpoint, in one transaction.
  *
  * **A separate interface from [[EventRepository]], deliberately.** Only the consumer has offsets; ferrite is a reader
  * and its repository stub should not have to implement — or be able to forget — a method it has no use for. Adding a
  * defaulted method to `EventRepository` instead would put a no-op checkpoint on every implementation, which is the
  * failure this arrangement makes unrepresentable: a writer that silently discards the offset it was handed.
  */
trait CheckpointingWriter:

  /** Writes the events and their checkpoint atomically, returning the rows the database actually took.
    *
    * The atomicity is the entire point. Two statements in two transactions can commit in either order and fail
    * independently: a checkpoint ahead of its rows loses events on the next start, and rows ahead of their checkpoint
    * replay them. One transaction has neither state.
    */
  def insertAllCheckpointed(events: Vector[NewEvent], commit: CheckpointCommit): Future[Long]
