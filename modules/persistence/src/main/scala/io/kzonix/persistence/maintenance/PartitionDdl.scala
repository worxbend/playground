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

package io.kzonix.persistence.maintenance

import scala.concurrent.duration.FiniteDuration

/** Every statement the partition jobs emit, as pure text.
  *
  * **Why the text is built here and not with `Sql.lit`.** The rest of this module cannot splice a runtime string into
  * SQL at all: `Sql.lit` is an inline method that rejects any argument without a compile-time constant type, so user
  * input is structurally incapable of becoming statement text. DDL cannot go through that door, because PostgreSQL
  * accepts no bind parameters in DDL whatsoever — `CREATE TABLE ? PARTITION OF ...` is not a statement, and neither is
  * `FOR VALUES FROM (?)`. The identifiers and bound literals below therefore *are* text, and the guarantee is
  * re-established one step earlier instead: [[MonthPartition.name]] validates itself against a digits-only pattern at
  * construction, and the bounds are rendered by a pinned UTC formatter from a `YearMonth`. Nothing a user can type
  * reaches any of it.
  *
  * Pure functions returning `String`, so a unit test asserts on the exact statement — including the `+00` offsets,
  * whose absence is invisible until a month boundary — without a database.
  */
object PartitionDdl:

  /** The `reloptions` `V1__events.sql` sets on every leaf.
    *
    * Repeated on each new partition because **autovacuum settings are not inherited from the partitioned parent**, and
    * PostgreSQL rejects storage parameters on the parent outright. The fact table is append-only, so dead-tuple
    * autovacuum never fires on it; without insert-driven vacuuming the newest month — the only one being written to —
    * is the single month whose visibility map goes stale, which quietly turns every index-only scan over recent events
    * into a heap fetch per row. `V1__events.sql` says this in as many words; this constant is the half of the sentence
    * that has to keep being true for partitions the migration never saw.
    */
  val InsertVacuumOptions: String =
    "autovacuum_vacuum_insert_scale_factor = 0.0, autovacuum_vacuum_insert_threshold = 50000"

  /** The columns that are not `GENERATED ALWAYS`, in DDL order.
    *
    * Only these five can be written by an `INSERT`; the other twenty-odd are derived from `raw` by the database. Any
    * hand-written data movement — the DEFAULT-partition remedy below is the one this module emits — has to name them
    * explicitly, because `INSERT INTO ... SELECT *` fails on a generated column with "cannot insert a non-DEFAULT
    * value into column".
    */
  val BaseColumns: String = "occurred_at, event_uid, ingested_at, raw, payload_sha256"

  /** `CREATE TABLE ... PARTITION OF ... FOR VALUES FROM ... TO ...`.
    *
    * Deliberately **not** `IF NOT EXISTS`. Idempotence is wanted, but `IF NOT EXISTS` buys it by turning "the table is
    * already there" into a notice this code cannot see, so the job could never report what it actually created — and
    * it would also silently accept a *non-partition* table that happened to occupy the name. Letting the duplicate
    * raise `42P07` and catching that one SQLSTATE gives the same idempotence with an exact answer, and it is the
    * behaviour a second concurrent creator needs anyway.
    */
  def create(partition: MonthPartition): String =
    s"CREATE TABLE ${partition.qualifiedName} PARTITION OF ${MonthPartition.QualifiedParent} " +
      s"FOR VALUES FROM ('${partition.fromLiteral}') TO ('${partition.untilLiteral}')"

  /** Re-applies [[InsertVacuumOptions]] to a freshly created leaf. */
  def applyStorageOptions(partition: MonthPartition): String =
    s"ALTER TABLE ${partition.qualifiedName} SET ($InsertVacuumOptions)"

  /** `ALTER TABLE ... DETACH PARTITION`, **without** `CONCURRENTLY`.
    *
    * Not an oversight, and not a preference. `DETACH PARTITION CONCURRENTLY` is rejected outright on a partitioned
    * table that has a `DEFAULT` partition, and `events.cloud_event` has one by design (it is the clock-skew safety net
    * `partition.default.rows` alerts on). So the only detach available here is the blocking form, which takes `ACCESS
    * EXCLUSIVE` on the parent for the duration.
    *
    * That lock is brief — detaching is a catalog update, not a scan — but "brief" is not "safe": `ACCESS EXCLUSIVE`
    * queues behind every open transaction on the parent and, while it waits, every *new* ingest insert queues behind
    * it. One long-running search is enough to convert a metadata operation into an ingest outage. Hence
    * [[lockTimeout]], which is applied in the same transaction and is what makes a contended detach fail and be
    * retried next cycle instead of stalling the write path.
    */
  def detach(partition: MonthPartition): String =
    s"ALTER TABLE ${MonthPartition.QualifiedParent} DETACH PARTITION ${partition.qualifiedName}"

  /** `SET LOCAL lock_timeout`, for the transaction that detaches.
    *
    * `LOCAL` and not a session `SET`: the connection comes from a pool and goes back to it, and a session-level
    * `lock_timeout` left behind would silently apply to whatever borrows that connection next — an ingest batch
    * failing with "canceling statement due to lock timeout" for reasons nothing in its own code path explains. `LOCAL`
    * expires at `COMMIT` and cannot leak.
    *
    * Milliseconds, rendered from a `FiniteDuration`, so the value is digits and a unit and nothing else.
    */
  def lockTimeout(timeout: FiniteDuration): String =
    s"SET LOCAL lock_timeout = '${timeout.toMillis.max(1L)}ms'"

  /** Rows already in `cloud_event_default` for this month, and the total, in one scan.
    *
    * `AT TIME ZONE 'UTC'` is not decoration: `date_trunc('month', occurred_at)` on a `timestamptz` truncates in the
    * *session* timezone, so the same row groups into September or October depending on which server ran the query —
    * the JVM-side twin of the bound-literal trap in [[MonthPartition]]. Rendering the key as `YYYY-MM` text keeps the
    * JDBC driver's own timezone handling out of the answer as well.
    *
    * Grouped rather than probed per month: the normal case is an empty table, this is one scan of it, and the total it
    * yields is exactly the number `partition.default.rows` gauges.
    */
  val defaultRowsByMonth: String =
    "SELECT to_char(date_trunc('month', occurred_at AT TIME ZONE 'UTC'), 'YYYY-MM') AS month, count(*) " +
      s"FROM ${MonthPartition.QualifiedDefaultPartition} GROUP BY 1 ORDER BY 1"

  /** The leaves currently attached to the parent. `pg_inherits` and not a name pattern: the catalog knows what is
    * really a partition, a `LIKE 'cloud_event_20%'` guess does not, and the difference matters the moment someone
    * leaves a detached table sitting in the schema.
    */
  val attachedPartitions: String =
    "SELECT c.relname FROM pg_inherits i " +
      "JOIN pg_class c ON c.oid = i.inhrelid " +
      "JOIN pg_class p ON p.oid = i.inhparent " +
      "JOIN pg_namespace n ON n.oid = p.relnamespace " +
      s"WHERE n.nspname = '${MonthPartition.Schema}' AND p.relname = '${MonthPartition.Parent}' ORDER BY 1"

  /** The runbook entry for a month whose rows are already sitting in `DEFAULT`, generated with this month's real names
    * and bounds so an operator can paste it rather than transcribe it.
    *
    * **The trap it answers.** PostgreSQL will not attach a partition whose range overlaps rows currently held by the
    * `DEFAULT` partition. `CREATE TABLE ... PARTITION OF` on such a range takes `ACCESS EXCLUSIVE` on the parent,
    * scans the default partition to check, finds a matching row and fails with "updated partition constraint for
    * default partition would be violated by some row" — having blocked ingest for the length of the scan on its way to
    * failing. There is no flag that makes it succeed. The rows must move first, and moving them is a decision with a
    * cost (an exclusive lock, a rewrite of however many rows drifted in) that a background job must not take on an
    * operator's behalf at 3am.
    *
    * So the job detects, reports, skips that month, and hands over this. The transaction:
    *
    *   1. builds the new leaf standalone with `LIKE ... INCLUDING ALL`, which carries the generated columns, the
    *      checks and the thirteen indexes — a partition attached without them would be a table scan for every query
    *      that touches its month;
    *   2. adds a `CHECK` matching the partition bound *before* attaching, which is what lets `ATTACH PARTITION` skip
    *      its validation scan and take its lock for a catalog update instead of a full read;
    *   3. moves the rows with `DELETE ... RETURNING` feeding the `INSERT`, so they are removed from `DEFAULT` and
    *      inserted in one statement and cannot exist in both places or neither;
    *   4. attaches, drops the now-redundant `CHECK` (the partition bound supersedes it), and restores the
    *      insert-driven autovacuum settings.
    *
    * If the rows are what they usually are — clock-skew garbage from one misconfigured producer, which is exactly what
    * the `DEFAULT` partition is a net for — the cheaper remedy is to delete them and let the job create the partition
    * on its next pass. That choice is the operator's; both are destructive in one direction and neither is automatic.
    */
  def adoptDefaultRows(partition: MonthPartition): String =
    val bounds = s"occurred_at >= '${partition.fromLiteral}' AND occurred_at < '${partition.untilLiteral}'"
    val check = s"${partition.name}_bound_ck"
    s"""|-- ${partition.name}: rows for this month are already in ${MonthPartition.DefaultPartition}, so the
        |-- partition cannot be created until they are moved. Run in a quiet window: the ATTACH takes
        |-- ACCESS EXCLUSIVE on ${MonthPartition.QualifiedParent}, which queues every ingest insert behind it.
        |BEGIN;
        |SET LOCAL lock_timeout = '5s';
        |CREATE TABLE ${partition.qualifiedName} (LIKE ${MonthPartition.QualifiedParent} INCLUDING ALL);
        |ALTER TABLE ${partition.qualifiedName}
        |  ADD CONSTRAINT $check CHECK ($bounds);
        |WITH moved AS (
        |  DELETE FROM ${MonthPartition.QualifiedDefaultPartition}
        |   WHERE $bounds
        |  RETURNING $BaseColumns)
        |INSERT INTO ${partition.qualifiedName} ($BaseColumns)
        |SELECT $BaseColumns FROM moved;
        |ALTER TABLE ${MonthPartition.QualifiedParent} ATTACH PARTITION ${partition.qualifiedName}
        |  FOR VALUES FROM ('${partition.fromLiteral}') TO ('${partition.untilLiteral}');
        |ALTER TABLE ${partition.qualifiedName} DROP CONSTRAINT $check;
        |ALTER TABLE ${partition.qualifiedName} SET ($InsertVacuumOptions);
        |COMMIT;
        |-- Or, if the rows are clock-skew garbage: DELETE FROM ${MonthPartition.QualifiedDefaultPartition}
        |-- WHERE $bounds;  and let the job create the partition on its next pass.""".stripMargin
