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

package com.worxbend.persistence.maintenance

import java.time.YearMonth
import scala.concurrent.duration.DurationInt

/** The generated DDL, asserted as text.
  *
  * Every statement here becomes SQL *text* rather than a bind parameter, because PostgreSQL accepts no parameters in
  * DDL at all. The rest of `modules/persistence` makes that impossible by construction (`Sql.lit` rejects any argument
  * without a compile-time constant type), so the two properties that construction would otherwise guarantee are
  * asserted here instead: that the identifiers are digits and underscores and nothing else, and that the bounds carry
  * the `+00` offset whose absence is invisible until a month boundary.
  */
final class PartitionDdlSuite extends munit.FunSuite:

  private val september: MonthPartition = MonthPartition(YearMonth.of(2026, 9))

  test("CREATE names the parent, the leaf and both bounds with explicit offsets"):
    assertEquals(
      PartitionDdl.create(september),
      "CREATE TABLE events.cloud_event_2026_09 PARTITION OF events.cloud_event " +
        "FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00')"
    )

  test("CREATE is not IF NOT EXISTS, because the job has to be able to report what it created"):
    // Idempotence comes from catching SQLSTATE 42P07 instead; `IF NOT EXISTS` would hide the distinction between
    // "created" and "was already there" behind a notice this code cannot read, and would also silently accept a
    // non-partition table squatting on the name.
    assert(!PartitionDdl.create(september).contains("IF NOT EXISTS"))

  test("a new partition re-applies the insert-driven autovacuum settings the migration sets on every leaf"):
    // They are not inherited from the parent, and the newest month is the only one being written to — so the month
    // that misses them is exactly the month whose visibility map goes stale and whose index-only scans stop being
    // index-only.
    assertEquals(
      PartitionDdl.applyStorageOptions(september),
      "ALTER TABLE events.cloud_event_2026_09 SET (autovacuum_vacuum_insert_scale_factor = 0.0, " +
        "autovacuum_vacuum_insert_threshold = 50000)"
    )

  test("DETACH is plain, never CONCURRENTLY, and never DROP"):
    // CONCURRENTLY is rejected outright on a partitioned table that has a DEFAULT partition, and this one has one by
    // design. DROP is absent for a different reason: destroying event data is an operator's decision.
    val statement = PartitionDdl.detach(september)
    assertEquals(statement, "ALTER TABLE events.cloud_event DETACH PARTITION events.cloud_event_2026_09")
    assert(!statement.contains("CONCURRENTLY"), "a DEFAULT partition makes the concurrent form illegal")
    assert(!statement.contains("DROP"), "retention detaches; it never destroys")

  test("no statement this module emits drops a table"):
    val statements = Vector(
      PartitionDdl.create(september),
      PartitionDdl.applyStorageOptions(september),
      PartitionDdl.detach(september),
      PartitionDdl.defaultRowsByMonth,
      PartitionDdl.attachedPartitions,
      RollupRefresh.RefreshConcurrently,
      RollupRefresh.RefreshBlocking
    )
    statements.foreach: statement =>
      assert(!statement.toUpperCase.contains("DROP TABLE"), s"a scheduled job must never drop a table: $statement")

  test("the detach lock timeout is SET LOCAL, so it cannot leak back into the pool"):
    // A session-level lock_timeout left on a pooled connection would surface as an ingest batch failing with
    // "canceling statement due to lock timeout" for reasons nothing in its own code path explains.
    assertEquals(PartitionDdl.lockTimeout(5.seconds), "SET LOCAL lock_timeout = '5000ms'")
    assert(PartitionDdl.lockTimeout(250.millis).matches("SET LOCAL lock_timeout = '\\d+ms'"))

  test("the default-partition survey truncates in UTC, not in the session timezone"):
    // date_trunc('month', timestamptz) uses the session TimeZone, so the same row would group into September or
    // October depending on which server ran the query.
    assert(PartitionDdl.defaultRowsByMonth.contains("occurred_at AT TIME ZONE 'UTC'"))
    assert(PartitionDdl.defaultRowsByMonth.contains("'YYYY-MM'"), "the key is text, so the driver cannot re-zone it")
    assert(PartitionDdl.defaultRowsByMonth.contains("events.cloud_event_default"))

  test("attached partitions come from pg_inherits and not from a name pattern"):
    // A LIKE 'cloud_event_20%' guess would also match a detached table left in the schema, and retention would then
    // try to detach something that is not attached.
    assert(PartitionDdl.attachedPartitions.contains("pg_inherits"))

  test("the remedy is a complete, paste-able transaction for the month it is about"):
    val remedy = PartitionDdl.adoptDefaultRows(september)
    assert(remedy.contains("BEGIN;"), remedy)
    assert(remedy.contains("COMMIT;"), remedy)
    assert(remedy.contains("CREATE TABLE events.cloud_event_2026_09 (LIKE events.cloud_event INCLUDING ALL)"), remedy)
    // The pre-attach CHECK is what lets ATTACH skip its validation scan and hold ACCESS EXCLUSIVE for a catalog
    // update rather than for a full read of the moved rows.
    assert(remedy.contains("ADD CONSTRAINT cloud_event_2026_09_bound_ck"), remedy)
    assert(remedy.contains("ATTACH PARTITION events.cloud_event_2026_09"), remedy)
    assert(remedy.contains("DELETE FROM events.cloud_event_default"), remedy)
    assert(remedy.contains("occurred_at >= '2026-09-01 00:00:00+00'"), remedy)
    assert(remedy.contains("occurred_at < '2026-10-01 00:00:00+00'"), remedy)

  test("the remedy moves only the columns an INSERT is allowed to write"):
    // Everything else on the fact table is GENERATED ALWAYS, and naming one of those columns makes the whole
    // transaction fail with "cannot insert a non-DEFAULT value into column".
    val remedy = PartitionDdl.adoptDefaultRows(september)
    assert(remedy.contains(s"INSERT INTO events.cloud_event_2026_09 (${PartitionDdl.BaseColumns})"), remedy)
    Vector("search_doc", "ce_type", "severity_rank", "extensions", "device_id").foreach: generated =>
      assert(!remedy.contains(generated), s"$generated is GENERATED ALWAYS and cannot be inserted into")

  test("the remedy restores the autovacuum settings the new leaf would otherwise not inherit"):
    assert(PartitionDdl.adoptDefaultRows(september).contains(PartitionDdl.InsertVacuumOptions))

  test("no generated identifier can be anything but the fixed prefix, four digits and two digits"):
    // The structural guarantee, restated where it is enforced. A `MonthPartition` cannot exist with a name outside
    // this shape, so no statement above can contain one either.
    (1 to 12).foreach: month =>
      Vector(1970, 2026, 9999).foreach: year =>
        val name = MonthPartition(YearMonth.of(year, month)).name
        assert(name.matches("cloud_event_\\d{4}_\\d{2}"), s"$name escapes the identifier shape")

  test("a year outside four digits is refused rather than rendered into DDL"):
    intercept[IllegalArgumentException](MonthPartition(YearMonth.of(12345, 6)).name)
    intercept[IllegalArgumentException](MonthPartition(YearMonth.of(-1, 6)).name)
