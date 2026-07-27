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

package io.kzonix.persistence

import scala.util.Using

/** `V1__events.sql` against a real PostgreSQL 18.
  *
  * This is the exit criterion of ADR Phase 3, and the reason it cannot be a unit test: the riskiest line in the DDL is
  * the `search_doc` generated column, which requires `jsonb_to_tsvector(regconfig, jsonb, jsonb)` and the two-argument
  * `to_tsvector` to be `IMMUTABLE`. PostgreSQL decides that at `CREATE TABLE` time. Nothing earlier in the pipeline —
  * not the compiler, not the text checks in `MigrationScriptSuite` — can tell you whether the schema is legal.
  */
final class MigrationIT extends PostgresSuite:

  test("migrate applies the schema and validate agrees with it afterwards"):
    val report = PostgresSuite.migrated
    assertEquals(report.targetVersion, Some("1"))
    // validate() throws on drift. An edited-in-place migration is the failure it exists to catch, and it is a failure
    // that otherwise shows up as a production database whose shape does not match the file describing it.
    Migrations.validate(database.write.get())

  test("the applied-version list matches the committed baseline"):
    // Pinned deliberately: adding V2 must be a conscious edit to this list, so a migration merged by accident fails
    // CI rather than silently reshaping every environment it reaches (ADR §9.3).
    assertEquals(Migrations.appliedVersions(database.write.get()), Vector("1"))

  test("a second migrate is a no-op, so every replica can run it at boot"):
    val second = Migrations.migrate(database.write.get())
    assertEquals(second.executed, 0)
    // And still reports where the schema *is*. Flyway leaves `targetSchemaVersion` null when nothing was pending, so
    // reporting it verbatim made cobalt log "schema at <none>" on every boot after the first and made any
    // "is the schema applied" gate built on this field fail on a database that was already migrated.
    assertEquals(second.targetVersion, Some("1"))
    assertEquals(second.initialVersion, Some("1"))

  test("the fact table is partitioned by month with a default catch-all"):
    val partitions = query("SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid " +
      "JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace " +
      "WHERE n.nspname = 'events' AND p.relname = 'cloud_event' ORDER BY 1")
    assert(partitions.contains("cloud_event_2026_07"), partitions.toString)
    assert(partitions.contains("cloud_event_2026_08"), partitions.toString)
    assert(partitions.contains("cloud_event_default"), partitions.toString)

  test("partition bounds land on UTC month boundaries, not on the session timezone"):
    // Run under any TimeZone and the bound must not move. A bare date in the DDL would shift with the server offset
    // and file a month-boundary event into the neighbouring partition — invisible except at month ends.
    //
    // Compared as a *value*, never as text. `pg_get_expr` renders a timestamptz in the session timezone, so a
    // `contains("2026-08-01 00:00:00+00")` assertion is an assertion about the JVM's default TimeZone: on a machine
    // set to Europe/Kyiv the catalog reads back "2026-08-01 03:00:00+03" — the correct instant, spelled locally —
    // and the test fails while the schema is right. Formatting the expected instants through the same session
    // renderer makes both sides move together, so what is left being compared is the instant.
    val bound = query(
      "SELECT pg_get_expr(c.relpartbound, c.oid) = format('FOR VALUES FROM (%L) TO (%L)', " +
        "timestamptz '2026-08-01 00:00:00+00', timestamptz '2026-09-01 00:00:00+00') " +
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
        "WHERE n.nspname = 'events' AND c.relname = 'cloud_event_2026_08'"
    ).head
    assertEquals(bound, "t", "the August partition does not start at the UTC month boundary")

  test("the clock-skew default partition is empty, which is the invariant the metric alerts on"):
    assertEquals(query("SELECT count(*) FROM events.cloud_event_default").head, "0")

  test("every index from ADR 5 exists on the parent table"):
    val indexes = query("SELECT indexname FROM pg_indexes WHERE schemaname = 'events' AND tablename = 'cloud_event'")
    Vector(
      "cloud_event_identity_uk",
      "cloud_event_type_time_ix",
      "cloud_event_device_time_ix",
      "cloud_event_source_time_ix",
      "cloud_event_room_time_ix",
      "cloud_event_person_time_ix",
      "cloud_event_ingested_brin",
      "cloud_event_data_gin",
      "cloud_event_extensions_gin",
      "cloud_event_tags_gin",
      "cloud_event_search_gin",
      "cloud_event_alerts_ix",
      "cloud_event_metric_ix"
    ).foreach(name => assert(indexes.contains(name), s"$name is missing; have: ${indexes.mkString(", ")}"))

  test("every index carries the comment naming the query it serves"):
    // The exclusion is "was this index written by hand", expressed as "is it a partition of another index" via
    // pg_inherits. A name pattern cannot express that: `NOT LIKE 'cloud_event_20%'` covers the monthly partitions and
    // misses `cloud_event_default`'s thirteen inherited indexes entirely, so the check passed by accident of naming
    // and would have gone on passing had a hand-written index been named `cloud_event_2026_anything`.
    val uncommented = query(
      "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
        "WHERE n.nspname = 'events' AND c.relkind = 'i' AND obj_description(c.oid, 'pg_class') IS NULL " +
        "AND c.relname NOT LIKE '%_pkey' AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid)"
    )
    assertEquals(uncommented, Vector.empty[String], s"indexes without a purpose: ${uncommented.mkString(", ")}")

  test("the insert-driven autovacuum settings reach the leaves, which are the tables that hold tuples"):
    // A partitioned parent stores nothing, so reloptions set on it would be both illegal and useless. Asserting on the
    // leaves is asserting on the thing autovacuum actually reads.
    val partitions = Vector("cloud_event_2026_07", "cloud_event_2026_08", "cloud_event_default")
    partitions.foreach: partition =>
      val options = query(s"SELECT unnest(reloptions) FROM pg_class WHERE relname = '$partition'")
      assert(
        options.contains("autovacuum_vacuum_insert_scale_factor=0.0"),
        s"$partition is missing the insert-driven vacuum scale factor; has: ${options.mkString(", ")}"
      )
      assert(
        options.contains("autovacuum_vacuum_insert_threshold=50000"),
        s"$partition is missing the insert-driven vacuum threshold; has: ${options.mkString(", ")}"
      )

  test("the hourly rollup is populated and can be refreshed concurrently"):
    // CONCURRENTLY is only legal because of the unique index; if that index is ever dropped the refresh silently
    // becomes an ACCESS EXCLUSIVE lock that blocks every dashboard for its duration.
    execute("REFRESH MATERIALIZED VIEW CONCURRENTLY events.event_rollup_hourly")

  test("severity_rank agrees with the kernel for every spelling the kernel accepts"):
    // Every spelling, not just the eight canonical labels: `Severity.rank` is documented as "the exact contract of
    // events.severity_rank()", and it is the syslog aliases — err, crit, emerg, panic — where the two drifted. A
    // NULL rank there means the row is invisible to the alert feed and to `severity>=warn` while still rendering as
    // critical in the detail view, which is the worst kind of disagreement: silent and only in one direction.
    io.kzonix.kernel.search.Severity.Spellings.foreach: spelling =>
      val rank = query(s"SELECT events.severity_rank('$spelling')").head
      assertEquals(Option(rank), io.kzonix.kernel.search.Severity.rank(spelling).map(_.toString), s"rank($spelling)")

  test("severity_rank returns NULL for a string that is not a severity, rather than raising"):
    // Generated columns are computed inside the INSERT: a raising severity_rank would abort a whole 500-event cobalt
    // batch because one device sent `severity: "banana"`.
    assertEquals(query("SELECT coalesce(events.severity_rank('banana')::text, 'null')").head, "null")
    assertEquals(query("SELECT coalesce(events.severity_rank(NULL)::text, 'null')").head, "null")

  test("the extraction helpers return NULL rather than aborting an insert on a malformed payload"):
    assertEquals(query("SELECT coalesce(events.jsonb_num('{\"a\":\"nope\"}'::jsonb, '{a}'), -1)").head, "-1")
    assertEquals(query("SELECT coalesce(events.jsonb_text_array('{\"a\":5}'::jsonb, '{a}'), '{}')").head, "{}")

  private def query(sql: String): Vector[String] =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        Using.resource(statement.executeQuery(sql)): rs =>
          Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toVector

  private def execute(sql: String): Unit =
    withConnection: connection =>
      Using.resource(connection.createStatement()): statement =>
        val _ = statement.execute(sql)
