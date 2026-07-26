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
    assertEquals(Migrations.migrate(database.write.get()).executed, 0)

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
    val bound = query(
      "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
        "WHERE n.nspname = 'events' AND c.relname = 'cloud_event_2026_08'"
    ).head
    assert(bound.contains("2026-08-01 00:00:00+00"), bound)

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
    val uncommented = query(
      "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
        "WHERE n.nspname = 'events' AND c.relkind = 'i' AND obj_description(c.oid, 'pg_class') IS NULL " +
        "AND c.relname NOT LIKE '%_pkey' AND c.relname NOT LIKE 'cloud_event_20%'"
    )
    assertEquals(uncommented, Vector.empty[String], s"indexes without a purpose: ${uncommented.mkString(", ")}")

  test("the hourly rollup is populated and can be refreshed concurrently"):
    // CONCURRENTLY is only legal because of the unique index; if that index is ever dropped the refresh silently
    // becomes an ACCESS EXCLUSIVE lock that blocks every dashboard for its duration.
    execute("REFRESH MATERIALIZED VIEW CONCURRENTLY events.event_rollup_hourly")

  test("severity_rank agrees with the kernel for every level it knows"):
    io.kzonix.kernel.search.Severity.values.foreach: severity =>
      val rank = query(s"SELECT events.severity_rank('${severity.label}')").head
      assertEquals(rank, severity.rank.toString, s"severity_rank(${severity.label})")

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
