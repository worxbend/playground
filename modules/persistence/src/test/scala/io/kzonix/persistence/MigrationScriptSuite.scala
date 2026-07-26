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

import io.kzonix.kernel.event.Envelope
import java.nio.charset.StandardCharsets.UTF_8
import scala.io.Source
import scala.util.Using

/** Checks on the migration script that do not need a database.
  *
  * Applying `V1__events.sql` needs PostgreSQL and lives in `src/it`. Everything asserted here is a property of the
  * *text*, and the reason to assert it here rather than there is that these are the failures that would otherwise be
  * discovered late and expensively: an index quietly dropped during a refactor, a partition bound written without an
  * offset, or the reserved-attribute list drifting from the kernel's — none of which makes the migration fail, all of
  * which make the data wrong.
  */
final class MigrationScriptSuite extends munit.FunSuite:

  private val script: String =
    val stream = getClass.getClassLoader.getResourceAsStream("db/migration/V1__events.sql")
    assert(stream != null, "V1__events.sql is not on the classpath — the migration would ship as an empty jar entry")
    Using.resource(stream)(open => Source.fromInputStream(open, UTF_8.name).mkString)

  test("the fact table is range partitioned on the column the ingestion layer writes"):
    assert(script.contains("PARTITION BY RANGE (occurred_at)"))
    assert(script.contains("CREATE TABLE events.cloud_event_default PARTITION OF events.cloud_event DEFAULT"))

  test("every partition bound carries an explicit UTC offset"):
    // A bare date is parsed in the SESSION timezone, so a server running in Europe/Warsaw would shift every
    // partition by an hour and file events into the neighbouring month. The failure is invisible until a month end.
    val bounds = "FOR VALUES FROM \\('([^']+)'\\) TO \\('([^']+)'\\)".r
    val found = bounds.findAllMatchIn(script).flatMap(m => Vector(m.group(1), m.group(2))).toVector
    assert(found.nonEmpty, "no partition bounds found — has the DDL changed shape?")
    found.foreach(bound => assert(bound.endsWith("+00"), s"partition bound '$bound' has no explicit offset"))

  test("the dedup index that makes at-least-once delivery idempotent is present"):
    assert(script.contains("CREATE UNIQUE INDEX cloud_event_identity_uk"))
    assert(script.contains("ON events.cloud_event (occurred_at, ce_source, ce_id)"))

  test("all twelve indexes from ADR 5 are present"):
    val expected = Vector(
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
    )
    expected.foreach(name => assert(script.contains(s"INDEX $name"), s"index $name is missing"))

  test("every index carries a catalog comment naming the query it serves"):
    // An index nobody can name is an index nobody can justify deleting, which is how a table ends up with thirty of
    // them. Requiring the comment at migration time keeps the justification next to the cost.
    val created = "CREATE (?:UNIQUE )?INDEX (\\w+)".r.findAllMatchIn(script).map(_.group(1)).toVector
    assert(created.sizeIs >= 13, s"expected at least 13 indexes, found ${created.size}")
    created.foreach: name =>
      assert(script.contains(s"COMMENT ON INDEX events.$name IS"), s"index $name has no COMMENT ON INDEX")

  test("the reserved-attribute list matches the kernel's, so `extensions` means the same thing on both sides"):
    val listed = "raw - '\\{([^}]+)\\}'::text\\[\\]".r
      .findFirstMatchIn(script)
      .map(_.group(1).split(',').map(_.trim).toSet)
    assertEquals(listed, Some(Envelope.ReservedAttributes))

  test("the search vector uses the two-argument to_tsvector form"):
    // The one-argument form is only STABLE and PostgreSQL rejects it in a generated column — a failure that happens
    // at migration time on a real server and nowhere earlier, so it is worth pinning in text.
    assert(script.contains("to_tsvector('simple'"))
    assert(script.contains("to_tsvector('english'"))
    assert("to_tsvector\\(\\s*coalesce".r.findFirstIn(script).isEmpty)

  test("the rollup has the unique index that makes a concurrent refresh legal"):
    assert(script.contains("CREATE MATERIALIZED VIEW events.event_rollup_hourly"))
    assert(script.contains("CREATE UNIQUE INDEX event_rollup_hourly_uk"))

  test("the severity ranks in SQL are the ranks the kernel uses"):
    // These two tables of numbers are the same fact written twice; if they diverge, the alert filter in the UI and
    // partial index (11) disagree about what an alert is, and only one of them is visible in a code review.
    io.kzonix.kernel.search.Severity.values.foreach: severity =>
      assert(
        script.contains(s"WHEN '${severity.label}' THEN ${severity.rank}"),
        s"severity_rank() does not map ${severity.label} to ${severity.rank}"
      )

  test("clean-up is metadata only: the script contains no DELETE and no DROP of the fact table"):
    assert(!script.toUpperCase.contains("DELETE FROM"))
    assert(!script.toUpperCase.contains("DROP TABLE"))
