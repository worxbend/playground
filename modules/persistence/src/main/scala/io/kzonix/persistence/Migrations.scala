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

import javax.sql.DataSource
import org.flywaydb.core.Flyway
import scala.jdk.CollectionConverters.*

/** What a migration run did.
  *
  * Returned rather than logged so the caller can assert on it: ADR §9.3 makes "the applied-version list matches a
  * committed baseline" a CI gate, which is the mechanism that catches a migration edited in place — the single most
  * common way a schema silently diverges between a developer's database and production.
  */
final case class MigrationReport(
  initialVersion: Option[String],
  targetVersion: Option[String],
  executed: Int,
  applied: Vector[String]
)

/** Programmatic Flyway, callable from a service's startup and from a test.
  *
  * **ferrite owns the migrations** (ADR §1) — cobalt and wolfram never run them, so there is exactly one writer of the
  * schema and no ordering question at deploy time. The same entry point is reused by the `db-migrate` one-shot in
  * compose, which runs the ferrite image in migrate mode rather than pulling the 380 MB `flyway/flyway` image for what
  * is one JDBC call.
  *
  * Two configuration choices are load-bearing:
  *
  *   - `validateOnMigrate` stays **on**. It is what turns "someone edited `V1__events.sql` after it shipped" into a
  *     failed startup instead of a database whose schema does not match the file that claims to describe it.
  *   - `cleanDisabled` is **on**. `Flyway.clean()` drops every object in the schema; there is no operational reason
  *     this codebase ever needs it, and leaving it available means a mis-typed command can destroy the event store.
  *
  * The history table deliberately lands in the connection's default schema (`public`) rather than in `events`:
  * `V1__events.sql` is what creates `events`, and a history table living inside the schema its own migrations create
  * makes the first run a chicken-and-egg problem that Flyway papers over with `createSchemas`.
  */
object Migrations:

  /** Migrations ship inside this module's jar, so the schema and the code that queries it version together and there is
    * no separate artefact to forget to deploy.
    */
  val Location: String = "classpath:db/migration"

  /** Applies every pending migration. Idempotent: a second call on an up-to-date database executes nothing and reports
    * `executed = 0`, which is what makes it safe to run on every boot of every replica.
    */
  def migrate(dataSource: DataSource): MigrationReport =
    val result = flyway(dataSource).migrate()
    MigrationReport(
      initialVersion = Option(result.initialSchemaVersion),
      targetVersion = Option(result.targetSchemaVersion),
      executed = result.migrationsExecuted,
      applied = result.migrations.asScala.toVector.map(_.version)
    )

  /** Checks the applied history against the migrations on the classpath, without applying anything.
    *
    * Throws `FlywayException` on mismatch. Deliberately not an `Either`: every caller's response to "the database does
    * not match this build" is to refuse to start, and an exception is the one signal a `main` cannot accidentally
    * ignore.
    */
  def validate(dataSource: DataSource): Unit = flyway(dataSource).validate()

  /** The versions this database has applied, oldest first — the baseline a test pins. */
  def appliedVersions(dataSource: DataSource): Vector[String] =
    flyway(dataSource).info.applied.toVector.map(info => info.getVersion.getVersion)

  private def flyway(dataSource: DataSource): Flyway =
    Flyway
      .configure()
      .dataSource(dataSource)
      .locations(Location)
      .validateOnMigrate(true)
      .cleanDisabled(true)
      // Out-of-order application would let a migration merged later slot in behind one already applied, so two
      // databases with the same version list could have had their DDL applied in different orders.
      .outOfOrder(false)
      // No baselining: an existing database with no history table is not a database this build should adopt.
      .baselineOnMigrate(false)
      .load()
