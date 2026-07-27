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

package io.kzonix.ferrite.wiring

import io.kzonix.ferrite.search.SearchExecutionContext
import io.kzonix.observability.Telemetry
import io.kzonix.observability.TelemetryConfig
import io.kzonix.persistence.Database
import io.kzonix.persistence.DatabaseConfig
import io.kzonix.persistence.repository.EventRepository
import io.kzonix.persistence.repository.PostgresEventRepository
import jakarta.inject.Inject
import jakarta.inject.Provider
import jakarta.inject.Singleton
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import pureconfig.ConfigSource
import scala.concurrent.Future

/** Whether this replica can serve a search right now.
  *
  * An interface with one method rather than a direct dependency on [[Database]] from the controller, so the readiness
  * probe can be driven in a test without a JDBC connection — a health check that can only be tested against a real
  * database is a health check nobody tests.
  */
trait Readiness:

  def check(): Future[Boolean]

/** The connection pools, opened once at boot and closed on shutdown.
  *
  * **ferrite never migrates.** ADR §1 assigns the Flyway migrations to this service's *repository module*, but running
  * them is a deployment step, not something a web replica does on boot: three replicas starting together would race on
  * the schema-history table, and a migration that fails halfway leaves a replica serving traffic against a schema it
  * cannot describe. `modules/persistence` exposes `Migrations` for a deliberate operator-run job; nothing here calls it,
  * and that is the whole of ferrite's relationship with DDL.
  *
  * Opening eagerly is deliberate: `HikariPool.open` keeps Hikari's default `initializationFailTimeout`, so an
  * unreachable database fails at boot rather than on the first user request. A ferrite that starts without a database
  * is a ferrite that reports itself healthy and serves errors.
  *
  * The stop hook is the only shutdown path. Pools hold server-side sessions and a restart loop that leaks them
  * exhausts `max_connections` for every *other* service too.
  */
@Singleton
final class Databases @Inject() (configuration: Configuration, lifecycle: ApplicationLifecycle):

  /** The parsed configuration, or a boot failure naming every field that was wrong.
    *
    * Read through the running application's `Configuration` rather than `ConfigSource.default` so that a test or an
    * integration run can override `database.jdbc-url` with `GuiceApplicationBuilder.configure(...)` and have it
    * honoured — `ConfigSource.default` re-reads the classpath and would silently ignore the override.
    */
  val config: DatabaseConfig =
    DatabaseConfig
      .load(ConfigSource.fromConfig(configuration.underlying))
      .fold(failures => throw IllegalStateException(s"ferrite: unusable database configuration: ${failures.prettyPrint()}"), identity)

  val database: Database = Database.open(config)

  private val _ = lifecycle.addStopHook(() => Future.successful(database.close()))

/** Readiness backed by a real connection from the read pool.
  *
  * `Connection.isValid` and not `SELECT 1`: the driver implements it as a protocol-level check with a timeout the pool
  * cannot swallow, and it does not consume a statement slot on a pool whose whole budget is a two-second
  * `statement_timeout`. The check runs on the search dispatcher because `getConnection` blocks — a probe that parks a
  * request thread is a probe that takes the service down while reporting on it.
  */
@Singleton
final class DatabaseReadiness @Inject() (databases: Databases)(using ec: SearchExecutionContext) extends Readiness:

  /** Seconds. Shorter than the pool's `connectionTimeout`, so a saturated pool answers "not ready" instead of hanging
    * the probe until the orchestrator's own timeout fires and reports something less specific.
    */
  private val ValidationSeconds: Int = 1

  def check(): Future[Boolean] =
    Future:
      val connection = databases.database.read.get().getConnection
      try connection.isValid(ValidationSeconds)
      finally connection.close()

/** The read-side repository, bound to the search pool and the bounded search dispatcher.
  *
  * Both transactors point at the *read* pool. ferrite is a reader (ADR §1) and `EventRepository.insertAll` is cobalt's
  * half of the interface; wiring the write pool in here would give this service a way to write that nothing needs and
  * that the read pool's `read-only = true` session default exists to prevent. If ferrite ever gains a write path, this
  * line — not a scattered `getConnection` — is where that decision becomes visible.
  */
@Singleton
final class EventRepositoryProvider @Inject() (databases: Databases, ec: SearchExecutionContext)
    extends Provider[EventRepository]:

  private lazy val repository: EventRepository =
    PostgresEventRepository(databases.database.read.transactor, databases.database.read.transactor)(using ec)

  def get(): EventRepository = repository

/** Metrics and traces, started once and closed on shutdown.
  *
  * Traces come from `OTEL_*` via the SDK's autoconfiguration (ADR §7.1), so there is no "tracing enabled" flag here
  * whose two branches could drift; disabling tracing is `OTEL_SDK_DISABLED=true`, which is the same switch every other
  * service in the fleet uses.
  */
@Singleton
final class TelemetryProvider @Inject() (lifecycle: ApplicationLifecycle) extends Provider[Telemetry]:

  /** The `service` tag on every meter and the `service.name` resource attribute on every span. */
  val ServiceName: String = "ferrite"

  private lazy val telemetry: Telemetry =
    val started = Telemetry.start(TelemetryConfig.fromEnv(ServiceName))
    val _ = lifecycle.addStopHook(() => Future.successful(started.close()))
    started

  def get(): Telemetry = telemetry
