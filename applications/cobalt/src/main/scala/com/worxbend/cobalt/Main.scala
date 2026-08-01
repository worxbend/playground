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
import com.worxbend.kernel.event.Source
import com.worxbend.observability.Telemetry
import com.worxbend.persistence.Database
import com.worxbend.persistence.DatabaseConfig
import com.worxbend.persistence.Migrations
import com.worxbend.persistence.maintenance.PartitionMaintenance
import com.worxbend.persistence.maintenance.RollupRefresh
import com.worxbend.persistence.repository.PostgresCheckpointStore
import com.worxbend.persistence.repository.PostgresEventRepository
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import org.apache.kafka.clients.admin.Admin
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.pattern.after
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Everything cobalt owns, constructed once and torn down in the reverse order.
  *
  * **A composition root and not a container.** The graph is a dozen objects with a construction order that is itself
  * load-bearing — telemetry before anything that meters, migrations before the repository that assumes the schema, the
  * consumer last because it starts doing work the moment it exists. Guice would hide exactly that ordering, and cobalt
  * has no other use for it.
  *
  * **Teardown order is the interesting half, and it is expressed as `CoordinatedShutdown` phases rather than as a
  * `close()` sequence.** A SIGTERM must:
  *
  *   1. unbind the admin listener, so an orchestrator's readiness probe starts failing and traffic stops being routed
  *      here (`PhaseServiceUnbind`);
  *   2. drain the Kafka consumer — finish the in-flight batch, write it, commit it (`PhaseServiceRequestsDone`,
  *      registered by [[EventConsumer]]);
  *   3. only then close the DLQ producer, the admin client, the connection pools and finally telemetry
  *      (`PhaseBeforeActorSystemTerminate`).
  *
  * Closing telemetry first is the common mistake, and it makes the drain — the one part of shutdown that can lose data
  * — the one part with no metrics and no spans.
  */
final class CobaltApp private (
  val adminServer: AdminServer,
  val supervisor: ConsumerSupervisor,
  system: ActorSystem
):

  /** The port the admin listener actually bound. */
  def port: Int = adminServer.boundPort

  /** Runs the full `CoordinatedShutdown` sequence and waits for the actor system to stop. Used by tests; production
    * gets here through the JVM shutdown hook Pekko installs.
    */
  def shutdown(): Future[Done] = CoordinatedShutdown(system).run(CoordinatedShutdown.UnknownReason)

object CobaltApp extends StrictLogging:

  /** cobalt's identity on the DLQ. "Which consumer gave up on this record" is the first question asked of a dead
    * letter, and the default in `modules/eventing` deliberately does not answer it.
    */
  val DeadLetterSource: Source =
    Source("urn:worxbend:cobalt").getOrElse(throw IllegalStateException("the cobalt dead-letter source is invalid"))

  /** Wires and starts everything. Blocks only for the things that must succeed before the process claims to be up:
    * opening the pools, running the migrations, and binding the listener.
    */
  def start(config: CobaltConfig, database: DatabaseConfig, telemetry: Telemetry): CobaltApp =
    // `hikaricp.connections.*` on the same registry every other cobalt meter uses. On the write side the reading that
    // matters is `pending`: a consumer whose batch inserts are queueing for a connection has a lag problem the Kafka
    // meters will report and the database meters will explain.
    val pools = Database.open(database, Some(MicrometerMetricsTrackerFactory(telemetry.registry)))

    // Migrations run here, before anything reads or writes. cobalt is the write side of this system, so it is the
    // service that must not start against a schema older than its own inserts. `Migrations.migrate` is idempotent, so
    // every replica running it on every boot converges rather than conflicting, and Flyway's own lock serialises them.
    val report = Migrations.migrate(pools.write.get())
    logger.info(s"schema at ${report.targetVersion.getOrElse("<none>")}; ${report.executed} migration(s) applied")

    // Bounded and sized to the write pool (ADR §0, decision 8): more threads than connections only moves the queue
    // into `getConnection`, where the pool's timeout stops being the thing that sheds load.
    val blocking = ExecutionContext.fromExecutorService(
      Executors.newFixedThreadPool(database.write.maximumPoolSize, daemon("cobalt-jdbc"))
    )
    val repository = PostgresEventRepository(pools.read.transactor, pools.write.transactor)(using blocking)

    given system: ActorSystem = ActorSystem("cobalt")
    given ExecutionContext = system.dispatcher

    val health = HealthChecks.create()
    val metrics = ConsumerMetrics(telemetry.registry)
    val admin = Admin.create(adminProperties(config).asJava)
    val probes = Probes(
      offsets = AdminOffsets(admin, config.lag.requestTimeout),
      gauge = ConsumerLagGauge(telemetry.registry, config.consumer.groupId),
      groupId = config.consumer.groupId,
      dataSource = pools.read.get(),
      health = health,
      validationTimeout = database.read.validationTimeout
    )
    val poller = Probes.start(probes, config.lag.refreshInterval)

    // Before the consumer exists, not after. A fresh deployment of this build has whatever partitions
    // `V1__events.sql` created and nothing else, so the first batch through the stream would either fail with
    // "no partition of relation found for row" or land in the default partition — where it is invisible to partition
    // pruning and where its presence then blocks the partition this job is about to try to create.
    val maintenance = MaintenanceJobs.start(
      partitions = PartitionMaintenance(pools.write.get(), config.maintenance.policy),
      rollup = RollupRefresh(pools.write.get()),
      metrics = MaintenanceMetrics(telemetry.registry),
      config = config.maintenance,
      scheduler = system.scheduler
    )(using blocking)

    val deadLetters = KafkaDeadLetterPublisher.start(config.consumer)
    val decoder = RecordDecoder(DeadLetterSource, telemetry.tracing.tracer)
    val processor = BatchProcessor(
      repository = repository,
      deadLetters = deadLetters,
      metrics = metrics,
      source = DeadLetterSource,
      attempts = config.consumer.writeAttempts,
      backoff = () => after(config.consumer.retryDelay, system.scheduler)(Future.unit),
      // The externalised offsets. `owner` is the container id, so a checkpoint that stops advancing names the
      // replica that owned the partition when it did — the first thing worth knowing after a rebalance.
      checkpoint = Some(
        BatchProcessor.Checkpointing(config.consumer.groupId, sys.env.get("HOSTNAME").filter(_.nonEmpty))
      )
    )

    // The consumer behind a supervisor rather than started directly. `EventConsumer.start` is now a *factory* the
    // supervisor calls once per start or resume — a drained stream is not restartable, so every resume needs a fresh
    // one, and the supervisor is the thing that knows when.
    val supervisor = ConsumerSupervisor(
      factory = () => EventConsumer.start(config.consumer, config.restart, decoder, processor),
      offsets = AdminOffsets(admin, config.lag.requestTimeout),
      checkpoints = PostgresCheckpointStore(pools.read.transactor, pools.write.transactor)(using blocking),
      groupId = config.consumer.groupId,
      topic = config.consumer.topic
    )
    val started = Await.result(supervisor.start(), ConsumerSupervisor.DrainTimeout)
    logger.info(s"consumer supervisor: ${started.status.state.name}")

    // The read/replay half of the DLQ, with its own consumer and producer. Built after the write half and closed
    // before it, so a replay can never outlive the publisher whose records it is putting back.
    val deadLetterStore = KafkaDeadLetterStore.start(config.consumer, config.replay)
    val deadLetterAdmin = DeadLetterAdmin(
      store = deadLetterStore,
      metrics = ReplayMetrics(telemetry.registry),
      config = config.replay,
      ownTopic = config.consumer.topic,
      dlqTopic = config.consumer.dlqTopic
    )

    val routes = CobaltRoutes(
      AdminHandlers(telemetry, health, deadLetterAdmin, SupervisorAdmin(supervisor, ConsumerSupervisor.DrainTimeout))
    )
    val adminServer = AdminServer(routes, config.server.host, config.server.port)
    adminServer.start()

    val shutdown = CoordinatedShutdown(system)
    shutdown.addTask(CoordinatedShutdown.PhaseServiceUnbind, "stop-admin-server"): () =>
      Future:
        quietly("stopping the admin server")(adminServer.stop())
        Done
    shutdown.addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close-resources"): () =>
      Future:
        quietly("stopping the probe scheduler")(poller.close())
        // Cancelled before the pools close, or a maintenance tick lands on a closed DataSource during shutdown and
        // logs a failure that is nothing but the shutdown itself.
        quietly("cancelling the maintenance schedule")(maintenance.close())
        // After the admin listener has been unbound in PhaseServiceUnbind, so an in-flight replay has already finished
        // or been refused a connection; closing these while a produce is in flight would fail it at the last step.
        quietly("closing the dead-letter reader")(deadLetterStore.close())
        quietly("closing the dead-letter producer")(deadLetters.close())
        quietly("closing the Kafka admin client")(admin.close())
        quietly("closing the connection pools")(pools.close())
        quietly("shutting down the JDBC dispatcher")(blocking.shutdown())
        quietly("closing telemetry")(telemetry.close())
        Done

    CobaltApp(adminServer, supervisor, system)

  /** The admin client's settings. Only what it needs: this client exists for lag and reachability, and every extra
    * property is another way for a monitoring path to fail differently from the data path.
    */
  def adminProperties(config: CobaltConfig): Map[String, AnyRef] =
    Map[String, AnyRef](
      "bootstrap.servers" -> config.consumer.bootstrapServers,
      "client.id" -> s"${config.consumer.groupId}-lag",
      "request.timeout.ms" -> config.lag.requestTimeout.toMillis.toString,
      "default.api.timeout.ms" -> config.lag.requestTimeout.toMillis.toString
    )

  private def quietly(what: String)(action: => Unit): Unit =
    try action
    catch case NonFatal(error) => logger.warn(s"$what failed during shutdown", error)

  private def daemon(name: String): ThreadFactory =
    runnable =>
      val thread = Thread(runnable, name)
      thread.setDaemon(true)
      thread

/** The process entry point. */
object Main extends StrictLogging:

  /** The `service` tag on every meter and the `service.name` resource attribute on every span. */
  val ServiceName: String = "cobalt"

  def main(args: Array[String]): Unit =
    val _ = args
    val source = ConfigSource.default
    val config = CobaltConfig.load(source).fold(unusable("cobalt"), identity)
    val database = DatabaseConfig.load(source).fold(unusable("cobalt database"), identity)

    val telemetry = Telemetry.start(ServiceName)
    val app = CobaltApp.start(config, database, telemetry)
    logger.info(s"cobalt started; admin endpoints on port ${app.port}")

  /** Refuses the boot rather than starting with a configuration nobody can read.
    *
    * A consumer that boots with the wrong topic name is the least debuggable failure in this system — it starts
    * cleanly, reports itself live, and receives nothing — so an unreadable configuration must stop the process here and
    * not be papered over with a default.
    */
  private def unusable(what: String): ConfigReaderFailures => Nothing =
    failures => throw IllegalStateException(s"$what configuration is unusable: ${failures.prettyPrint()}")
