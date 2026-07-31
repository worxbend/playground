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

import com.worxbend.persistence.maintenance.PartitionPolicy
import pureconfig.ConfigReader
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scala.concurrent.duration.FiniteDuration

/** Where the operational HTTP surface binds.
  *
  * cobalt serves no business endpoints (ADR §1): this listener exists only for `/metrics` and the two health probes, so
  * it is configured separately from everything that touches Kafka — a platform team owns the port, the service team
  * owns the stream.
  */
final case class ServerConfig(host: String, port: Int)

object ServerConfig:

  /** `forProductN` rather than `derives ConfigReader`, matching `modules/persistence` and wolfram: derivation needs a
    * wildcard given-import that `-Wunused:all` + `-Werror` flags spuriously, and this way the HOCON keys are literally
    * in the source beside the fields they fill.
    */
  given reader: ConfigReader[ServerConfig] = ConfigReader.forProduct2("host", "port")(ServerConfig.apply)

/** How the stream batches, retries, and gives up.
  *
  * Every field here is a correctness knob rather than a tuning one, and each is spelled out because the default that
  * would apply in its absence is wrong for this pipeline.
  *
  * @param batchSize
  *   the `groupedWithin` element bound of ADR §4.3. It is also the largest number of rows one failed insert can force
  *   the isolation search to bisect, so raising it trades steady-state throughput against poison-pill recovery time.
  * @param batchWindow
  *   the `groupedWithin` time bound. This is the *latency floor* of the whole pipeline when traffic is sparse: with one
  *   event per minute, an event is durable `batchWindow` after it arrives and not before.
  * @param writeAttempts
  *   how many times a whole batch is retried before the isolation search starts bisecting it. Retries exist to ride out
  *   a connection blip; bisecting exists to find a row the database will never accept. Conflating the two is what turns
  *   a five-second database outage into a DLQ full of perfectly good events.
  * @param retryDelay
  *   pause between those attempts. Deliberately short — the real backoff is [[RestartConfig]], which unwinds the whole
  *   consumer including its Kafka session.
  * @param commitMaxBatch
  *   how many offsets `Committer.flow` aggregates into one `commitAsync`. Committing per record is a round trip per
  *   event; committing too rarely widens the replay window after a crash.
  * @param properties
  *   free-form consumer overrides (security, DNS, tuning). `bootstrap.servers` and `group.id` are set from the typed
  *   fields above and always win, so a stray property cannot silently point the consumer at another cluster.
  */
final case class ConsumerConfig(
  bootstrapServers: String,
  topic: String,
  dlqTopic: String,
  groupId: String,
  batchSize: Int,
  batchWindow: FiniteDuration,
  writeAttempts: Int,
  retryDelay: FiniteDuration,
  commitMaxBatch: Long,
  commitMaxInterval: FiniteDuration,
  commitParallelism: Int,
  drainTimeout: FiniteDuration,
  properties: Map[String, String]
)

object ConsumerConfig:

  given reader: ConfigReader[ConsumerConfig] =
    ConfigReader.forProduct13(
      "bootstrap-servers",
      "topic",
      "dlq-topic",
      "group-id",
      "batch-size",
      "batch-window",
      "write-attempts",
      "retry-delay",
      "commit-max-batch",
      "commit-max-interval",
      "commit-parallelism",
      "drain-timeout",
      "properties"
    )(ConsumerConfig.apply)

/** Bounded exponential backoff for the consumer's `RestartSource` (ADR §4.3).
  *
  * **Bounded on purpose.** An unbounded restart loop against a broker that is never coming back looks identical, from
  * the outside, to a healthy consumer with no traffic: the process stays up, the pod stays Ready, and the only symptom
  * is lag. `maxRestarts` within `maxRestartsWithin` makes the stream *fail*, which fails readiness, which is a signal
  * an operator actually receives.
  *
  * @param randomFactor
  *   jitter. Without it every replica of a service retries in lockstep and the recovering broker is hit by a
  *   synchronised thundering herd at each backoff step.
  */
final case class RestartConfig(
  minBackoff: FiniteDuration,
  maxBackoff: FiniteDuration,
  randomFactor: Double,
  maxRestarts: Int,
  maxRestartsWithin: FiniteDuration
)

object RestartConfig:

  given reader: ConfigReader[RestartConfig] =
    ConfigReader.forProduct5(
      "min-backoff",
      "max-backoff",
      "random-factor",
      "max-restarts",
      "max-restarts-within"
    )(RestartConfig.apply)

/** Consumer-lag polling (ADR §7.1).
  *
  * @param refreshInterval
  *   how often the `AdminClient` is asked for committed and end offsets. Two admin round trips per interval per
  *   replica, so this is deliberately measured in tens of seconds and not in seconds: lag is a trend, and sampling it
  *   faster than Prometheus scrapes it buys nothing but broker load.
  * @param requestTimeout
  *   bound on each admin call. It doubles as the broker-reachability probe's timeout, so a hung controller marks the
  *   consumer not-ready instead of parking the poller thread forever.
  */
final case class LagConfig(refreshInterval: FiniteDuration, requestTimeout: FiniteDuration)

object LagConfig:

  given reader: ConfigReader[LagConfig] =
    ConfigReader.forProduct2("refresh-interval", "request-timeout")(LagConfig.apply)

/** The two scheduled database maintenance jobs (ADR §5).
  *
  * Both intervals are here rather than derived from anything, because they answer unrelated questions. The partition
  * interval is "how stale may our knowledge of the calendar be", which is measured in hours because months are long;
  * the refresh interval is "how stale may a dashboard be", which is measured in minutes because that is what a person
  * looking at a chart will tolerate.
  *
  * @param enabled
  *   the escape hatch for a deployment whose partitions are managed elsewhere. Defaults to `true`: the failure mode of
  *   forgetting to switch it on is a total ingest outage at a month boundary.
  * @param monthsAhead
  *   months beyond the current one to keep partitioned — ADR §5's N+3. Also the number of consecutive runs the job may
  *   miss before an event has nowhere to go, which is the reading that makes `3` generous rather than arbitrary.
  * @param retainMonths
  *   months to keep attached, counting back from and including the current one. `None` — the default — disables
  *   retention entirely. Detaching is the one thing here that makes data vanish from queries, so it happens because
  *   somebody configured it and never because a default did.
  * @param partitionInterval
  *   how often to re-check the calendar. Hours, not minutes: creating a partition is a `CREATE TABLE` with thirteen
  *   indexes and there is nothing to gain from discovering the new month sooner than a few hours in.
  * @param refreshInterval
  *   how often to rebuild the hourly rollup. **The floor on how stale every dashboard number is**, and simultaneously
  *   the budget the refresh must fit inside: `REFRESH ... CONCURRENTLY` is a full recompute, so when its duration
  *   approaches this value the materialized-view design is finished and ADR §12.4's incremental merge tables are the
  *   replacement. That is what `maintenance.job.duration` is watched for.
  * @param detachLockTimeout
  *   how long a detach may wait for `ACCESS EXCLUSIVE` on the fact table before giving up until the next pass. The
  *   waiting is the dangerous part: while the detach queues, every new ingest insert queues behind it.
  */
final case class MaintenanceConfig(
  enabled: Boolean,
  monthsAhead: Int,
  retainMonths: Option[Int],
  partitionInterval: FiniteDuration,
  refreshInterval: FiniteDuration,
  detachLockTimeout: FiniteDuration
):

  /** The policy half, as `modules/persistence` wants it. The intervals stay here: *when* to run is cobalt's business,
    * *what* a run does is the job's.
    */
  def policy: PartitionPolicy = PartitionPolicy(monthsAhead, retainMonths, detachLockTimeout)

object MaintenanceConfig:

  given reader: ConfigReader[MaintenanceConfig] =
    ConfigReader.forProduct6(
      "enabled",
      "months-ahead",
      "retain-months",
      "partition-interval",
      "refresh-interval",
      "detach-lock-timeout"
    )(MaintenanceConfig.apply)

/** The bounds on the dead-letter admin surface (`AdminRoutes.DlqPath`).
  *
  * Every field here exists to make an operator action *bounded*, which is the whole difference between a replay tool
  * and a way to turn a poison message into a poison-message storm.
  *
  * @param enabled
  *   whether a replay may be committed. `false` still permits dry runs and the inspection endpoints — a deployment that
  *   has switched replay off gains nothing from also blinding its operators, and would only push them back to a console
  *   consumer. Defaults to `true`: the endpoint is on the admin listener, which is not a public port, and a tool nobody
  *   can use during an incident is the failure this whole surface exists to prevent.
  * @param maxRecords
  *   the ceiling on how many dead letters one request may list or replay. A request above it is **refused, not
  *   clamped** — see [[ReplayRequest.parse]] — and it bounds the fetch as well as the produce, so a listing on a topic
  *   with a million dead letters costs the same as one on a topic with ten.
  * @param maxAttempts
  *   how many times any one record may be replayed, ever. **The bound on the poison loop:** a record that fails again
  *   comes back to the DLQ under new origin coordinates, so nothing in Kafka limits the cycle; the counter in
  *   [[ReplayHeaders.Attempt]] does. Three is generous — a defect that survives three deploys is not going to be fixed
  *   by a fourth replay.
  * @param pollTimeout
  *   how long one read of the DLQ may take before it answers with what it has. A listing that hangs is worse than a
  *   listing that is short, and this endpoint is consulted when the broker is the suspect.
  */
final case class ReplayConfig(enabled: Boolean, maxRecords: Int, maxAttempts: Int, pollTimeout: FiniteDuration)

object ReplayConfig:

  given reader: ConfigReader[ReplayConfig] =
    ConfigReader.forProduct4("enabled", "max-records", "max-attempts", "poll-timeout")(ReplayConfig.apply)

/** cobalt's whole configuration, read once by the composition root.
  *
  * One aggregate rather than four independent lookups, so a typo in any namespace refuses the boot rather than
  * surfacing the first time that subsystem is exercised — for a consumer, "the first time" can be hours after deploy.
  */
final case class CobaltConfig(
  server: ServerConfig,
  consumer: ConsumerConfig,
  restart: RestartConfig,
  lag: LagConfig,
  maintenance: MaintenanceConfig,
  replay: ReplayConfig
)

object CobaltConfig:

  /** The config namespace. */
  val Namespace: String = "cobalt"

  given reader: ConfigReader[CobaltConfig] =
    ConfigReader.forProduct6("server", "consumer", "restart", "lag", "maintenance", "replay")(CobaltConfig.apply)

  /** Loads from the ambient config, returning failures rather than throwing.
    *
    * Whether an unusable configuration is fatal is the composition root's call ([[Main]] does abort); keeping the
    * failure as a value lets a test assert on a bad configuration without killing the JVM.
    */
  def load(source: ConfigSource = ConfigSource.default): Either[ConfigReaderFailures, CobaltConfig] =
    source.at(Namespace).load[CobaltConfig]
