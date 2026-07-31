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

package com.worxbend.observability

import scala.concurrent.duration.*

/** The shared metric vocabulary (ADR §7.1).
  *
  * Every meter name and tag key the three services have in common lives here as a constant, for one reason: a Grafana
  * dashboard is written against strings, and a string typed twice is a string that eventually differs. If wolfram
  * counts `ingest.events.received` and cobalt counts `ingest.event.received`, nothing fails — the panel just goes flat
  * for one service, which is the failure mode that takes longest to notice.
  *
  * Names follow Micrometer's dot-delimited convention, **not** Prometheus's underscores: the registry's naming
  * convention performs that translation at scrape time, and hard-coding the Prometheus spelling here would break the
  * day a second registry is added.
  *
  * Cardinality is the standing hazard. Tag *keys* are listed here; the values a caller may attach are constrained
  * either by an enumeration below or by a Micrometer filter in [[Telemetry]]. Anything derived from user input — a URI
  * path, an event id, a device serial — is not a tag.
  */
object Meters:

  /** Tag keys. Shared, because a panel that groups by `reason` in one service and `cause` in another cannot be one
    * panel.
    */
  object TagKeys:
    /** Deployment unit. Applied as a common tag by [[Telemetry]]; never set per-meter. */
    val Service: String = "service"

    /** Build version. Applied as a common tag by [[Telemetry]]. */
    val Version: String = "version"

    /** Replica. See [[Telemetry.commonTags]] for why this is not applied by default. */
    val Instance: String = "instance"

    /** CloudEvents `type`. Bounded by the event taxonomy, so safe; still capped in [[Telemetry]] against a misbehaving
      * producer inventing types.
      */
    val EventType: String = "type"

    /** CloudEvents Kafka content mode — see [[Modes]]. Two values, ever. */
    val Mode: String = "mode"

    /** Why something was rejected, dropped or failed — see [[Reasons]]. Must come from a closed set: this is the tag
      * most likely to be filled with an exception message, which is unbounded.
      */
    val Reason: String = "reason"

    /** Kafka topic. */
    val Topic: String = "topic"

    /** Kafka partition, as a decimal string. Bounded by the partition count. */
    val Partition: String = "partition"

    /** Kafka consumer group. */
    val Group: String = "group"

    /** The *shape* of a search query (which filter families it uses), not the query itself — see
      * [[Meters.SearchQueryDuration]].
      */
    val Shape: String = "shape"

    /** Matched route template, e.g. `/events/:id`. Never the raw path (ADR §7.1). */
    val Route: String = "http.route"

    /** Micrometer's own tag on `http.server.requests`. Named by the framework, repeated here so the cardinality cap in
      * [[Telemetry]] and the services' interceptors agree on the spelling.
      */
    val Uri: String = "uri"

    /** Success/failure of an operation — see [[Outcomes]]. */
    val Outcome: String = "outcome"

    /** Which scheduled maintenance job — see [[Jobs]]. A closed set of two, so one timer family covers both without
      * either one hiding the other's failures behind the other's volume.
      */
    val Job: String = "job"

  /** Closed value set for [[TagKeys.Mode]]: the CloudEvents Kafka content modes. */
  object Modes:
    val Binary: String = "binary"
    val Structured: String = "structured"

  /** Closed value set for [[TagKeys.Outcome]]. Four values, and never the exception class name.
    *
    * [[Skipped]] is the one that needs justifying. A replica that loses the race for a maintenance job's advisory lock
    * neither succeeded nor failed: folding it into `success` makes "every replica is doing the work" indistinguishable
    * from "one replica is doing the work", and folding it into `failure` pages somebody every cycle for the system
    * behaving exactly as designed. With N replicas the healthy steady state is one `success` and N-1 `skipped` per
    * tick, and that ratio is itself the signal that the locking works.
    */
  object Outcomes:
    val Success: String = "success"
    val Failure: String = "failure"
    val Duplicate: String = "duplicate"
    val Skipped: String = "skipped"

  /** Closed value set for [[TagKeys.Job]]. */
  object Jobs:
    /** Rolling monthly partition creation and retention. */
    val Partitions: String = "partition-maintenance"

    /** `REFRESH MATERIALIZED VIEW CONCURRENTLY` on the hourly rollup. */
    val Rollup: String = "rollup-refresh"

  /** Closed value set for [[TagKeys.Reason]].
    *
    * The rule this encodes: a rejection reason is a *category the operator can act on*, not a diagnostic. The
    * diagnostic belongs on the log line (via [[LogContext.kv]]), where cardinality costs nothing.
    */
  object Reasons:
    /** The bytes were not valid CloudEvents JSON / the binary headers were incomplete. */
    val Malformed: String = "malformed"

    /** Structurally valid CloudEvents, but a required attribute was missing or ill-typed. */
    val InvalidAttributes: String = "invalid-attributes"

    /** Valid envelope, but `data` did not match the schema the `type` implies — the stage-two decode failure of ADR
      * §4.2.
      */
    val InvalidPayload: String = "invalid-payload"

    /** A `type` no consumer in this deployment recognises. Expected and benign in a rolling deploy; a persistent
      * non-zero rate means a producer shipped before its consumer.
      */
    val UnknownType: String = "unknown-type"

    /** Payload exceeded the configured byte ceiling. */
    val TooLarge: String = "too-large"

    /** The record could not be persisted after retries and went to the DLQ. */
    val Unpersistable: String = "unpersistable"

  // --- wolfram: ingestion ------------------------------------------------------------------------------------------

  /** Counter. Accepted CloudEvents, tagged [[TagKeys.EventType]] and [[TagKeys.Mode]]. Incremented *after* validation,
    * so `received - rejected` is not the arrival rate; that is intentional — arrival rate is `http.server.requests`.
    */
  val IngestReceived: String = "ingest.events.received"

  /** Counter. Rejected CloudEvents, tagged [[TagKeys.Reason]]. */
  val IngestRejected: String = "ingest.events.rejected"

  /** Timer. Broker acknowledgement latency for a produce, tagged [[TagKeys.Topic]] and [[TagKeys.Outcome]].
    *
    * A timer and not a counter because the p99 is the signal: a rising median here is broker pressure, while a rising
    * p99 with a flat median is one slow partition leader.
    */
  val KafkaProduceLatency: String = "kafka.produce.latency"

  // --- cobalt: consumption -----------------------------------------------------------------------------------------

  /** Distribution summary. Records per poll. Small batches under load mean the consumer is starved, not saturated — a
    * distinction `consume.records.persisted` alone cannot make.
    */
  val ConsumeBatchSize: String = "consume.batch.size"

  /** Counter. Rows written, tagged [[TagKeys.EventType]]. */
  val ConsumePersisted: String = "consume.records.persisted"

  /** Counter. Records the idempotent upsert recognised as already stored.
    *
    * Non-zero is *normal* — the pipeline is at-least-once — so this is a rate to watch for spikes, never an error
    * count. Kept separate from `persisted` so redelivery storms are visible without inflating throughput.
    */
  val ConsumeDuplicate: String = "consume.records.duplicate"

  /** Timer. Wall-clock latency of one batch insert, tagged [[TagKeys.Outcome]].
    *
    * A timer for the same reason [[KafkaProduceLatency]] is one: a rising median is database pressure, a rising p99
    * over a flat median is one oversized payload or one hot partition.
    */
  val ConsumeBatchLatency: String = "consume.batch.latency"

  /** Counter. Records routed to the DLQ, tagged [[TagKeys.Reason]]. Any sustained non-zero rate is a page. */
  val ConsumePoison: String = "consume.records.poison"

  /** Gauge (Micrometer `MultiGauge`), tagged [[TagKeys.Group]], [[TagKeys.Topic]] and [[TagKeys.Partition]].
    *
    * Fed from an `AdminClient` comparing committed offsets against `listOffsets(LATEST)` — **not** from the consumer's
    * own `records-lag-max` (ADR §7.1). The client metric only covers partitions currently being fetched, so it reads
    * zero during a rebalance and disappears entirely when the consumer is down: precisely the two moments lag matters.
    */
  val ConsumerLag: String = "consume.group.lag"

  // --- shared: the event taxonomy ---------------------------------------------------------------------------------

  /** Counter. A CloudEvents `type` that decoded structurally but has no domain refinement, tagged [[TagKeys.EventType]]
    * and [[TagKeys.Reason]]. Emitted by whichever service met it first, which is why it lives here rather than under an
    * `ingest.` or `consume.` prefix.
    */
  val EventUnrecognised: String = "event.unrecognised"

  // --- ferrite: search ---------------------------------------------------------------------------------------------

  /** Timer. End-to-end search latency, tagged [[TagKeys.Shape]].
    *
    * Tagged by query *shape* (which filter families were present) and never by the filter values: shape is bounded by
    * the grammar, values are user input. Shape is also the only grouping that makes the timer actionable — "queries
    * with a free-text term and a time range are slow" is a fixable statement.
    */
  val SearchQueryDuration: String = "search.query.duration"

  /** Counter. A facet result hit the display cap and was truncated. Rising means the cap is now hiding information
    * users need, which is a product signal, not an error.
    */
  val SearchFacetsCapped: String = "search.facets.capped"

  // --- shared: storage ---------------------------------------------------------------------------------------------

  /** Gauge. Rows in the default partition of `events.cloud_event`.
    *
    * **Must stay 0** (ADR §7.1). Anything else means the partition-maintenance job stopped creating future partitions
    * and rows are landing in the catch-all, where they are invisible to partition pruning and expensive to move back
    * out. Alert on `> 0`, not on a rate.
    */
  val PartitionDefaultRows: String = "partition.default.rows"

  // --- shared: scheduled database maintenance ----------------------------------------------------------------------

  /** Timer. One scheduled maintenance run, tagged [[TagKeys.Job]] and [[TagKeys.Outcome]].
    *
    * **The meter that makes a silently dead job visible.** Partition creation and rollup refresh share the property
    * that failing produces no error anywhere a user or an alert can see it: the partition job stops creating months and
    * nothing happens until a month boundary, at which point ingest fails completely; the refresh job stops running and
    * dashboards keep serving numbers that are merely old. Both failure modes are invisible in logs unless somebody is
    * reading them. A timer with an `outcome` tag makes three different alerts expressible on one meter family — "the
    * failure rate is non-zero", "no run of any outcome in the last N intervals" (the job thread is dead, and a counter
    * that stops incrementing is the only trace it leaves), and "the p99 duration is approaching the interval", which is
    * ADR §12.4's tripwire for retiring the materialized view in favour of incremental merge tables.
    *
    * A timer rather than a counter plus a gauge because Micrometer's timer already exposes the count, so a run that is
    * timed is a run that is counted, and there is no way to record one without the other.
    */
  val MaintenanceDuration: String = "maintenance.job.duration"

  /** Counter. Monthly partitions created, cumulative. Its *rate* is the interesting reading and it should be a step
    * function of roughly one per month; a flat zero for longer than that means the job is running and achieving
    * nothing, which [[MaintenanceDuration]] alone cannot distinguish from a healthy no-op.
    */
  val MaintenancePartitionsCreated: String = "maintenance.partitions.created"

  /** Counter. Partitions detached by the retention policy. Detached, never dropped — see
    * `com.worxbend.persistence.maintenance.PartitionMaintenance` for why that asymmetry is deliberate — so this counts
    * tables that still exist and still hold their rows.
    */
  val MaintenancePartitionsDetached: String = "maintenance.partitions.detached"

  /** Gauge. Consecutive months from the current one that already have a partition — ADR §5's "months of headroom".
    *
    * The leading indicator that [[PartitionDefaultRows]] is the lagging one for: headroom decaying towards zero is a
    * job that has stopped working, and it is visible weeks before the first row lands in the catch-all. Alert below
    * two, not below one — one means the fix has to happen this month.
    */
  val MaintenancePartitionHeadroom: String = "maintenance.partitions.headroom"

  /** Gauge. Months whose partition cannot be created because the `DEFAULT` partition already holds rows for them.
    *
    * Non-zero means a *manual* remedy is required and the job has said so in its logs with the exact statements to run:
    * PostgreSQL refuses an overlapping partition while matching rows sit in `DEFAULT`, and moving them is an
    * exclusive-lock operation no unattended job should take on an operator's behalf.
    */
  val MaintenancePartitionsBlocked: String = "maintenance.partitions.blocked"

  // --- shared: HTTP ------------------------------------------------------------------------------------------------

  /** Micrometer's HTTP server timer name.
    *
    * The enforceable rule of ADR §7.1: exactly one component times each request and it is always Micrometer, so this
    * family has identical names and tags across Play, Vert.x/Tapir and Cask, and one dashboard works everywhere. Never
    * introduce a framework-native equivalent (`request_total`, `http_requests_total`, …) alongside it.
    */
  val HttpServerRequests: String = "http.server.requests"

  /** Prometheus scrape endpoint, mounted identically by all three services so one scrape config covers the fleet. */
  val MetricsPath: String = "/metrics"

  /** Liveness/readiness endpoint. */
  val HealthPath: String = "/health"

  /** Endpoints excluded from [[HttpServerRequests]].
    *
    * Scrapes and health probes are the highest-frequency requests a service serves and are not user traffic; leaving
    * them in inflates request rate, drags down latency percentiles, and gives the scrape endpoint a timeseries
    * describing itself.
    */
  val UninstrumentedPaths: Set[String] = Set(MetricsPath, HealthPath)

  // --- shared: latency distributions -------------------------------------------------------------------------------

  /** The bucket boundaries every timer and distribution summary in this system publishes.
    *
    * **Why this exists at all.** A Micrometer `Timer` with no distribution configuration reaches Prometheus as three
    * series — `_count`, `_sum` and `_max` — and nothing else. `_sum / _count` is a mean, which for latency is the one
    * statistic that reliably lies: it is dominated by the fast majority and moves barely at all when the slow tail
    * doubles. `_max` is the opposite failure, a single unsmoothed sample. Neither can answer "what does the 99th
    * percentile user see", and `histogram_quantile` cannot be asked without `_bucket` series to ask it of. Until these
    * ladders were installed, every percentile panel in the Grafana dashboard was unwritable.
    *
    * **Why explicit ladders and not `publishPercentileHistogram`.** Micrometer's automatic percentile histogram emits a
    * generated ladder — on the order of 70 buckets for a timer left unbounded — and the count varies with the
    * configured min/max. Bucket count multiplies by every tag combination on the meter, and the resulting cardinality
    * is then a property of a library default rather than of a decision anyone made. A hand-written ladder is a fixed,
    * auditable number: the entries below plus the implicit `+Inf`.
    *
    * **Why not client-side percentiles (`publishPercentiles`).** Those arrive as pre-aggregated `quantile` labels which
    * cannot be summed across replicas — averaging p99s is meaningless — and this system scrapes per replica. Buckets
    * aggregate correctly, which is the whole reason Prometheus prefers them.
    *
    * **Choosing boundaries.** Each ladder brackets the range where a decision changes: dense where the meter normally
    * sits, so the median and p99 land in different buckets, and extended past the point where the operator would act,
    * so a pathological tail is still visible rather than collapsed into `+Inf`. Boundaries are approximate by nature —
    * `histogram_quantile` interpolates within a bucket — so the goal is bracketing, not precision.
    */
  object Buckets:

    /** [[HttpServerRequests]], for all three services.
      *
      * The widest range here, because it covers a Vert.x endpoint that should answer in single-digit milliseconds and a
      * server-rendered search page that legitimately takes a second. 5ms is below anything worth distinguishing; 10s is
      * past every client timeout in the stack.
      *
      * This is also the ladder that costs the most: it multiplies whatever `uri` cardinality survives
      * [[Telemetry.MaxUriTagValues]], times the distinct status codes. With the handful of route templates the three
      * services actually serve that is a few hundred series each — the cap exists so that stays true after a mistake.
      */
    val HttpServerRequests: Seq[FiniteDuration] =
      Seq(
        5.millis,
        10.millis,
        25.millis,
        50.millis,
        100.millis,
        250.millis,
        500.millis,
        1.second,
        2500.millis,
        5.seconds,
        10.seconds
      )

    /** [[SearchQueryDuration]]. Starts at 10ms — a search that fast was served entirely from cache or matched nothing —
      * and shares the HTTP ladder's ceiling, since a search is what makes an HTTP request slow.
      */
    val SearchQueryDuration: Seq[FiniteDuration] =
      Seq(
        10.millis,
        25.millis,
        50.millis,
        100.millis,
        250.millis,
        500.millis,
        1.second,
        2500.millis,
        5.seconds,
        10.seconds
      )

    /** [[KafkaProduceLatency]]. An order of magnitude faster than HTTP: an in-datacentre `acks=all` produce is a
      * single-digit millisecond operation, so the interesting resolution is at the bottom. The 2.5s ceiling is
      * `delivery.timeout.ms` territory — past it the produce is failing, not slow.
      */
    val KafkaProduceLatency: Seq[FiniteDuration] =
      Seq(
        1.milli,
        2500.micros,
        5.millis,
        10.millis,
        25.millis,
        50.millis,
        100.millis,
        250.millis,
        500.millis,
        1.second,
        2500.millis
      )

    /** [[ConsumeBatchLatency]]. A batch insert of up to a few hundred rows; the 5s ceiling is where the batch is
      * blocking the consumer long enough to threaten `max.poll.interval.ms` and provoke a rebalance.
      */
    val ConsumeBatchLatency: Seq[FiniteDuration] =
      Seq(
        5.millis,
        10.millis,
        25.millis,
        50.millis,
        100.millis,
        250.millis,
        500.millis,
        1.second,
        2500.millis,
        5.seconds
      )

    /** [[MaintenanceDuration]]. Seconds to minutes, not milliseconds: creating a partition is instant, while
      * `REFRESH MATERIALIZED VIEW CONCURRENTLY` scales with the table. The top boundaries are the ones that matter —
      * ADR §12.4 retires the materialized view when the refresh p99 approaches the refresh interval, and that tripwire
      * is only expressible because 5m and 15m are boundaries here.
      */
    val MaintenanceDuration: Seq[FiniteDuration] =
      Seq(100.millis, 500.millis, 1.second, 5.seconds, 15.seconds, 30.seconds, 1.minute, 5.minutes, 15.minutes)

    /** [[ConsumeBatchSize]] — a distribution summary, so these are record counts, not durations.
      *
      * Deliberately dense at the low end. The reading this meter exists for is "the consumer is starved, not
      * saturated", which is a *shift* from large batches to batches of one or two; a ladder that started at 50 would
      * show that as a flat line at the bottom bucket.
      */
    val ConsumeBatchSize: Seq[Double] = Seq(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000)

    /** Every timer family and its ladder. [[Telemetry]] installs one `MeterFilter` per entry.
      *
      * A map rather than a filter written per meter so that adding a timer to [[Meters]] and forgetting to give it
      * buckets is visible as an absence from one list, rather than as a panel nobody can write six months later.
      */
    val timers: Map[String, Seq[FiniteDuration]] =
      Map(
        Meters.HttpServerRequests -> HttpServerRequests,
        Meters.SearchQueryDuration -> SearchQueryDuration,
        Meters.KafkaProduceLatency -> KafkaProduceLatency,
        Meters.ConsumeBatchLatency -> ConsumeBatchLatency,
        Meters.MaintenanceDuration -> MaintenanceDuration
      )

    /** Every distribution summary family and its ladder. */
    val summaries: Map[String, Seq[Double]] =
      Map(Meters.ConsumeBatchSize -> ConsumeBatchSize)
