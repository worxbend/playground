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

package io.kzonix.observability

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

  /** Closed value set for [[TagKeys.Mode]]: the CloudEvents Kafka content modes. */
  object Modes:
    val Binary: String = "binary"
    val Structured: String = "structured"

  /** Closed value set for [[TagKeys.Outcome]]. Deliberately three values and not the exception class name. */
  object Outcomes:
    val Success: String = "success"
    val Failure: String = "failure"
    val Duplicate: String = "duplicate"

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
