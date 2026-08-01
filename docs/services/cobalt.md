# cobalt — the Kafka consumer

A `pekko-connectors-kafka` committable source feeding batched idempotent inserts into PostgreSQL, with Cask
serving nothing but `/metrics` and the two health probes. Source: `applications/cobalt/`.

---

## What it is responsible for

- Consuming `events.cloudevents.v1` as group `cobalt-cloudevents-v1`, decoding each record from the CloudEvents
  Kafka binding into a `kernel` `Envelope`, and inserting batches into `events.cloud_event`.
- **Owning the schema.** cobalt is the write side, so it runs the Flyway migrations at boot (`Migrations.migrate`,
  before the repository is constructed). ferrite never does.
- Routing every record it cannot store — undecodable *or* rejected by the database — to
  `events.cloudevents.v1.dlq` in structured mode, and committing past it.
- **Serving that dead-letter queue back to an operator**: `GET /admin/dlq`, `GET /admin/dlq/records` and
  `POST /admin/dlq:replay`, so a poison record can be inspected and put back on the topic without a console
  consumer and without reconstructing it by hand.
- Committing offsets **only after** the batch is durable.
- Publishing consumer-group lag from an `AdminClient`, and answering readiness from a background poller.
- Continuing the trace that started at wolfram's HTTP ingress, by extracting the `traceparent` from the record
  headers.

## What it is explicitly NOT

- **Not an HTTP API.** Cask is its operational surface and nothing more (`AdminHandlers`). There are no business
  endpoints and there will not be: an HTTP write path would be a second, unordered, uncommitted way into the same
  database.
- **Not a reader.** It never serves a query. `EventRepository.search`/`facets`/`histogram`/`find` exist on the
  shared interface for ferrite; cobalt uses `insertAll`.
- **Not a transformer.** The CloudEvent is stored verbatim in `raw jsonb`; every queryable column is
  `GENERATED ALWAYS AS … STORED`. cobalt writes `occurred_at`, `raw` and `payload_sha256` and nothing else.
- **Not a filter.** An event type nobody has ever heard of is stored, searchable and viewable. Refinement into the
  `Observation` ADT is total and happens on the read side.
- **Not exactly-once at the broker.** It is at-least-once delivery made *observationally* exactly-once at the
  database by `ON CONFLICT DO NOTHING` against `cloud_event_identity_uk`. There are no Kafka transactions.

---

## Public surface

### HTTP (Cask on Undertow)

| Route | Response |
| --- | --- |
| `GET /metrics` | Prometheus text exposition, `text/plain; version=0.0.4; charset=utf-8` |
| `GET /health/live` | Always `200 {"status":"UP"}` |
| `GET /health/ready` | `200`/`503` with `{"status":…,"dependencies":{"kafka":{"status","detail"},"postgresql":{…}}}` |
| `GET /admin/dlq` | DLQ depth per partition, plus the replay limits in force |
| `GET /admin/dlq/records?limit&reason` | Bounded, newest-first listing of dead letters |
| `POST /admin/dlq:replay?limit&reason&refs&dryRun` | Plans a replay; publishes only with `dryRun=false` |

Cask does the routing (`cask.main.Main.defaultHandler` is its dispatch trie); Undertow is built explicitly in
`AdminServer` rather than by `cask.main.Main.main`, for two reasons. Cask's own `main` does not expose the bound
port, so an integration test could not bind port `0` and then talk to it; and its shutdown would be a second,
independently-ordered JVM hook that may run before or after the consumer drain and the pool close, instead of a
step inside `CoordinatedShutdown`. The route path literals are asserted against `Meters.MetricsPath`/`HealthPath`
in `AdminRoutesSuite`, so a divergence fails a test instead of silently giving Prometheus a 404.

The `/admin/` prefix is not decoration. `/metrics` and `/health` are platform-owned and safe to expose; the replay
route is the only thing cobalt serves that changes anything, and a prefix is what lets an ingress or a network
policy separate the two without enumerating paths. **It is also not a contradiction of "cobalt is not an HTTP
API".** ADR §1 forbids a business *write* path over HTTP — a second, unordered, uncommitted way into the database.
Replay is the opposite: it puts records back onto Kafka, so every one still travels the single ordered committed
path through the consumer, and the database never hears from this endpoint.

**cobalt publishes no `http.server.requests` family.** Its only HTTP traffic is scrapes and probes, which ADR §7.1
excludes from that meter anyway; there is no metrics filter in front of Cask.

### Kafka

| | |
| --- | --- |
| Consumes | `events.cloudevents.v1` (12 partitions), group `cobalt-cloudevents-v1` |
| Deserializers | `StringDeserializer` / `ByteArrayDeserializer` — **never** `CloudEventDeserializer` |
| Produces | `events.cloudevents.v1.dlq` (3 partitions), structured mode, `application/cloudevents+json` |
| Fixed consumer settings | `enable.auto.commit=false`, `auto.offset.reset=earliest`, `max.poll.records = batch-size` — applied **after** the free-form `properties`, so they always win |
| DLQ key | the original `(topic, partition, offset)`, so a replayed poison record overwrites rather than accumulates |

The three fixed consumer settings are correctness properties, not tuning. `enable.auto.commit=true` acknowledges
records the moment they are polled — the exact opposite of this pipeline's contract — and leaving it to
configuration would let one property file turn at-least-once into at-most-once with no other visible symptom.
`auto.offset.reset=latest` is how a rebuilt consumer group silently loses everything published before it started.

The DLQ producer inherits `acks=all` and idempotence from `KafkaCodecs.producerConfig`, which configuration cannot
override: a DLQ producer that acknowledges before replication makes the dead letter *less* durable than the record
it replaces, defeating the mechanism.

### PostgreSQL

Writes `events.cloud_event (occurred_at, raw, payload_sha256)` via
`INSERT … ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING`, one statement per batch. Everything else in the
row is generated or defaulted. Owns `modules/persistence/src/main/resources/db/migration/*.sql` (currently
`V1__events.sql`: the schema, the partitions, the dimension tables and `events.event_rollup_hourly`).

Migrations run on every boot of every replica. `Migrations.migrate` is idempotent and Flyway's own lock serialises
concurrent replicas, so they converge rather than conflict.

---

## The stream

```text
committableSource ─► decode ─► groupedWithin(500, 250ms) ─► mapAsync(1) write ─► mapConcat ─► Committer.flow ─► Sink.ignore
```

Assembled in `ConsumerStream`, wrapped by `EventConsumer`. Four decisions in it are non-obvious.

### Commit strictly after the write

`Committer.flow` sits **downstream** of `BatchProcessor`, so an offset physically cannot reach the committer until
the batch is durable — in PostgreSQL or in the DLQ. An offset is a receipt for a durable effect. Put the committer
upstream, run it in parallel, or commit in the write stage's `andThen`, and a crash in the window between the two
loses every in-flight event *silently*, with the consumer group reporting zero lag. At-least-once redelivery plus
the `(occurred_at, ce_source, ce_id)` unique index then makes the pipeline observationally exactly-once at the
database.

This is also why offset aggregation (`commit-max-batch = 1000`, `commit-max-interval = 5s`) is safe: every offset
the committer holds is already durable, so delaying the acknowledgement only widens the replay window after a
crash and can never lose anything.

`ConsumerStream.processing` takes the committer flow as a *parameter* precisely so a unit test can substitute one
that records when each offset arrived and assert it never precedes the write it belongs to.

### `mapAsync(1)`, not a larger parallelism

`mapAsync(n)` preserves *emission* order but starts n batches concurrently, so two batches from one partition
would be writing simultaneously and a failure in the older one would already have had its successor's offset
committed past it. One in flight is also enough: a batch is 500 rows in one `executeBatch`, so the database is the
bottleneck, not the stream.

### Decoding in a stream stage, never in a `Deserializer`

A throwing deserializer throws inside `KafkaConsumer.poll`, *before* the connector sees the record: the stream
dies, the offset is never committed, and every restart replays the same poison pill forever. `RecordDecoder` is
total by construction — unrecognised content mode, malformed JSON, missing attribute, an envelope with no `time`,
and even an unexpected runtime exception all become `Left(DeadLetter)`.

The spec-valid-but-unstorable case is worth naming: CloudEvents makes `time` optional, the partition key does not.
`NewEvent.from` refuses such an envelope and it is dead-lettered rather than given an invented timestamp that
would file it in a month it did not happen in.

### The `Consumer.Control` is re-captured on every restart

`ConsumerStream.restarting` sets the `AtomicReference` inside `mapMaterializedValue`, per attempt. Capturing it
once outside the restart means that after the first broker hiccup the reference points at a dead consumer:
`CoordinatedShutdown` calls `drainAndShutdown` on it, the call returns immediately and *successfully*, and the
live consumer is killed by the JVM exiting underneath it — dropping exactly the in-flight batch graceful shutdown
existed to save. It fails silently and only under load.

`RestartSource.onFailuresWithBackoff`, not `withBackoff`: normal completion of the inner source means the consumer
was deliberately stopped (a drain), and restarting it would make shutdown unachievable.

---

## Poison-pill isolation: bisection with SQLSTATE classification

`BatchProcessor` turns one batch into a durable effect. Records that failed to *decode* are dead-lettered up
front. The rest go to a single insert, and if it fails:

1. **Retry the whole batch** up to `write-attempts` times with `retry-delay` between attempts — but only while
   `isDataError(error)` is false. A database blip is not a data problem, and conflating the two is what turns a
   five-second outage into a DLQ full of perfectly good events.
2. **Bisect.** A still-failing batch is split in half and each half written independently, recursively, until the
   failure is attributed to a single record. `log₂(500) ≈ 9` extra round trips is a cheap price for never losing a
   good event and never stalling the stream. The naïve alternative — dead-letter the whole batch — discards up to
   500 good events for one malformed one.
3. **Attribute or rethrow.** At size 1: if the error is a data error, the record is dead-lettered
   (`reason=unpersistable`) and the batch proceeds. If it is *not*, the future fails, the stream fails with it,
   the offsets are never committed, and `RestartSource` backs off and tries again.

**The classification is what makes the bisection safe.** Without it, a database that is merely *down* would bisect
all the way to singletons and dead-letter the entire batch — converting a recoverable outage into permanent data
loss, which is strictly worse than stalling.

`BatchProcessor.DataErrorClasses = {"22", "23"}`:

- SQLSTATE class **22** — data exception (bad datetime, numeric overflow, invalid text representation);
- SQLSTATE class **23** — integrity constraint violation, which on this schema is how
  `cloud_event_specversion_ck`, `cloud_event_required_ck` and "no partition of relation found for row" all
  surface.

Both are deterministic properties of the row: retrying or restarting can only reproduce them. **Everything else is
treated as transient — including class 42** (syntax and privilege errors), which is a *build* defect rather than a
record defect and must page rather than quietly shovel the whole topic into the DLQ.

`isDataError` walks the cause chain (bounded at 32 links, because drivers do re-link exceptions and an infinite
loop inside an error handler is the worst possible place for one), following `getNextException` for
`SQLException`s with no `getCause` — a `BatchUpdateException`'s real reason lives there, not in the cause. Missing
a state is the benign direction: the stream restarts and eventually bisects to the same record, so the walk is
deliberately conservative rather than clever.

DLQ publishing is sequential, never a fan-out: a DLQ burst is by definition a bad moment for the pipeline, and
firing unbounded parallel produces at a broker that may itself be the problem is how a poison batch becomes an
outage. It also keeps the DLQ readable.

**No span wraps the batch write.** A batch aggregates records from many unrelated traces, so it cannot be a child
of any one of them, and a span with 500 links is not something a backend renders usefully. Per-record trace
continuation lives in `RecordDecoder`'s CONSUMER span; batch health is a metric.

---

## The dead-letter queue: inspection and replay

`docs/operations.md` used to say, in full: *"There is no replay tool in this repo. Once the defect is fixed,
re-publish the original events through `POST /events` on wolfram."* That instruction assumes the events still
exist somewhere other than the DLQ, that their bytes can be retyped by hand, and that there is time. Retention on
`events.cloudevents.v1.dlq` is 7 days, so what an incident actually leaves is a 7-day window in which the only
tool is `kafka-console-consumer`. `DeadLetterAdmin`, `DeadLetterReplay` and `DeadLetterStore` are that window's
tooling.

### Inspection

```bash
curl -s localhost:8082/admin/dlq | jq
curl -s 'localhost:8082/admin/dlq/records?limit=20&reason=unconvertible' | jq '.records[]'
```

`GET /admin/dlq` reports per-partition `earliest`/`latest` and their sum. The sum is labelled
`outstandingIsUpperBound: true` and it means it: `latest - earliest` counts *offsets*, and the DLQ is keyed on
origin coordinates, so a compacted topic holds fewer live records than its offset range suggests. Reporting an
exact count would be the more comfortable lie; the question this number answers is "empty, a handful, or a flood",
and an upper bound answers all three honestly.

`GET /admin/dlq/records` returns the newest dead letters, each with its `reason` and `detail`, the origin
coordinates, the CloudEvents `id`/`type`/`source` where they are recoverable, `replayAttempts`, and a payload
preview. Three bounds apply, because **an unbounded listing endpoint on a topic is a way to OOM the service that
is supposed to be telling you it is unhealthy**:

- the seek is `max(earliest, latest - limit)` per partition, so at most `limit × partitions` records are fetched
  however large the topic is;
- the read stops at the log end computed before it started, so a concurrent producer cannot extend it, and it
  gives up after `poll-timeout` with whatever it has — a listing that hangs is worse than one that is short;
- the payload preview is the first 512 bytes, decoded UTF-8-with-replacement, alongside the true byte length and a
  `truncated` flag. The cut is by *bytes*, so a 4 MB record costs 512 bytes of decoding rather than 4 MB.

A request over `max-records` is refused with `400` rather than trimmed, for the same reason a replay is.

The identity fields are best effort, and they are absent when they have to be: a record is usually on the DLQ
*because* its attributes could not be read. The `ce_*` headers are tried first (the main topic is binary mode),
the payload JSON second (for a structured record). A record that yields neither is still listed — "a dead letter
whose id is unknowable" is itself the diagnosis. So is a record that is not a dead letter at all: the DLQ is a
topic like any other, and one written by a foreign producer is listed as `readable: false` with its problem rather
than costing the whole page.

**There is no cursor and no `offset` parameter.** The DLQ is a log with a 7-day retention being written to while it
is read; a cursor over it would either mean something different on every partition or promise a stability the
topic cannot keep. Anything deeper than `max-records` is a job for `kcat`, and the listing hands over the exact
refs to feed it.

**No depth gauge.** The count is on demand rather than a Prometheus gauge: it costs two round trips per sample, and
`consume.records.poison` already gives the rate a dashboard needs. A gauge would duplicate that reading and add
standing broker load for it.

### Replay

```bash
# what would happen — the default, and the shape to run first
curl -sX POST 'localhost:8082/admin/dlq:replay?limit=50&reason=unconvertible' | jq

# do it
curl -sX POST 'localhost:8082/admin/dlq:replay?limit=50&reason=unconvertible&dryRun=false' | jq

# exactly these records, all or nothing
curl -sX POST 'localhost:8082/admin/dlq:replay?refs=events.cloudevents.v1/3/9912&dryRun=false' | jq
```

**The replayed record is a byte copy, never a re-encode.** `DeadLetterReplay.producerRecord` rebuilds it from the
dead letter's recorded headers and its verbatim payload bytes; the envelope is never decoded and re-serialised.
That matters twice. It is the only way to replay a record that *cannot* be decoded, which is most of a DLQ; and it
keeps the CloudEvents `id` and `source` byte-identical, which is the entire reason replaying an event that DID
land is a no-op — `ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING` only absorbs a duplicate whose identity
survived the round trip. The original key goes back too, so the record lands on the partition it originally did
and per-device ordering survives; so does `traceparent`, so a replayed event continues the trace that started at
wolfram's ingress days earlier.

Two transport headers are added: `x-worxbend-replay-attempt` and `x-worxbend-replay-of`. Neither carries the `ce_`
prefix, and that is load-bearing — under the Kafka binding a `ce_`-prefixed header *is* a context attribute, so
`ce_replayattempt` would change the event's extension set, its `raw jsonb`, and therefore its row.

| Decision | What it is, and why |
| --- | --- |
| **Method** | `POST`. It is neither safe nor free of effect at the broker. |
| **Default** | `dryRun=true`. The keystroke that publishes is the one you have to add. An operator who cannot see what a replay would do will not run one during an incident, which makes the tool worthless exactly when it is needed. |
| **Scope** | `limit` (plus optional `reason`) for "whatever is there, up to N", or `refs` for "these exact records". Never unbounded. |
| **Bounding** | A request over `max-records` is **refused, not clamped**. Silently giving 200 to someone who asked for 500 means they believe 500 events are back in the pipeline; the missing 300 get found later, from a gap in a dashboard. |
| **Rate** | One produce at a time, each acknowledged before the next. The same reasoning as the DLQ writes in `BatchProcessor`: a fan-out of produces at a broker that may itself be why these records died is how a bad moment becomes an outage. |
| **Idempotence** | **Not** idempotent at the broker — each call appends new records to the topic. **Idempotent at the database**, because the event id survives. That asymmetry is what makes retrying a half-finished replay safe rather than a duplication risk. |
| **Failure** | The plan is computed in full before anything is published, so a named set that cannot be satisfied completely is refused with `422` having published nothing. The produce loop itself cannot be atomic — Kafka offers no transaction that would help, and one would put the replay's fate in the coordinator that may be the problem — so it stops at the first refusal and reports `published` and the ref it stopped on. A boundary, not a guess. |
| **Off switch** | `cobalt.replay.enabled=false` refuses commits with `403`. Dry runs and the read endpoints keep working: switching replay off should not also blind the operator, which would only send them back to the console consumer. |

A dead letter is *skipped* — reported, counted, never published — for exactly one of three reasons.
`undecodable`: the DLQ record is not a readable dead letter. `foreign-topic`: its origin topic is not the topic
this consumer owns. `budget-exhausted`: see below. The middle one is the check that keeps this endpoint from being
a general-purpose producer — the destination comes out of the record's own payload, so without it the DLQ is a way
to make cobalt write to any topic in the cluster that a foreign record can name.

Selection is newest-first, because "the last N" is what an operator means. Publication is oldest-first, so a
device's events reach the topic in the order they originally did rather than backwards.

### The poison loop, and what bounds it

A record that fails again after being replayed lands back on the DLQ — **under a new key.** The DLQ key is the
origin `topic/partition/offset`, and the replayed record occupies a *new* offset on the main topic, so its dead
letter is a new record rather than a compaction-overwrite of its predecessor. Nothing in Kafka stops one defect
accumulating one dead letter per replay.

What stops it is `x-worxbend-replay-attempt`. The counter is written onto the replayed record, and a dead letter
records its record's headers verbatim, so it survives into the next generation and the one after. The planner
refuses any record at or above `cobalt.replay.max-attempts` (default 3). An attempt header that is present but
unparseable also counts as exhausted, never as zero: nothing in this build writes anything else there, so the safe
reading of "I cannot tell how many times this has gone round" is "do not send it round again".

The loop is therefore bounded at `max-attempts` generations, visible in every listing as `replayAttempts`, and
traceable — each generation names its predecessor in `x-worxbend-replay-of`, so a loop reads as a chain rather
than a pile of unrelated dead letters.

### How it is wired

`DeadLetterStore` is one `KafkaConsumer` and one `KafkaProducer` behind a single lock. The lock is a feature:
`KafkaConsumer` is not thread-safe and Undertow hands every request to a different worker thread, so some
serialisation is mandatory — and one lock over the whole store additionally means two operators cannot replay
simultaneously, which is the correct outcome. The consumer uses `assign`, never `subscribe`: it joins no group, it
seeks to explicit offsets, and it has no committed offsets that could be confused with the real consumer's.
Reading the DLQ must not be able to move anything. It has its own producer rather than sharing the dead-letter
publisher's, so each owner closes what it opened.

`DeadLetterReplay` holds every decision as a pure function over values — selection, bounding, classification,
record reconstruction — so `DeadLetterReplaySuite` asserts all of it without a broker, and the dry run and the
commit are provably the same computation because they call the same `plan`.

---

## Configuration

Namespace `cobalt`, plus `database` from `modules/persistence`'s `reference.conf` and `pekko` overrides. An
unreadable configuration aborts the boot — a consumer that boots with the wrong topic name is the least debuggable
failure in this system, because it starts cleanly, reports itself live, and receives nothing.

| Env var | HOCON key | Default | Notes |
| --- | --- | --- | --- |
| `HTTP_HOST` | `cobalt.server.host` | `0.0.0.0` | Admin listener only. |
| `HTTP_PORT` | `cobalt.server.port` | `8080` | `0` binds an ephemeral port; `AdminServer.boundPort` reports the real one. |
| `KAFKA_BOOTSTRAP_SERVERS` | `cobalt.consumer.bootstrap-servers` | `localhost:9092` | **Effectively mandatory.** |
| `KAFKA_TOPIC` | `cobalt.consumer.topic` | `events.cloudevents.v1` | Must match wolfram's. |
| `KAFKA_DLQ_TOPIC` | `cobalt.consumer.dlq-topic` | `events.cloudevents.v1.dlq` | |
| `KAFKA_GROUP_ID` | `cobalt.consumer.group-id` | `cobalt-cloudevents-v1` | Changing it replays from `earliest`. |
| `CONSUMER_BATCH_SIZE` | `cobalt.consumer.batch-size` | `500` | Also `max.poll.records`, and the largest batch the isolation search can be asked to bisect. |
| `CONSUMER_BATCH_WINDOW` | `cobalt.consumer.batch-window` | `250 millis` | The pipeline's **latency floor** under sparse traffic: at one event per minute, an event is durable `batch-window` after it arrives and not before. |
| `CONSUMER_WRITE_ATTEMPTS` | `cobalt.consumer.write-attempts` | `3` | Whole-batch retries before bisection. |
| `CONSUMER_RETRY_DELAY` | `cobalt.consumer.retry-delay` | `200 millis` | Deliberately short — the real backoff is the `RestartSource`, which also unwinds the Kafka session. |
| `CONSUMER_DRAIN_TIMEOUT` | `cobalt.consumer.drain-timeout` | `30 seconds` | Consumer stop timeout; also the DLQ producer's close timeout. |
| `LAG_REFRESH_INTERVAL` | `cobalt.lag.refresh-interval` | `20 seconds` | Two admin round trips per interval per replica. Lag is a trend; sampling faster than Prometheus scrapes buys only broker load. |
| `LAG_REQUEST_TIMEOUT` | `cobalt.lag.request-timeout` | `5 seconds` | Bounds each admin call, and doubles as the broker-reachability probe timeout. |
| `REPLAY_ENABLED` | `cobalt.replay.enabled` | `true` | Whether a replay may be **committed**. Dry runs and the read endpoints stay available either way. |
| `REPLAY_MAX_RECORDS` | `cobalt.replay.max-records` | `200` | Ceiling on one listing or one replay. A request above it is refused, never clamped. Bounds the fetch as well as the produce. |
| `REPLAY_MAX_ATTEMPTS` | `cobalt.replay.max-attempts` | `3` | Generations of replay any one record may survive. The bound on the poison loop. |
| `REPLAY_POLL_TIMEOUT` | `cobalt.replay.poll-timeout` | `5 seconds` | How long one read of the DLQ may take before it answers with what it has. |
| `DATABASE_URL` | `database.jdbc-url` | `jdbc:postgresql://localhost:5432/observatory` | **Effectively mandatory.** |
| `DATABASE_USER` | `database.username` | `observatory` | |
| `DATABASE_PASSWORD` | `database.password` | `""` | **Mandatory** wherever the server requires a password. |

No env var (file or `-D` only): `cobalt.consumer.commit-max-batch` (1000), `commit-max-interval` (5 s),
`commit-parallelism` (1), `properties` (`{}`); the whole `cobalt.restart` block (`min-backoff` 1 s, `max-backoff`
30 s, `random-factor` 0.2, `max-restarts` 50, `max-restarts-within` 10 minutes); and the `database.read`/
`database.write` pool blocks (write pool `maximum-pool-size = 6`, which also sizes the JDBC dispatcher).

Pekko overrides in `application.conf`: SLF4J logging (so the connector's own logs join the same JSON pipeline),
`coordinated-shutdown.run-by-jvm-shutdown-hook = on`, and
`phases.service-requests-done.timeout = 40 seconds` — long enough for a full batch write plus its commit, short
enough that a wedged drain cannot hold a rolling deploy open.

Cross-cutting: `SERVICE_VERSION`, `HOSTNAME`, `OTEL_*` (traces only; `OTEL_SDK_DISABLED=true` disables),
`LOG_LEVEL` (default `INFO`).

---

## Failure modes and what it does about them

| Failure | Response |
| --- | --- |
| Record does not decode (bad content mode, malformed JSON, missing attribute, no `time`) | `Left(DeadLetter)` → DLQ → `consume.records.poison{reason}` → **offset committed** |
| Unexpected exception during decode | Same path, `reason=unconvertible`; nothing in `RecordDecoder` throws |
| Insert fails transiently (connection blip, deadlock, timeout) | Up to `write-attempts` whole-batch retries; then, if the error is still not a data error, the stream fails and `RestartSource` backs off with jitter |
| Insert fails on one bad row (SQLSTATE 22/23) | Bisection isolates it, dead-letters it, and the rest of the batch is written |
| Duplicate record (redelivery) | `ON CONFLICT DO NOTHING`; the shortfall is counted on `consume.records.duplicate`. Non-zero is **normal** here |
| DLQ record cannot be encoded | Future fails, offset stays uncommitted, record is replayed rather than silently dropped |
| DLQ produce fails | Future fails; same conservative direction |
| Broker unreachable | Restart backoff; `health.broker` goes down within one lag interval; readiness → 503 |
| PostgreSQL unreachable | Inserts fail as transient, stream restarts; `health.database` down; readiness → 503 |
| Restart budget exhausted (50 restarts in 10 min) | The stream **fails permanently**, deliberately. An unbounded retry loop against a broker that is never coming back looks identical from the outside to a healthy consumer with no traffic — same process, same Ready pod, only lag as a symptom |
| Replay: a named ref is absent or unreplayable | `422`, **nothing published**; the response names every missing and every skipped ref |
| Replay: the broker refuses a produce part way | The loop stops; `500`, with `published` and the ref it stopped on. Retrying is safe — the insert is idempotent |
| Replay: the replayed record fails again | A new dead letter one generation on, `x-worxbend-replay-attempt` incremented; refused once `max-attempts` is reached |
| Replay: the DLQ topic is unreachable | `503`, not `500` — the same request will work when the broker is back |
| Unreadable configuration | Boot aborts |
| SIGTERM | See below |

**Shutdown** is expressed as `CoordinatedShutdown` phases, not a `close()` sequence:

1. `PhaseServiceUnbind` — stop the admin listener, so readiness starts failing and traffic stops being routed here.
2. `PhaseServiceRequestsDone` — `EventConsumer.drain()`: `drainAndShutdown` stops the Kafka fetcher, lets the
   in-flight batch finish, write and commit, and only then completes. This is what makes a rolling deploy
   invisible in the data — killing the stream mid-batch loses nothing durable, but every deploy would replay a
   batch.
3. `PhaseBeforeActorSystemTerminate` — probe scheduler, DLQ producer, admin client, connection pools, JDBC
   dispatcher, **then** telemetry.

Closing telemetry first is the common mistake, and it makes the drain — the one part of shutdown that can lose
data — the one part with no metrics and no spans.

---

## Metrics and health semantics

Common tags `service=cobalt`, `version`, `instance`.

| Meter | Type | Tags | Meaning |
| --- | --- | --- | --- |
| `consume.batch.size` | summary | — | Records per `groupedWithin` batch. Small batches under sustained load mean the consumer is **starved**, not saturated — a distinction throughput alone cannot make. |
| `consume.batch.latency` | timer | `outcome` | One batch insert's wall clock. |
| `consume.records.persisted` | counter | `type` | Records the database took responsibility for in a successful batch — new **or** already present. |
| `consume.records.duplicate` | counter | — | The shortfall between batch size and rows actually written. **Untagged, deliberately**: `ON CONFLICT DO NOTHING` reports one number and there is no way to attribute the shortfall to a `type`. Inventing a per-type split would produce a number that looks precise and is not. |
| `consume.records.poison` | counter | `reason` | DLQ routings. `reason` is always a bounded value, never an exception message. |
| `consume.group.lag` | multi-gauge | `group`, `topic`, `partition` | See below. |
| `dlq.replay.operations` | counter | `outcome` | One replay request. `success` = committed, `failure` = refused or stopped part way, `skipped` = a dry run. **The audit trail as a metric**: every increment is a human intervening in the pipeline. |
| `dlq.replay.records` | counter | `outcome` | Individual dead letters: `success` published, `failure` refused by the broker, `skipped` declined by the plan. |

Both replay meters are needed, not one or the other: one operation replaying 200 records and 200 operations
replaying one each give the same record count and describe very different situations — a recovery, and somebody in
a loop. Only the operation counter distinguishes them. Neither is tagged by skip reason: the reasons are a closed
set and would be safe as a tag, but the actionable number is "did the replay do what I asked", which `success`
beside `skipped` answers, and the per-record reason is in the response body and on the log line where the person
who ran the command is already reading it.

Plus the JVM/system binders. `consume.batch.latency` is the one name in this service not already in `Meters`: ADR
§7.1's minimum set names `consume.batch.size` but no companion timer, and `modules/observability` is a finished
module this build does not edit, so it is spelled as a single constant in the same `consume.` family and flagged
as a deviation rather than left as a surprise.

**Lag comes from an `AdminClient`, not the consumer's `records-lag-max`.** The client metric only covers
partitions the consumer is currently fetching: it reads zero during a rebalance and disappears entirely when the
process is down — the two moments at which lag is the only number anyone wants. Asking the broker means the
measurement survives the thing it is measuring. One `AdminClient` is reused; one per poll would turn a monitoring
signal into broker connection churn.

Two rules in `ConsumerLag.lags`, both there because the alternative produces a false alarm:

- **A partition with no committed offset is omitted** — not zero, not `end`. A group that has never committed has
  genuinely unknown lag; reporting `end` pages the on-call the first time a topic is created, reporting `0` claims
  a consumer is caught up when it has not read a record. A gap in the series is the honest rendering of "no data".
- **Negative differences clamp to zero.** Committed can legitimately exceed the log end for a moment after a
  truncation, reassignment or offset reset, and a negative lag on a dashboard reads as a broken exporter.

`MultiGauge.register(rows, overwrite = true)` makes an unassigned partition *disappear* from the exposition rather
than freeze at its last value — a frozen gauge is indistinguishable from a stuck consumer.

**Liveness** consults nothing. A liveness probe that checked Kafka would fail on every replica at once during a
broker outage and the orchestrator would restart them all, turning a recoverable dependency failure into a crash
loop that outlives it.

**Readiness** consults exactly two dependencies, kafka and postgresql, because cobalt has exactly two: without
Kafka there is nothing to consume, without PostgreSQL there is nowhere to put it, and a consumer that keeps
polling while every insert fails burns through its restart budget and then dies. The DLQ producer and the admin
client share a fate with one of those two; adding them would only make the probe flap for reasons that do not
change the answer.

**Readiness never probes inline.** A handler that opened a JDBC connection or asked a broker for metadata would
turn the readiness endpoint into a load generator against the dependency that is already struggling, and its own
latency into the thing that fails the probe. `Probes` — one daemon thread, one `scheduleWithFixedDelay` — writes
`DependencyHealth` and the handler reads a field. Stale-but-instant beats fresh-but-hanging. The unreachable
dependency's cause is included in the readiness body, so the probe carries its own diagnosis.

Details worth knowing: `DependencyHealth` starts **down** ("not probed yet"), because a process that has not
proved it can reach its dependencies is not ready; `Probes.start` runs one probe synchronously before scheduling,
so the first answer is evidence and not one interval of false negatives; the schedule is fixed *delay*, not fixed
rate, because when the broker is unreachable each probe takes the full admin timeout and a fixed rate would queue
overlapping runs against a struggling dependency; a failure inside the poller is recorded and never thrown,
because an exception escaping a `ScheduledExecutorService` task silently cancels all subsequent runs, freezing
both the gauge and the readiness answer; and the database check is `Connection.isValid`, not `SELECT 1` — the JDBC
contract's own liveness check, bounded by a timeout the driver honours and consuming no statement-cache slot.

---

## Running it locally

```bash
docker compose -f deploy/docker-compose.yml up -d postgres kafka kafka-init

DATABASE_URL=jdbc:postgresql://localhost:5432/observatory \
DATABASE_USER=observatory \
DATABASE_PASSWORD=... \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
sbt cobalt/run                  # admin endpoints on :8080; override with HTTP_PORT
```

cobalt applies the migrations itself, so this is also how the schema gets created for a local ferrite.

```bash
curl -s localhost:8080/health/ready | jq
curl -s localhost:8080/metrics | grep consume_

# the DLQ, without a console consumer
curl -s localhost:8080/admin/dlq | jq
curl -s 'localhost:8080/admin/dlq/records?limit=10' | jq '.records[] | {ref, reason, event}'
curl -sX POST 'localhost:8080/admin/dlq:replay?limit=10' | jq        # dry run: the default
curl -sX POST 'localhost:8080/admin/dlq:replay?limit=10&dryRun=false' | jq

# still there, and still the right tool for anything deeper than max-records
docker compose -f deploy/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic events.cloudevents.v1.dlq --from-beginning
```

Full stack: `sbt cobalt/Docker/publishLocal` then `docker compose -f deploy/docker-compose.yml up -d`; cobalt's
admin surface is published on host port **8082**.

Tests: `sbt cobalt/test` needs no Docker — the broker is behind `DeadLetterPublisher`, commit ordering is tested
with a substituted committer flow, and the lag arithmetic is a pure function over two maps.
`sbt "cobalt/IT/testFull"` runs the Testcontainers suites (`CobaltIngestIT`, `AdminServerIT`, `DlqReplayIT`).
`DlqReplayIT` is the one that matters for the DLQ surface: a replay tool that has never replayed anything is not a
tool, so it dead-letters a record for real, replays it into a row, and separately proves that a record which fails
again comes back one generation on and is then refused.
