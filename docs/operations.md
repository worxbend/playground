# Operations

How to deploy, observe and repair the event observatory. Everything here is derived from what the code and
`deploy/` actually do; where the running system disagrees with `docs/adr/0000-architecture.md`, this document
follows the code and the disagreement is recorded under [Known limitations](#8-known-limitations).

---

## 1. Topology

One `deploy/docker-compose.yml` brings up the whole stack on a single host.

| Container | Image | Host port | Role |
| --- | --- | --- | --- |
| `postgres` | `postgres:18.4-alpine` | — | The event store. `PGDATA=/var/lib/postgresql/18/docker`. |
| `kafka` | `apache/kafka:4.3.1` | — | Single-node KRaft broker. No ZooKeeper. |
| `kafka-init` | `apache/kafka:4.3.1` | — | One-shot. Creates the two topics; auto-creation is disabled. |
| `wolfram` | `wolfram:latest` | `8081 -> 8080` | CloudEvents ingestion API (Tapir on Vert.x). Owns no state. |
| `cobalt` | `cobalt:latest` | `8082 -> 8080` | Kafka consumer, Postgres writer. **Runs the Flyway migrations.** |
| `ferrite` | `ferrite:latest` | `9000 -> 9000` | Play 3 web UI and search. Reads Postgres. Never sees Kafka. |
| `otel-collector` | `otel/opentelemetry-collector-contrib:0.157.0` | — | OTLP/gRPC on `:4317`, OTLP/HTTP on `:4318`. Traces only. |
| `prometheus` | `prom/prometheus:v3.13.1` | `9090` | Scrapes `/metrics` off all three services every 15 s, 30 d retention. |
| `grafana` | `grafana/grafana:13.1.1` | `3000` | Prometheus provisioned as the default datasource, plus the **Event observatory** dashboard (§5.2). |

Topics (`kafka-init`, and `io.kzonix.kernel.event.Topics`):

- `events.cloudevents.v1` — 12 partitions, replication factor 1, CloudEvents **binary** content mode.
- `events.cloudevents.v1.dlq` — 3 partitions, replication factor 1, CloudEvents **structured** mode, keyed
  `topic/partition/offset` so a replayed poison record overwrites its predecessor instead of accumulating copies.

Data flow: producer → `POST /events` on wolfram → Kafka → cobalt → `INSERT … ON CONFLICT DO NOTHING` into
`events.cloud_event` → ferrite reads. A W3C `traceparent` is injected into the Kafka headers at ingest and
extracted by the consumer, so one trace spans HTTP → Kafka → database.

---

## 2. Deploying

Images are built by sbt, not by compose.

```bash
# One quoted argument each, or a ";a;b;c" sequence. sbt 2 does NOT split a single
# space-joined string: `sbt "a b c"` fails to parse.
sbt ";ferrite/Docker/publishLocal;cobalt/Docker/publishLocal;wolfram/Docker/publishLocal"

cd deploy
cp .env.example .env      # then edit; .env is gitignored
$EDITOR .env
docker compose config -q  # validates interpolation and the mandatory vars
docker compose up -d
```

Startup ordering is enforced with healthchecks rather than plain `depends_on`, because Kafka and Postgres both
accept TCP connections well before they can serve requests:

```
postgres/kafka healthy -> kafka-init completes -> wolfram, cobalt -> (cobalt healthy) -> ferrite
```

`cobalt` applies the Flyway migrations on boot, which is why `ferrite` waits for it: the schema must exist before
the reader starts. `Migrations.migrate` is idempotent and Flyway's own lock serialises concurrent replicas.

### Smoke test after `up -d`

```bash
docker compose ps                                   # every service "healthy", kafka-init "exited (0)"
curl -fsS localhost:8081/health/ready                # wolfram: {"status":"UP","broker":"reachable"}
curl -fsS localhost:8082/health/ready | jq           # cobalt: broker + database both UP
curl -fsS localhost:9000/health/ready                # ferrite: read pool hands out a connection
curl -fsS localhost:8081/openapi.json | jq .info     # the running build's own API document

curl -fsS -X POST localhost:8081/events \
  -H 'ce-specversion: 1.0' -H 'ce-id: smoke-1' \
  -H 'ce-source: urn:kzonix:smoke' -H 'ce-type: io.kzonix.smoke.v1' \
  -H "ce-time: $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -H 'content-type: application/json' \
  -d '{"deviceId":"smoke","severity":"info","value":1}'
# 202 Accepted, then the event appears at http://localhost:9000/events within a second
```

Prometheus targets: <http://localhost:9090/targets> — all three `observatory` targets must be `UP`.

JVM flags are baked into each image's `conf/application.ini` by `containerJvmOptions` in `build.sbt`: 70 % of the
container memory limit as heap (the JVM's container default of 25 % left three quarters of the budget unusable) and
G1 (ergonomics selects **Serial** below 1792 MB, which every one of these containers is). Verify after a base-image
bump with `docker exec observatory-wolfram-1 java -XX:+PrintFlagsFinal -version | grep -w MaxHeapSize` and by
checking that `jvm_memory_max_bytes` reports `G1 Eden Space` rather than `Eden Space`. Note that
`conf/application.ini` is spliced straight onto the `java` command line and the start script strips `-J` **only**
from `-X`/`-XX` options — so a `-J--enable-native-access=…` there kills the container at boot.

### Rolling a new build

```bash
sbt ";cobalt/Docker/publishLocal;wolfram/Docker/publishLocal"
cd deploy && docker compose up -d cobalt wolfram
```

`SIGTERM` is handled: wolfram stops the listener, drains the producer, then closes telemetry; cobalt runs
`CoordinatedShutdown` — unbind admin listener → drain the in-flight Kafka batch and commit it → close the DLQ
producer, admin client, pools and telemetry. Give containers at least `pekko.coordinated-shutdown` +
`CONSUMER_DRAIN_TIMEOUT` (40 s + 30 s defaults) before `docker compose` sends `SIGKILL` if you raise the batch size.

---

## 3. Environment variables

### 3.1 Read by `docker compose` from `deploy/.env`

Mandatory — compose refuses to start without them (`${VAR:?message}`):

| Variable | Used by | Notes |
| --- | --- | --- |
| `POSTGRES_PASSWORD` | postgres, cobalt, ferrite | `openssl rand -base64 32`. |
| `APPLICATION_SECRET` | ferrite | Play refuses to start in prod mode without it. `openssl rand -base64 48`. |
| `GRAFANA_ADMIN_PASSWORD` | grafana | Sign-up is disabled, so this is the only way in. |

Optional, with compose defaults:

| Variable | Default | Notes |
| --- | --- | --- |
| `POSTGRES_DB` | `observatory` | Also interpolated into `DATABASE_URL` for cobalt and ferrite. |
| `POSTGRES_USER` | `observatory` | |
| `ALLOWED_HOSTS` | `localhost,127.0.0.1` | Play's allowed-hosts filter. Comma-separated; split by `io.kzonix.ferrite.wiring.AllowedHosts`. Compose appends `ferrite` so Prometheus can scrape `http://ferrite:9000/metrics` — without it the filter answers 400 and that target stays DOWN. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | |
| `OTEL_TRACES_SAMPLER` | `parentbased_traceidratio` | |
| `OTEL_TRACES_SAMPLER_ARG` | `0.1` | 10 % of traces. |
| `LOG_LEVEL` | `INFO` | |

### 3.2 Read by the services

Every service reads its configuration from the environment with development-only defaults, so an image is
configured at deploy time and never rebuilt for an environment change. `application.conf` holds **overrides only**.

**All three services**

| Variable | Default | Effect |
| --- | --- | --- |
| `SERVICE_VERSION` | `0.0.0-unknown` | The `version` tag on every meter and the `service.version` resource attribute. Compose passes it from `.env` (default `0.1.0-SNAPSHOT`); keep it in step with the image tag so a rollout is visible in dashboards. |
| `HOSTNAME` | container id | `service.instance.id` on spans. Docker sets it; nothing else needs wiring. |
| `OTEL_SERVICE_NAME` | the service's own name | Set per service in compose. Overriding it to something else makes traces and metrics disagree — don't. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `_PROTOCOL` | `http://otel-collector:4317` / `grpc` | The SDK is autoconfigured; gRPC is forced by `modules/observability`. |
| `OTEL_TRACES_EXPORTER` | `otlp` | Set to `none` for a local run with no collector, otherwise the exporter logs a WARN per batch interval. |
| `OTEL_SDK_DISABLED` | `false` | The fleet-wide off switch for tracing. Metrics are unaffected — they are Micrometer's, never OTel's. |
| `LOG_LEVEL` | `INFO` | Logback JSON to stdout, `trace_id`/`span_id` in the MDC. |

Metrics exporters cannot be turned on through `OTEL_*`: `modules/observability` forces `otel.metrics.exporter`
and `otel.logs.exporter` to `none` as *system properties*, which beat the environment. That is deliberate — a
second metrics pipeline would double-count every meter.

**wolfram** (`applications/wolfram/src/main/resources/application.conf`)

| Variable | Default | Effect |
| --- | --- | --- |
| `HTTP_HOST` / `HTTP_PORT` | `0.0.0.0` / `8080` | Listener binding. `0` binds an ephemeral port (tests). |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `KAFKA_TOPIC` | `events.cloudevents.v1` | |
| `INGEST_MAX_EVENT_BYTES` | `1048576` (1 MiB) | Body ceiling; over it is `413` with `reason=too-large`. Kept under Kafka's `message.max.bytes` so the API says no, not the broker. |
| `INGEST_MAX_BATCH_EVENTS` | `256` | Events per `application/cloudevents-batch+json` request. |
| `INGEST_MAX_FUTURE_SKEW` | `24 hours` | `time` further ahead is rejected `400`. |
| `INGEST_MAX_PAST_SKEW` | `90 days` | `time` further behind is rejected `400`. |
| `KAFKA_MAX_BLOCK` | `2 seconds` | Producer `max.block.ms`. Kafka's own default of 60 s parks the request thread for a minute when the broker is unreachable. |
| `KAFKA_DELIVERY_TIMEOUT` | `10 seconds` | Bounds the whole send including retries. Must be ≥ linger + request timeout. |
| `KAFKA_REQUEST_TIMEOUT` | `5 seconds` | One round trip. |
| `KAFKA_CLOSE_TIMEOUT` | `10 seconds` | Drain window on shutdown. |
| `KAFKA_QUEUE_CAPACITY` | `1024` | In-flight sends. Beyond it, requests are shed as `503` rather than queued — explicit backpressure. |

**cobalt** (`applications/cobalt/src/main/resources/application.conf`)

| Variable | Default | Effect |
| --- | --- | --- |
| `HTTP_HOST` / `HTTP_PORT` | `0.0.0.0` / `8080` | Admin listener only: `/metrics`, `/health/live`, `/health/ready`. There are no business endpoints. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `KAFKA_TOPIC` | `events.cloudevents.v1` | |
| `KAFKA_DLQ_TOPIC` | `events.cloudevents.v1.dlq` | |
| `KAFKA_GROUP_ID` | `cobalt-cloudevents-v1` | Changing it replays the topic from `auto.offset.reset`. |
| `CONSUMER_BATCH_SIZE` | `500` | `groupedWithin` size; also the largest batch the poison bisection must halve. |
| `CONSUMER_BATCH_WINDOW` | `250 millis` | The pipeline's latency floor when traffic is sparse. |
| `CONSUMER_WRITE_ATTEMPTS` | `3` | Whole-batch retries before poison isolation starts. |
| `CONSUMER_RETRY_DELAY` | `200 millis` | Between those attempts. The real backoff for a sustained outage is the stream's `RestartSource`. |
| `CONSUMER_DRAIN_TIMEOUT` | `30 seconds` | How long shutdown waits for the in-flight batch. Also the DLQ producer's close timeout. |
| `LAG_REFRESH_INTERVAL` | `20 seconds` | Broker/database probe period; also the lag gauge's period. |
| `LAG_REQUEST_TIMEOUT` | `5 seconds` | AdminClient request timeout. |

Restart policy for the consumer stream is config-only (not env): `restart.min-backoff` 1 s, `max-backoff` 30 s,
`random-factor` 0.2, `max-restarts` **50** within 10 minutes. Past that budget the stream stops for good and the
process stays *live* while consuming nothing — which is precisely why lag is measured from an AdminClient.

**ferrite**

| Variable | Default | Effect |
| --- | --- | --- |
| `APPLICATION_SECRET` | a development-only literal | Mandatory outside development. |
| `ALLOWED_HOSTS` | `localhost,127.0.0.1,.local` | Allowed-hosts filter. A **comma-separated string**, split by `io.kzonix.ferrite.wiring.AllowedHosts` — Play's own `play.filters.hosts.allowed` list key cannot be fed from the environment. An empty value fails the boot rather than rejecting every request. |
| `PLAY_HTTP_PORT` | `9000` | Play's own variable (`play.server.http.port`). |
| `PLAY_HTTP_ADDRESS` | `0.0.0.0` | |

**Database — all services that touch Postgres** (`modules/persistence/src/main/resources/reference.conf`)

| Variable | Default | Effect |
| --- | --- | --- |
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/observatory` | JDBC URL. |
| `DATABASE_USER` | `observatory` | |
| `DATABASE_PASSWORD` | *(empty)* | |

Pool sizes are configuration, not environment: read pool 8 connections with a server-side
`SET statement_timeout = '2s'` and `read-only = true`; write pool 6 connections. Each pool's size must equal the
size of the executor in front of it (ferrite's `database.search-dispatcher.fixed-pool-size`, cobalt's JDBC
dispatcher) — otherwise the queue moves from a place you configured into HikariCP, where it surfaces as a
`connectionTimeout` exception instead of as backpressure.

---

## 4. Health and metrics endpoints

All three services mount the same paths, so one scrape config and one ingress rule cover the fleet.

| Path | Meaning |
| --- | --- |
| `GET /metrics` | Prometheus text exposition, `text/plain; version=0.0.4; charset=utf-8`. One registry per process. |
| `GET /health/live` | **Consults nothing.** Liveness that checked Kafka would restart every replica during a broker outage and turn a recoverable failure into a crash loop. |
| `GET /health/ready` | Consults dependencies. wolfram: broker reachability. cobalt: broker **and** database, with the failing dependency's reason in the body. ferrite: the read pool can hand out a valid connection. |
| `GET /openapi.json` | wolfram only. Generated from the running build's endpoint values. |

`/metrics` and `/health/*` are excluded from `http.server.requests` in all three services — in wolfram
structurally, by mounting them outside the Tapir interpreter.

---

## 5. Reading the metrics

Names are declared once in `modules/observability`'s `Meters` object in Micrometer's dot convention; the registry
translates them to Prometheus's underscores at scrape time, and counters gain `_total`, timers `_seconds_{count,sum,max}`.
Every meter carries the common tags `service` and `version`. `instance` comes from the Prometheus scrape target,
not from the exposition.

### The ones that matter

| Metric | Emitted by | Read it as |
| --- | --- | --- |
| `ingest_events_received_total{type,mode}` | wolfram | Durable events, counted **after** the broker acknowledged. Not the arrival rate — that is `http_server_requests`. |
| `ingest_events_rejected_total{reason}` | wolfram | `malformed`, `invalid-attributes`, `invalid-payload`, `unknown-type`, `too-large`. A closed set by construction. |
| `kafka_produce_latency_seconds{topic,outcome}` | wolfram | p99 is the signal. Rising median = broker pressure; rising p99 over a flat median = one slow partition leader. `outcome="failure"` is the broker-error count — there is no separate error counter. |
| `consume_group_lag{group,topic,partition}` | cobalt | Gauge from an AdminClient (committed vs `listOffsets(LATEST)`), refreshed every `LAG_REFRESH_INTERVAL`. Not the client's `records-lag-max`, which reads zero during a rebalance and vanishes when the consumer is down. |
| `consume_batch_size{…}` | cobalt | Distribution summary. Small batches under load mean the consumer is *starved*, not saturated. |
| `consume_records_persisted_total{type}` | cobalt | Rows the database took responsibility for, by CloudEvents type. |
| `consume_records_duplicate_total` | cobalt | Idempotent-insert no-ops. Non-zero is **normal** for an at-least-once pipeline; spikes mean redelivery. |
| `consume_records_poison_total{reason}` | cobalt | Records sent to the DLQ. Any sustained non-zero rate is a page. |
| `consume_batch_latency_seconds{outcome}` | cobalt | Batch-insert wall clock. Rising = database or GIN-index pressure. |
| `http_server_requests_seconds{uri,outcome,…}` | all three | The one HTTP timer. Tagged by matched route template, never raw path; the `uri` tag is capped at 100 distinct values. |
| `jvm_*`, `process_*`, `system_*` | all three | The standard binders, including `VirtualThreadMetrics` on JDK 25. |

### 5.1 There are no histogram buckets — do not write `histogram_quantile`

Micrometer publishes a timer to Prometheus as a **summary**: `_count`, `_sum` and a decaying `_max`. Nothing in
`modules/observability` enables `publishPercentileHistogram`, so there is not a single `_bucket` series in the
exposition and `histogram_quantile(…_bucket)` silently returns nothing — an empty panel, not an error. Verified by
scraping `/metrics`.

What these meters can answer is the mean over an interval and the interval max:

```promql
# mean latency
sum(rate(consume_batch_latency_seconds_sum[5m]))
  / clamp_min(sum(rate(consume_batch_latency_seconds_count[5m])), 0.001)

# worst observation Micrometer is still carrying (a ~2-minute decaying window)
max(consume_batch_latency_seconds_max)
```

`clamp_min` is not decoration: without it the ratio is `0/0` whenever traffic stops, and the panel goes to `NaN`
rather than to zero. Adding real quantiles means enabling percentile histograms in `Telemetry`, which multiplies
the series count for every timer in the fleet — a decision, not an omission.

Useful expressions:

```promql
# ingest throughput and rejections by reason
sum(rate(ingest_events_received_total[5m]))
sum by (reason) (rate(ingest_events_rejected_total[5m]))

# total consumer lag, and whether it is growing
sum by (group) (consume_group_lag)
deriv(sum(consume_group_lag)[15m:])            # > 0 sustained = falling behind

# write path health
sum by (reason) (rate(consume_records_poison_total[15m]))   # alert on > 0 for 15m
max by (outcome) (consume_batch_latency_seconds_max)

# ingest durability — there is no separate produce-error counter
sum by (outcome) (rate(kafka_produce_latency_seconds_count[5m]))

# UI latency (search has no dedicated timer today — see Known limitations)
sum by (uri) (rate(http_server_requests_seconds_sum{service="ferrite",uri=~"/events.*"}[5m]))
  / clamp_min(sum by (uri) (rate(http_server_requests_seconds_count{service="ferrite",uri=~"/events.*"}[5m])), 0.001)

# partition headroom — the leading indicator for the worst failure mode here
min(maintenance_partitions_headroom)            # alert below 2
sum(partition_default_rows)                     # alert above 0
```

Suggested alerts: `consume_group_lag` above a threshold for 10 minutes; any `consume_records_poison_total`
increase over 15 minutes; `kafka_produce_latency_seconds_count{outcome="failure"} > 0`; readiness probe failing
on any service for 5 minutes; `up{job="observatory"} == 0`; and — see §7 — partition headroom.

### 5.2 The provisioned dashboard

`deploy/observability/dashboards/observatory.json` is provisioned into Grafana's **Observatory** folder as
*Event observatory*, by `deploy/observability/grafana-dashboards.yml`. It is a file in this repository, not an
object in Grafana's database: the `grafana-data` volume can be destroyed without losing it.

Five rows, in the order an incident is actually worked:

| Row | Panels |
| --- | --- |
| **Fleet** | Ingest rate, rejection rate, total consumer lag, dead-letter rate, partition headroom, targets up. |
| **Ingest — wolfram** | Accepted events by `type`, rejections by `reason`, produce latency (mean and max) and produce outcomes. |
| **Consume — cobalt** | Lag per partition, batch write latency, persisted vs de-duplicated rows, dead letters by reason, batch size against the cap, and the scheduled-maintenance job outcomes. |
| **Search and HTTP** | ferrite's `/events` latency, and request rate by service and outcome. |
| **JVM health** | Heap used vs max, GC pause and overhead, CPU, threads (platform and virtual), allocation rate and uptime — all grouped by `service`, so one row covers all three. |

Every panel is written against the shared meter names in `modules/observability`'s `Meters`, so a panel that is
blank for one service means that service is not emitting, never that the panel is spelled for a different one.

Two things about it that are load-bearing:

- **The datasource is referenced by `uid: observatory-prometheus`**, pinned in `grafana-datasources.yml`. Left to
  Grafana to generate, the uid differs per install and every panel comes up "Datasource not found".
- **The provider config and the dashboards are in different directories.** Grafana parses everything under the
  provider's `options.path` as a dashboard, so a provider YAML living beside them is read as one and fails on
  every boot. That is why compose mounts the config to `/etc/grafana/provisioning/dashboards/` and the JSON to
  `/var/lib/grafana/dashboards/`.

Provisioned dashboards are read-only in the UI (`allowUiUpdates: false`), deliberately: an edit saved in the
browser is reverted by the next provisioning sweep and the person who made it gets no warning. Edit the JSON.

---

## 6. Runbooks

### 6.1 Ingestion has stopped

Symptom: `ingest_events_received_total` flat, or clients report errors.

1. **Is wolfram accepting requests at all?** `curl -i localhost:8081/health/live`. A dead process is a container
   problem (`docker compose ps`, `docker compose logs wolfram`), not an ingestion problem.
2. **Is the broker reachable?** `curl -i localhost:8081/health/ready` — a `503` with `"broker":"unreachable"`
   means every request is being shed. Check `docker compose ps kafka` and the broker healthcheck.
3. **What are clients actually getting?** Split by status:
   - `400` — `ingest_events_rejected_total{reason="malformed"|"invalid-attributes"}`. The most common cause in
     the field is `time` outside the plausibility window: a device with a wrong clock is rejected at
     `INGEST_MAX_FUTURE_SKEW` (24 h) or `INGEST_MAX_PAST_SKEW` (90 d). The rejection body's `detail` names it.
     Second most common: headers stripped in transit — binary mode carries every attribute in `ce-*` headers, and
     any proxy that drops them leaves a payload with no identity, which is refused rather than defaulted.
   - `413` — `reason="too-large"`: over `INGEST_MAX_EVENT_BYTES`, or a batch over `INGEST_MAX_BATCH_EVENTS`.
   - `503` — either the broker did not acknowledge within `KAFKA_DELIVERY_TIMEOUT`, or ingestion shed load
     because more than `KAFKA_QUEUE_CAPACITY` sends were in flight. Both are retryable and the event was *not*
     published. Confirm with `kafka_produce_latency_seconds_count{outcome="failure"}`.
4. **Does the topic exist?** Auto-creation is disabled. If `kafka-init` never completed (`docker compose ps`
   shows it as anything but `exited (0)`), produces fail. Re-run it: `docker compose up kafka-init`.
5. **Nothing wrong upstream?** `http_server_requests_seconds_count{service="wolfram"}` at zero means no one is
   calling. Check the producers, DNS, and the port mapping (`8081` on the host, `8080` in the container).

### 6.2 Consumer lag is growing

Symptom: `sum(consume_group_lag)` rising.

1. **Is cobalt consuming at all?** `curl -s localhost:8082/health/ready | jq` reports the broker and the database
   separately, each with a reason. Note that liveness stays `UP` even when the consumer stream has given up:
   the stream's restart budget is 50 restarts in 10 minutes and, once exhausted, the process keeps answering
   probes while consuming nothing. `docker compose logs cobalt | grep -i restart` and restart the container.
2. **Starved or saturated?** `consume_batch_size` distinguishes them. Batches far below `CONSUMER_BATCH_SIZE`
   (500) while lag grows means the consumer is *waiting* — network, broker, or too few partitions being fetched.
   Batches at the cap with rising `consume_batch_latency_seconds` means the write side is the bottleneck.
3. **Write side.** Check `consume_batch_latency_seconds{outcome="failure"}` and Postgres: connection count,
   locks, autovacuum on the current month's partition, and the four GIN indexes, which materially slow inserts.
   The write pool is 6 connections by design; raising it adds lock contention on the same partitions rather than
   throughput.
4. **Missing partition.** `no partition of relation "cloud_event" found for row` in cobalt's logs is the single
   most damaging failure mode of a partitioned design: Postgres reports it as SQLSTATE 23514, which cobalt
   classifies as a *data* error — so the pipeline does not stall, it **dead-letters every affected event** at full
   throughput. cobalt's partition-maintenance job creates months ahead every six hours, so reaching this state
   means the job stopped or was blocked; `maintenance_partitions_headroom` and
   `maintenance_job_duration_seconds_count{job="partition-maintenance"}` say which. See §7.3.
5. **Scale.** The topic has 12 partitions, so up to 12 cobalt replicas can share the work. `docker compose up -d
   --scale cobalt=3` requires removing cobalt's fixed `8082:8080` host mapping first — a fixed published port
   makes a second replica fail to start. Beyond 12 replicas, add partitions. Raising `CONSUMER_BATCH_SIZE` helps
   only when batches are already at the cap.
2. **Backlog burn-down is safe.** Offsets are committed only after a durable write, and the write deduplicates on
   `(occurred_at, ce_source, ce_id)`, so replay never duplicates rows — `consume_records_duplicate_total` simply
   rises.

### 6.3 The DLQ is filling

Symptom: `rate(consume_records_poison_total[15m]) > 0`.

1. **Which reason?** `sum by (reason) (rate(consume_records_poison_total[15m]))`:
   - `malformed` / `invalid-attributes` / `invalid-payload` — a producer is shipping events this build cannot
     decode. Fix the producer; the events are recoverable from the DLQ.
   - `unknown-type` — a `type` no consumer here recognises. Benign and transient during a rolling deploy; a
     persistent rate means a producer shipped before its consumer.
   - `unpersistable` — the database deterministically refused the row (SQLSTATE class 22 *data exception* or 23
     *integrity constraint*). **This is the class that includes a missing partition** — check §6.2 step 4 first,
     because that cause dead-letters good events at full throughput.
2. **Read the dead letters.** They are structured-mode CloudEvents keyed `topic/partition/offset`, carrying the
   origin coordinates, the `reason` and a human `detail`:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:9092 --topic events.cloudevents.v1.dlq \
     --from-beginning --max-messages 20 --property print.key=true
   ```

3. **Mind the clock.** The DLQ inherits `KAFKA_LOG_RETENTION_HOURS=168` — dead letters are gone after **7 days**.
   If a fix will take longer than that, copy the topic out to durable storage first.
4. **Replay.** There is no replay tool in this repo. Once the defect is fixed, re-publish the original events
   through `POST /events` on wolfram; the idempotent insert makes a re-ingested event a no-op if it did land.
5. **Rate limiting.** DLQ publishes are sequential on purpose — a poison burst is by definition a bad moment for
   the pipeline, and a fan-out of produces at a struggling broker turns it into an outage. A slow DLQ therefore
   also slows the main path; that is the intended trade.

### 6.4 Search is slow

1. **Confirm it is the database.** The mean-latency expression in §5.1 for `{service="ferrite",uri="/events"}`,
   or the *Search latency* panel. The read pool
   sets `statement_timeout = '2s'` server-side, so a genuinely runaway query surfaces as a failed page within two
   seconds rather than as a hang — if pages hang longer than that, the wait is for a *connection*, not a query.
2. **Pool saturation.** Read pool: 8 connections, `connection-timeout` 3 s, fronted by an 8-thread dispatcher.
   Symptoms are 3-second failures and a flapping `/health/ready`. Causes: too many concurrent users, or a leak.
   Raise `database.read.maximum-pool-size` **and** `database.search-dispatcher.fixed-pool-size` together — they
   must stay equal.
3. **Query shape.** The expensive shapes are: no time bound (no partition pruning — the fact table is
   range-partitioned by month), a free-text term over a wide window, and facet computation over a broad filter.
   Totals are capped (the UI renders `10,000+`), so a slow page is the page query, the facets or the histogram,
   not the count. Reproduce with `EXPLAIN (ANALYZE, BUFFERS)` and check that partitions were pruned.
4. **Index health.** Every index in `V1__events.sql` carries a `COMMENT ON INDEX` naming the query it serves —
   start there when a plan does not use the index you expected. GIN indexes on `data`, `extensions`, `tags` and
   `search_doc` are the ones whose `fastupdate` pending list can make both reads and writes erratic; a `VACUUM`
   on the hot partition flushes it.
5. **Statistics.** `ce_type` and `device_id` carry `SET STATISTICS 1000`. After a large backfill, `ANALYZE
   events.cloud_event` before concluding the plan is wrong.
6. **Note:** `search.query.duration` and `search.facets.capped` are declared in `Meters` but are **not emitted**
   today — use `http_server_requests` until they are wired (see Known limitations).

---

## 7. The event store: backup, retention, partitions

### 7.1 What is durable, and what is not

| Volume | Holds | Consequence of loss |
| --- | --- | --- |
| `postgres-data` | The event store. | Total loss of history. This is the volume that matters. |
| `kafka-data` | The Kafka log — **see the caveat below.** | Loss of un-consumed events and of consumer offsets. |
| `prometheus-data` | 30 days of metrics. | Dashboards lose history; nothing else breaks. |
| `grafana-data` | Dashboards, users. | Re-provision. |

> `KAFKA_LOG_DIRS: /var/lib/kafka/data` must stay set on the `kafka` service. The `apache/kafka` image defaults
> to `/tmp/kraft-combined-logs`, which is *inside the container*: without the variable the `kafka-data` volume is
> mounted and never written to, and a container recreate discards the log and the committed consumer offsets.
> Verify with `docker compose up -d --force-recreate kafka` — the topics and the group's offsets must survive.

### 7.2 Backup

The database is the only thing that cannot be reconstructed.

```bash
# nightly logical dump, custom format, parallel restore-able
docker compose exec -T postgres pg_dump -U observatory -d observatory -Fc \
  | gzip > observatory-$(date -u +%Y%m%d).dump.gz

# restore into an empty database
gunzip -c observatory-20260726.dump.gz \
  | docker compose exec -T postgres pg_restore -U observatory -d observatory --clean --if-exists
```

Notes that matter for this schema:

- **Dump per partition when the table gets large.** `pg_dump -t 'events.cloud_event_2026_*'` lets you take a
  fast incremental copy of only the months that changed; older months are immutable once their month closes.
- **Generated columns are not dumped as data** — they are recomputed on restore from `raw`, which is the point of
  storing the CloudEvent verbatim. A restore that changes `events.severity_rank` would silently change history;
  keep that function in lockstep with `io.kzonix.kernel.search.Severity`.
- **The materialized view restores empty** (`WITH NO DATA` semantics differ per restore path); refresh it
  explicitly if you start depending on it.
- **Run everything with `-Duser.timezone=UTC`/`TZ=UTC`.** Partition bounds are parsed in the session timezone; a
  non-UTC session silently shifts every partition.
- Test restores. A backup nobody has restored is a hypothesis.

### 7.3 Retention and partition maintenance

The fact table is `PARTITION BY RANGE (occurred_at)`, monthly. Retention is therefore a `DETACH`, never a
`DELETE`:

```sql
-- drop a month of history (instant, no bloat, no vacuum)
ALTER TABLE events.cloud_event DETACH PARTITION events.cloud_event_2025_07;
DROP TABLE events.cloud_event_2025_07;          -- or keep it detached as an archive
```

**Creating future partitions is cobalt's job, not yours.** `MaintenanceJobs` runs `PartitionMaintenance` every
`MAINTENANCE_PARTITION_INTERVAL` (6 hours) behind an advisory lock, creating the current month and
`MAINTENANCE_MONTHS_AHEAD` (3) beyond it with the same autovacuum settings the migration applies, and detaching
past `MAINTENANCE_RETAIN_MONTHS` when — and only when — that is configured. `V1__events.sql` still ships only
2026-07 and 2026-08; the job supplies the rest on first run.

| Variable | Default | Effect |
| --- | --- | --- |
| `MAINTENANCE_ENABLED` | `true` | The escape hatch for a deployment whose partitions are managed elsewhere. |
| `MAINTENANCE_MONTHS_AHEAD` | `3` | Months of headroom, so the job may miss three consecutive runs before an event has nowhere to go. |
| `MAINTENANCE_RETAIN_MONTHS` | *(unset)* | Retention, counting back from and including the current month. Unset means **never detach** — the one operation here that makes data vanish from queries happens because somebody configured it. |
| `MAINTENANCE_PARTITION_INTERVAL` | `6 hours` | How often the calendar is re-checked. |
| `MAINTENANCE_REFRESH_INTERVAL` | `5 minutes` | Rollup refresh period. |
| `MAINTENANCE_DETACH_LOCK_TIMEOUT` | `5 seconds` | Bound on waiting for `ACCESS EXCLUSIVE` during a detach. Waiting is the dangerous part: every ingest insert queues behind the waiter, so the job gives up and retries next pass. |

Watch `maintenance_partitions_headroom` (alert below **2**) and
`maintenance_job_duration_seconds_count{job="partition-maintenance"}` (alert on *no* run, which is the only trace
a dead job thread leaves). Doing it by hand is still the fallback if the job is disabled — the autovacuum settings
are *not* inherited, and the newest partition is the only one being written to:

```sql
CREATE TABLE events.cloud_event_2026_09 PARTITION OF events.cloud_event
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');
ALTER TABLE events.cloud_event_2026_09 SET (
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold    = 50000);
```

Always write the explicit `+00` offset. Never a bare date.

**Watch the default partition.** `events.cloud_event_default` is a clock-skew safety net and must stay empty:

```sql
SELECT count(*) FROM events.cloud_event_default;   -- must be 0
```

Once it holds rows, creating an overlapping partition takes `ACCESS EXCLUSIVE` on the hierarchy and scans it, so
the empty-check is the cheap moment to act. cobalt emits `partition_default_rows` for exactly this alert, and
`maintenance_partitions_blocked` counts the months whose partition it *could not* create for this reason — a
non-zero value there is a request for a human, and the job has already logged the exact statements to run, with
the actual bounds, for the actual table. Moving those rows is an exclusive-lock operation no unattended job
should take on an operator's behalf.

Kafka retention is `KAFKA_LOG_RETENTION_HOURS=168` (7 days) for both topics: the bus is a buffer, not an archive.
Prometheus keeps 30 days (`--storage.tsdb.retention.time=30d`).

### 7.4 Adding an index to a live table

Indexes on a partitioned parent cannot be built `CONCURRENTLY` and take `ACCESS EXCLUSIVE` on the whole
hierarchy. On a live system: `CREATE INDEX ON ONLY parent`, then `CREATE INDEX CONCURRENTLY` per partition, then
`ALTER INDEX … ATTACH PARTITION`.

---

## 8. Known limitations

Each entry below was re-checked against the code and `deploy/` on the date of the last commit to this file, and
each one is still true. **That check is the point of the list.** An entry that has been fixed and left in place
does more damage than no list at all: it makes the entries that *are* real look equally doubtful, and the next
person triaging an incident wastes their first ten minutes ruling out a defect that no longer exists. If you fix
one, delete it here in the same change.

Ordered by operational blast radius.

1. **Single-node Kafka, replication factor 1, is not highly available.** One broker, `KRaft` combined
   broker+controller, `RF=1` on both topics and on the internal offsets/transaction-state topics. There is no
   redundancy: broker downtime is ingestion downtime, and loss of the log is loss of everything not yet consumed.
   The `acks=all` + idempotent producer settings are honest but only ever wait for one replica. Suitable for a
   homelab; a production deployment needs three brokers and `RF=3` with `min.insync.replicas=2`.
2. **Play is on a milestone release.** `3.1.0-M9` is the only Play line cross-published for sbt 2; the stable
   3.0.x line ships an sbt 1 plugin only. Test-kit and server APIs can shift between milestones, and there is no
   security-support commitment for a milestone. Pin every Play artifact to one version and bump them together;
   do not "fix" the version without accepting the loss of sbt 2.
3. **Timers have no percentile histograms, so no quantiles exist.** `publishPercentileHistogram` is not enabled
   anywhere in `modules/observability`, so every Micrometer timer reaches Prometheus as a summary and there is not
   one `_bucket` series in the exposition. `histogram_quantile` returns nothing, quietly. §5.1 has the expressions
   that do work, and the provisioned dashboard uses them. Enabling buckets is a real decision — it multiplies the
   series count for every timer in the fleet — not an oversight to be corrected in passing.
4. **Three declared metrics are never emitted:** `search.query.duration`, `search.facets.capped` and
   `event.unrecognised`; so is Hikari's `db.pool.*` binding, because the pools are built without a
   `MetricsTrackerFactory`. Confirmed by scraping `/metrics`. Use `http_server_requests` for search latency
   (§5.1). Everything else in the vocabulary **is** emitted, including `partition.default.rows`,
   `maintenance.partitions.headroom` and `maintenance.partitions.blocked`.
5. **The hourly rollup is created, refreshed, and read by nothing.** `events.event_rollup_hourly` is populated by
   the migration and kept current by `RollupRefresh` on cobalt's maintenance schedule
   (`maintenance_job_duration_seconds{job="rollup-refresh"}` increments), but ferrite's histogram queries the fact
   table directly. It is a standing cost with no reader: either wire it into the histogram query or drop it.
6. **Compose omits the hardening and tuning ADR §10.2 calls for:** no `security_opt: [no-new-privileges:true]`,
   no `pg_stat_statements`/`track_io_timing`/`log_min_duration_statement` on Postgres, no `postgres-exporter`.
   None is required to run; all are cheap to add and the first slow-query investigation will want
   `pg_stat_statements`.
7. **Traces go to the collector's `debug` exporter only** — they are logged and dropped. The pipeline is live (a
   single smoke event produces `resource spans: 2, spans: 3`, so HTTP → Kafka → consumer is one trace), but point
   `otel-collector.yaml` at Tempo or Jaeger before you need to *read* one.
8. **ferrite ships `logback.xml` twice** — once inside `io.kzonix.ferrite-*.jar` and once at `conf/logback.xml`,
   because Play's packaging maps `src/main/resources` into both. Verified in the staged image. Logback logs a
   `Resource [logback.xml] occurs multiple times` WARN on boot and uses the `conf/` copy. Both copies are the same
   file, so the outcome is deterministic; it is noise, not a defect, but it will confuse the first person who
   greps for it.
9. **ferrite's stylesheet is committed output and the build does not verify it.** `sbt ferrite/tailwindCheck`
   exists and catches drift, but it needs the Tailwind CLI binary and is therefore not part of `verify`, which
   must stay runnable with nothing but a JDK. A template that gains a utility class in a change where nobody ran
   the task ships a page that renders without it. See `docs/development.md` §8.

Fixed, and no longer listed: `deploy/.env.example` (present), ferrite's `DockerPlugin` (enabled, so
`ferrite:latest` builds), compose's database variables (`DATABASE_URL`/`DATABASE_USER`/`DATABASE_PASSWORD`,
matching what the services read), and the absence of a partition-maintenance job (cobalt runs one — §7.3).

---

## See also

- `docs/adr/0000-architecture.md` — the architecture contract, including the schema DDL and the index rationale.
- `docs/development.md` — building, testing, and changing the system.
