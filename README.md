# kzonix

An **event observatory** for smart-home and IoT telemetry. It ingests [CloudEvents](https://cloudevents.io/) over
HTTP, streams them through Kafka, stores them in PostgreSQL, and serves a server-rendered UI for exploring,
searching and monitoring them.

The design goal is closer to Grafana, Kibana or Home Assistant than to a CRUD application: the event log is the
product, and search over it is the primary feature.

## The three services

Three deployable services, each on a different Scala 3 web stack, sharing one sbt 2 build. Named after metals
rather than their frameworks, so a name does not have to change if a stack does.

| Service | Stack | Responsibility |
| --- | --- | --- |
| **wolfram** | Tapir on Vert.x 5 | HTTP ingestion. Validates CloudEvents and publishes to Kafka. Owns no state. |
| **cobalt** | Pekko Streams Kafka + Cask | Consumes, decodes and persists events. Cask serves only metrics and health. |
| **ferrite** | Play 3 + Twirl/htmx | The web application: PostgreSQL, search, and the UI. Never sees Kafka. |

```
client --POST /events--> wolfram --> Kafka --> cobalt --> PostgreSQL <-- ferrite --> browser
```

## Shared libraries

`applications/` holds exactly those three services. The shared contracts live in `modules/`, as libraries with no
`main` and no image:

- **kernel** — the domain: the CloudEvents envelope, the observation ADT and the search filter grammar. Depends on
  circe and the standard library and *nothing else*; a build-load assertion fails the build if that ever changes.
- **eventing** — the only place that knows both the domain envelope and the Kafka wire format, so the producer and
  the consumer cannot disagree about the encoding.
- **persistence** — the schema, the connection pools, the Flyway migrations, and the compiler from a search filter
  to parameterised SQL.
- **observability** — one metric vocabulary and one tracing setup, shared so dashboards written against one
  service work against the others.

Dependency arrows point inward: `ferrite → kernel, persistence, observability`;
`cobalt → kernel, eventing, persistence, observability`; `wolfram → kernel, eventing, observability`.

## Design commitments

**CloudEvents are the source of truth.** Events are stored verbatim as `jsonb`; every queryable column is
`GENERATED ALWAYS AS … STORED` from that raw document, so a projection cannot drift from the payload it describes.
An event type this system has never seen is still persisted, still searchable, and still viewable.

**Search is pure PostgreSQL.** JSONB with GIN, BRIN on time, partial indexes and a rollup materialized view — no
second datastore.

**At-least-once, made idempotent.** The consumer commits only after a durable write, and the write deduplicates on
the CloudEvents `(source, id)` identity, so a redelivery is a no-op rather than a duplicate row.

**One trace, end to end.** A W3C trace context is injected into Kafka headers at ingestion and extracted by the
consumer, so a single trace spans HTTP → Kafka → database.

## Toolchain

sbt 2.0.3, Scala 3.8.4, JDK 25, Play 3.1.0-M9. `.sdkmanrc` pins the JDK and sbt; run `sdk env` to adopt them.

Scala sources use the indentation-based syntax. `-new-syntax` is enabled and warnings are fatal (`-Werror`,
`-Wunused:all`, `-Wvalue-discard`), so brace-style code and unused imports are compile errors rather than review
comments.

## Commands

```bash
sbt verify      # fmtCheck + headerCheck + Test/testFull — exactly what CI runs. Fast; no Docker.
sbt verifyIt    # IT/testFull — the slow tier. Needs a working Docker daemon.
sbt fmt         # scalafmt, build sources included
sbt headerCreate  # stamp licence headers — never hand-write one
sbt doc         # Scaladoc; -Werror applies, so a broken doc link fails the build

sbt wolfram/run   # :8080 (HTTP_PORT)
sbt cobalt/run    # :8080 (HTTP_PORT)
sbt ferrite/run   # :9000, Play dev mode
```

Run a single suite with `sbt "cobalt/testOnly io.kzonix.cobalt.BatchProcessorSuite"`.

> **The sbt 2 `test` trap.** sbt 2 inverted sbt 1's naming: `test` is *incremental* and `testFull` runs
> everything. That is why `verify` is spelled `Test/testFull`. Never run `sbt clean`.

## Running the stack

`deploy/docker-compose.yml` brings up the whole system — Postgres, Kafka, the three services, an OpenTelemetry
collector, Prometheus and Grafana — on a single host.

```bash
sbt cobalt/Docker/publishLocal wolfram/Docker/publishLocal
cd deploy
$EDITOR .env              # POSTGRES_PASSWORD, APPLICATION_SECRET, GRAFANA_ADMIN_PASSWORD are mandatory
docker compose config -q  # validates interpolation and the mandatory vars
docker compose up -d
```

Then the UI is at <http://localhost:9000/events>, ingestion at <http://localhost:8081/events>, Prometheus at
`:9090` and Grafana at `:3000`. See [docs/operations.md](docs/operations.md) for the smoke test, the runbooks and
the environment-variable reference.

> **The stack does not yet come up clean from a cold checkout.** `deploy/.env.example` is missing, ferrite has no
> `DockerPlugin` (so `ferrite:latest` cannot be built), and compose passes `DB_URL`/`DB_USER`/`DB_PASSWORD` where
> the services read `DATABASE_*`. All of these and their workarounds are listed under
> [Known limitations](docs/operations.md#8-known-limitations).

## Documentation

- [docs/index.md](docs/index.md) — the documentation site (`mkdocs build --strict`; published to GitHub Pages).
- [docs/adr/0000-architecture.md](docs/adr/0000-architecture.md) — the architecture contract: dependency table,
  schema DDL, index rationale, and the risks with their fallbacks. **Read this first.**
- [docs/event-model.md](docs/event-model.md) and [docs/data/schema.md](docs/data/schema.md) — the wire contract and
  the database.
- [docs/development.md](docs/development.md) — build, test tiers, module layout and the dependency rule.

## Licence

MIT — see [LICENSE](LICENSE). Headers are applied automatically by `sbt-header`.
