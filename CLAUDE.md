# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

An **event observatory** for smart-home / IoT telemetry: CloudEvents in over HTTP, through Kafka, into PostgreSQL,
out through a server-rendered search UI. One sbt 2 build, package prefix `com.worxbend`.

```
client --POST /v1/events--> wolfram --> Kafka --> cobalt --> PostgreSQL <-- ferrite --> browser
```

Three deployable services under `applications/`, each on a different Scala 3 web stack. They are named after
metals, not after their frameworks, so the name does not have to change if the stack does.

| Module | Stack | Responsibility |
| --- | --- | --- |
| `applications/wolfram` | Tapir on Vert.x 5 | Ingestion. JWT-authenticated, AIP-shaped `/v1/events`, OpenAPI + Swagger UI. Publishes to Kafka. Owns no state. |
| `applications/cobalt` | Pekko Streams Kafka + Cask | Consumes, decodes, persists. Runs the Flyway migrations. Owns the consumer supervisor, the DLQ surface and scheduled maintenance. |
| `applications/ferrite` | Play 3 + Twirl/htmx/Alpine/Tailwind | The web application: search, an overview dashboard, an SSE live tail. Reads Postgres; never sees Kafka. |

Four shared libraries under `modules/`. They have no `main` and produce no image.

| Module | Contents |
| --- | --- |
| `modules/kernel` | The domain: CloudEvents `Envelope`, the `Observation` ADT, the search `Filter` ADT and its querystring codec. |
| `modules/eventing` | The Kafka wire format: CloudEvents binary/structured codecs, the dead-letter envelope, trace propagation. |
| `modules/persistence` | Flyway schema, Hikari pools, Magnum repositories, the `Filter` → SQL compiler, maintenance jobs. |
| `modules/observability` | The Micrometer/Prometheus meter vocabulary, OTel tracing, log context. |

Dependencies point inward: `ferrite → kernel, persistence, observability`;
`cobalt → kernel, eventing, persistence, observability`; `wolfram → kernel, eventing, observability`.

**`modules/kernel` must stay framework-free.** `build.sbt` asserts it at build load: any compile-scoped dependency
outside `io.circe` / `org.scala-lang` **fails the build**. Test-scoped ones are allowed.

`applications/` stays at exactly three. New shared code is a `modules/` library, added with `library("name")`.

## Commands

```bash
sbt verify      # fmtCheck + headerCheck + IT/headerCheck + IT/scalafmtCheck + IT/compile + Test/testFull.
                # Exactly what CI runs. Fast; no Docker.
sbt verifyIt    # IT/testFull — the slow tier. Needs a working Docker daemon; the suites start their own containers.
sbt fmt         # scalafmt, build sources included
sbt headerCreate  # stamp licence headers on new files — never hand-write one
sbt doc         # Scaladoc; -Werror applies, so a broken doc link fails the build

sbt wolfram/run   # :8080 (HTTP_PORT). Needs AUTH_SECRET, or AUTH_ENABLED=false.
sbt cobalt/run    # :8080 (HTTP_PORT)
sbt ferrite/run   # :9000, Play dev mode

sbt "cobalt/testOnly com.worxbend.cobalt.BatchProcessorSuite"
sbt ferrite/Docker/publishLocal
```

> **The sbt 2 `test` trap.** sbt 2 **inverted** sbt 1's naming: `test` is the *incremental* task and `testFull` runs
> everything. `Test/test` reports success having executed zero tests. Always `Test/testFull` and `IT/testFull` —
> which is why `verify` is spelled that way. Never run `sbt clean`.

Run `sbt verify` before handing work back — `headerCheck` fails on any file `sbt-header` has not stamped, and new
files are only stamped once they have been compiled or `sbt headerCreate` has run.

## Toolchain constraints

- **sbt 2.0.3, Scala 3.8.4, JDK 25.** Build definitions under `project/` are themselves Scala 3. `.sdkmanrc` pins
  the JDK and sbt; `sdk env` adopts them.
- **Play is pinned to `3.1.0-M9`, a milestone.** This is deliberate and load-bearing: `3.1.0-M9` is the first Play
  line cross-published for sbt 2 (`sbt-plugin_sbt2_3`). The stable 3.0.x line ships only an sbt 1 plugin
  (`sbt-plugin_2.12_1.0`). Going back to a stable Play means giving up sbt 2, so do not "fix" the milestone
  version without raising that trade-off.
- sbt 2 plugin artifacts use the `_sbt2_3` suffix, not `_3_2.0`. When checking whether a plugin supports sbt 2,
  search Maven Central for `<plugin>_sbt2_3` — the wrong pattern makes supported plugins look unavailable.
- **Play 3 uses `jakarta.inject`,** not `javax.inject`.

## Compiler flags that will bite

`project/BaseSettings.scala` sets, and `-Werror` promotes all of these to errors:

- `-new-syntax -indent` — **indentation syntax is mandatory.** Braces are a compile error, not a style nit. Match
  the surrounding style exactly. 120 columns.
- `-Wunused:all` — an unused import or parameter fails the build. This catches unused `using ec: ExecutionContext`
  parameters, easy to leave behind when a method stops returning a `Future`. It does **not** catch an unused public
  method or an unreferenced constant, which is where dead code actually accumulates.
- `-Wvalue-discard` / `-Wnonunit-statement` — a discarded non-`Unit` value fails the build. Every Micrometer and
  OTel builder returns `this`, and Guice builder chains (`bind[X].asEagerSingleton()`) return values, so bind them
  to `val _ =` or chain into one expression.

`-Wnonunit-statement` is **removed in `Test` scope only** (`Test / scalacOptions`), because ScalaTest's `assert`
returns an `Assertion` and every multi-assertion test would otherwise fail. Do not re-add it there.

Twirl-generated sources do not obey these rules, so ferrite drops `-new-syntax` in `Compile` only.

## Dependencies

`project/Dependencies.scala` owns every coordinate; `Versions` is public so `build.sbt` can reference it directly.
Every module except `kernel` automatically gets pureconfig, quicklens, scala-logging and logback (main) plus
scalatest, scalacheck, munit and munit-scalacheck (test) — do not re-declare those per module.

Two coordinate traps, both already resolved:

- pureconfig publishes **no `pureconfig_3` aggregate**; Scala 3 derivation lives in `pureconfig-core`.
- The `scala-garden/scala-logging` fork on Scaladex publishes nothing to Maven Central. The build uses
  `com.typesafe.scala-logging`. Switching to the fork would require a non-Central resolver.

## Testing

munit leads. ScalaTest only where Play's test helpers require it; scalacheck for properties — `FilterGenerators`,
`WireGenerators` and `Generators` exist, reuse them. Test the pure decision, not the socket.

Integration tests live in `src/it/scala`, provision their own Testcontainers, and run only in `IT/testFull`. The
fast tier still compiles, formats and header-checks that tree, so a broken `src/it` fails `verify`.

## Per-service notes

**wolfram** (Tapir + Vert.x) — endpoints are **values** in `Endpoints`, with the server logic bound separately in
`IngestApi`; the same values drive the Vert.x routes, the OpenAPI document, Swagger UI and the stub-server tests.
The surface follows Google's AIPs: `POST /v1/events` is the Create, custom methods use a **colon**
(`/v1/events:batchCreate`, `/v1/events:validate`), and every failure is the AIP-193 `{"error": {…}}` envelope.
Every `/v1` operation is authenticated — the security logic is applied once, on the shared `base` endpoint, so
there is no route that *could* be added without it. `AUTH_ENABLED` defaults to `true` and there is no key default:
a process with neither `AUTH_SECRET` nor `AUTH_ENABLED=false` refuses to boot.

**cobalt** (Cask) — routes are annotations on `cask.MainRoutes` in `AdminRoutes`; handler bodies delegate to pure
functions so behaviour is testable without binding a socket. Beyond metrics and health it owns the consumer
supervisor (pause/resume/restart/offset control), the DLQ inspect+replay surface and the scheduled maintenance
jobs. tapir is on this classpath for **documentation only** — the endpoint values generate the admin API's
OpenAPI, there is no server interpreter, and `CobaltApiDocsSuite` asserts the description and the Cask routes
cannot drift apart.

**ferrite** (Play 3) — **there is no `conf/routes` file.** Routing is `routing/Routers.scala`, plain
`SimpleRouter`s using SIRD interpolation, selected by `play.http.router` in `application.conf`. Adding an endpoint
means: controller method → `case` in the router. Do not introduce a routes file.

`public/css/app.css` is **committed Tailwind CLI output**. If you touch a template's classes you must run
`sbt ferrite/tailwind` and verify with `sbt ferrite/tailwindCheck`. Neither is in `verify`, because `verify` must
stay runnable with nothing but a JDK — without the CLI both warn and pass.

Play's test helpers stream the result, so a suite calling `Helpers.call` needs both a `Materializer` in scope and
`Helpers.writeableOf_AnyContentAsEmpty`.

## Configuration

`application.conf` files hold **overrides only**. Earlier revisions inlined verbatim copies of upstream reference
configuration, which meant every upstream change had to be merged by hand — don't reintroduce that.

Everything deployment-shaped comes from the environment with development-only defaults: `HTTP_HOST` / `HTTP_PORT`,
`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `DATABASE_URL` / `DATABASE_USER` / `DATABASE_PASSWORD`, `AUTH_*`
(wolfram), `APPLICATION_SECRET` and `ALLOWED_HOSTS` (ferrite), `SERVICE_VERSION` / `LOG_LEVEL` / `OTEL_*` (all).
`docs/operations.md` §3 is the full reference. **CSRF is enabled on ferrite** — it serves a browser form and htmx
requests that carry a token; the `play.filters.disabled += CSRFFilter` line was removed rather than commented out.

## Documentation

- `docs/adr/0000-architecture.md` — the architecture contract: dependency table, schema DDL, index rationale,
  risks and fallbacks. **Read this first.** Code comments cite it by section (`ADR §4.3`); keep those accurate.
- `docs/services/{wolfram,cobalt,ferrite}.md` — one page per service.
- `docs/operations.md` — runbooks, environment variables, metrics, and §8 "Known limitations".
- `docs/event-model.md`, `docs/data/schema.md`, `docs/development.md`.

This codebase documents **why**, not what. Scaladoc on a public type explains the decision and names the failure
mode it avoids. Match that register: terse, concrete, no marketing, and never a comment that restates the code.
A stale comment here is more dangerous than in a codebase nobody reads, because these are trusted — if you change
behaviour, change the sentence that described it.
