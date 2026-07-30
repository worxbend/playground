# Development

Everything needed to build, test and extend the observatory. Read
`docs/adr/0000-architecture.md` first — it is the implementation contract, and this document only tells you how to
work inside it.

---

## 1. Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| JDK | **25** (`25.0.1-tem`) | Pinned in `.sdkmanrc`; the same JDK CI and the container base images use. |
| sbt | **2.0.3** | Pinned in `.sdkmanrc` and `project/build.properties`. sbt 1 will not load this build. |
| Docker | any recent daemon | Required for `sbt verifyIt` (Testcontainers) and for the images. Not needed for `sbt verify`. |
| Python + MkDocs | 3.12, `mkdocs` 1.6.1 + `mkdocs-material` 9.7.7 | Only for building the docs site locally. |
| Tailwind CSS CLI | **4.3.3** standalone binary | Only for `ferrite/tailwind` — see §8. Absent, the task warns and leaves the committed stylesheet alone. |

```bash
sdk env          # adopts the JDK and sbt from .sdkmanrc
java -version    # 25
sbt --version    # 2.0.3
```

The build is Scala **3.8.4** with `-source:3.3`. Build definitions under `project/` are themselves Scala 3.

---

## 2. Command vocabulary

```bash
sbt verify      # fmtCheck + headerCheck + Test/testFull  — exactly what CI runs. Fast; no Docker.
sbt verifyIt    # IT/testFull — the slow tier. Needs a working Docker daemon.
sbt fmt         # scalafmt, build sources included
sbt fmtCheck    # scalafmtSbtCheck + scalafmtCheckAll
sbt headerCreate  # stamp licence headers — NEVER hand-write one
sbt doc         # Scaladoc; -Werror applies, so a broken doc link fails the build
sbt scaladocSite  # doc for every module, collected under target/site/api/<module>

sbt cobalt/run    # :8080 (HTTP_PORT)
sbt wolfram/run   # :8080 (HTTP_PORT)
sbt ferrite/run   # :9000, Play dev mode

sbt ferrite/tailwind       # regenerate ferrite's committed stylesheet — see §8
sbt ferrite/tailwindCheck  # fail if it is stale

sbt "cobalt/testOnly io.kzonix.cobalt.BatchProcessorSuite"
sbt "persistence/IT/testOnly io.kzonix.persistence.MigrationIT"

# One quoted argument each, or a ";a;b;c" sequence: sbt 2 does not split a single space-joined string.
sbt ";ferrite/Docker/publishLocal;cobalt/Docker/publishLocal;wolfram/Docker/publishLocal"
```

Run `sbt verify` before handing work back. `headerCheck` fails on any file `sbt-header` has not stamped, and new
files are only stamped once they have been compiled or `sbt headerCreate` has run.

Never run `sbt clean` — it throws away an incremental-compile state this build takes minutes to rebuild.

### The sbt 2 `test` trap — read this once, remember it forever

**sbt 2 inverted sbt 1's naming.**

| Task | What it does |
| --- | --- |
| `test` | **Incremental.** Runs only the tests affected by what changed. `testQuick` is merely an alias for it. |
| `testFull` | Runs **everything**. |

`Test/test` still selects the incremental task, so a command alias written as `Test/test` reports success while
executing zero tests. That is why `verify` is spelled `Test/testFull` and `verifyIt` is spelled `IT/testFull`.
When you want a real answer, use `testFull`.

### The two test tiers

| Tier | Source root | Config | Needs Docker | Run by |
| --- | --- | --- | --- | --- |
| unit | `src/test/scala` | `Test` | no | `sbt verify` |
| integration | `src/it/scala` | `IT` (declared in `project/ItConfig.scala`, extends `Test`) | yes | `sbt verifyIt` |

sbt no longer ships an `IntegrationTest` configuration, so `IT` is declared by hand. `ItConfig` also wires
`headerSettings(IT)` and `scalafmtConfigSettings(IT)` — without them `IT/headerCheck` and `IT/scalafmtCheck`
simply would not exist and every file under `src/it` would escape the formatting and licence gates. Note that
`verify` runs `headerCheck`, not `IT/headerCheck`: run `sbt IT/headerCheck IT/scalafmtCheck` after touching
integration sources.

`IT/parallelExecution` is `false` (containers bind host ports and would collide) and `IT/fork` is `true`.
The root project carries `.configs(IT)` so `IT/testFull` aggregates to every module; without it, it silently
tests nothing.

---

## 3. Module layout and the dependency rule

```
modules/          libraries — no main, no image
  kernel          the domain: CloudEvents envelope, observation ADT, search filter grammar
  eventing        CloudEvents ↔ Kafka wire adapters, trace propagation
  persistence     Hikari pools, jsonb codecs, filter→SQL compiler, Flyway migrations
  observability   Micrometer registry, OTel tracing, Logback JSON, the metric vocabulary

applications/     exactly three deployable services
  wolfram         Tapir on Vert.x 5     — ingestion
  cobalt          Pekko Streams Kafka + Cask — consumer
  ferrite         Play 3 + Twirl/HTMX   — web UI and search
```

Dependency arrows point inward:

```
ferrite  -> kernel, persistence, observability
cobalt   -> kernel, eventing, persistence, observability
wolfram  -> kernel, eventing, observability
eventing -> kernel
persistence -> kernel
observability -> (nothing in this repo)
```

**`applications/` stays at exactly three.** The three services must agree byte-for-byte on one wire contract and
one metric taxonomy; duplication drift only ever surfaces in production. Shared code goes in a `modules/` library.

**`modules/kernel` must stay framework-free, and the build enforces it — not the reviewer.** `kernel` is declared
with `domainLibrary`, which gives it *no* ambient dependencies (not even the common ones), and `build.sbt` runs an
assertion at build load:

```scala
val allowed = Set("io.circe", "org.scala-lang")
val foreign = declared.filter(m => m.configurations.isEmpty && !allowed(m.organization))
require(foreign.isEmpty, "modules/kernel must stay framework-free; …")
```

Adding a compile-scoped dependency on anything else — Play, Kafka, JDBC, even logging — fails `sbt` before a
single file compiles. Test-scoped dependencies are allowed. That constraint is the whole value of the module: the
domain compiles without a framework in scope, so nothing about the domain can depend on how it is transported.

Every other module automatically gets pureconfig, quicklens, scala-logging and logback (main) plus munit,
munit-scalacheck, scalacheck and scalatest (test) from `commonDependencies`/`testDependencies` — do not re-declare
them. Coordinates live in `project/Dependencies.scala`; `Versions` is public so `build.sbt` can reference it.
Two traps already resolved there: pureconfig publishes no `pureconfig_3` aggregate (Scala 3 derivation lives in
`pureconfig-core`), and the `scala-garden/scala-logging` fork publishes nothing to Maven Central.

---

## 4. Compiler flags that will bite

`project/BaseSettings.scala` sets, and `-Werror` promotes to errors:

- **`-new-syntax -indent` — indentation syntax is mandatory.** Braces are a compile error, not a style comment.
  `.scalafmt.conf` rewrites brace style away (`rewrite.scala3.removeOptionalBraces = yes`), so `sbt fmt` fixes
  most of it for you.
  **Exception:** `ferrite` removes `-new-syntax` (`Compile / scalacOptions ~= (_.filterNot(_ == "-new-syntax"))`)
  because Twirl generates code this build's rules do not govern. Enforcement in ferrite therefore rests entirely
  on scalafmt — write indentation syntax there anyway.
- **`-Wunused:all`** — an unused import or parameter fails the build. This catches unused
  `using ec: ExecutionContext` parameters, which are easy to leave behind when a method stops returning a `Future`.
- **`-Wvalue-discard` / `-Wnonunit-statement`** — a discarded non-`Unit` value fails the build. This is the flag
  you will meet most often, because almost every fluent builder returns `this`:

  | API | What returns non-`Unit` |
  | --- | --- |
  | Guice / Play modules | `bind[X].asEagerSingleton()`, `addBinding.to[Y]` |
  | Micrometer | `MeterRegistry#counter/timer/summary`, `Config#commonTags`, `Config#meterFilter` |
  | OpenTelemetry | every `spanBuilder.set…`, `Span#setStatus`, `Span#recordException` |
  | Vert.x | `router.get(path).handler(…)`, `response().setStatusCode(…)…end(…)` |
  | Kafka | `producer.send(…)`, `Headers#add` |
  | Java executors | `scheduler.scheduleWithFixedDelay(…)` |

  Chain into one expression, or bind with `val _ = …`. The codebase uses `val _ =` consistently; follow it.

- `-Wnonunit-statement` is **removed in `Test` scope only**, because ScalaTest's `assert` returns an `Assertion`
  and every multi-assertion test would otherwise fail. Do not re-add it there.
- 120 columns, enforced by scalafmt.
- Scaladoc is compiled with the same flags: a broken `[[link]]` fails `sbt doc`.
- **Never hard-code an sbt output path.** sbt 2 writes to one shared root — `target/out/jvm/scala-<v>/<project>/`
  — not to `<module>/target/scala-<v>/`. The Pages workflow used to copy Scaladoc from the sbt 1 location behind
  an `[ -d "$src" ] &&` guard, so all seven modules were skipped in silence for as long as it existed. Ask sbt for
  the path (`sbt scaladocSite`, `sbt "show <project>/Compile/doc"`) rather than reconstructing it.

Other toolchain facts worth knowing:

- sbt 2 plugin artifacts use the `_sbt2_3` suffix, not `_3_2.0`. When checking whether a plugin supports sbt 2,
  search Maven Central for `<plugin>_sbt2_3`.
- Play 3 uses `jakarta.inject`, not `javax.inject`.
- `version` comes from `BUILD_VERSION` and defaults to `0.1.0-SNAPSHOT`. Keep it deterministic.
- `application.conf` holds **overrides only**. Never inline a copy of an upstream reference configuration.

---

## 5. Adding an endpoint

The three services route in three deliberately different ways. Keep operational endpoints
(`/metrics`, `/health/live`, `/health/ready`) separate from business endpoints in all three — they have a
different audience, a different failure mode, and they must stay out of the `http.server.requests` timer.

### 5.1 wolfram — Tapir endpoint values on Vert.x

Endpoints are **values**, so the same description drives the Vert.x routes, the OpenAPI document and the tests.

1. **Models** — add request/response case classes to `ApiModel.scala`. Circe codecs plus Tapir's
   `generic.auto` schema derivation cover them.
2. **Description** — add a `val` to `object Endpoints` in `Endpoints.scala`:

   ```scala
   val describeEvent: PublicEndpoint[String, IngestFailure, EventSummary, Any] =
     endpoint.get
       .in("events" / path[String]("id"))
       .out(jsonBody[EventSummary])
       .errorOut(failures)          // the shared oneOf — variants match on class, so status cannot drift
       .name("describeEvent")
       .summary("…")
       .description("…")
   ```

3. **Register it** — add it to `Endpoints.all` (that is what `OpenApi` documents), and to
   `Endpoints.requestMediaTypes` if it takes a body.
4. **Logic** — write it as a pure function on `IngestionService` (or a new service class), then bind it in
   `IngestApi.routes` with `Endpoints.describeEvent.serverLogic(…)`. Descriptions and logic stay separate so the
   same endpoints could drive a client or a spec.
5. **Tests** — `EndpointsSuite` asserts on the description, `OpenApiSuite` on the generated document, and the
   logic is tested directly without a server.
6. **Operational routes are not Tapir.** They are plain Vert.x routes in `AdminRoutes.mount`, mounted outside the
   interpreter so `HttpMetrics` structurally cannot see them and no exclusion list can fall out of date.

### 5.2 cobalt — Cask annotations, one line each

**cobalt has no business endpoints and never will.** Events arrive over Kafka; an HTTP write path would be a
second, unordered, uncommitted way into the same database. New routes here are operational only.

1. Add a pure method returning `AdminReply` to `AdminHandlers` in `AdminRoutes.scala` — every decision lives
   here, because Cask has no test kit and the alternative is binding a port in a unit test.
2. Add a one-line delegation to `CobaltRoutes`:

   ```scala
   @cask.get("/health/startup")
   def startup(): cask.Response[String] = CobaltRoutes.respond(handlers.startup())
   ```

   The path must be a string literal (Cask's annotations are macros and want a constant).
3. Assert the literal against the corresponding constant in `modules/observability`'s `Meters` in
   `AdminRoutesSuite` — that is what stops this file drifting from the shared vocabulary and quietly handing
   Prometheus a 404.

### 5.3 ferrite — SIRD routers, no `conf/routes`

There is **no routes file** and there must not be: `Compile / routes / sources := Nil`, and routing is
`AppRouter`, selected by `play.http.router` in `application.conf`.

1. **URL** — add the path to `io.kzonix.ferrite.web.Urls` (and a builder function if it takes parameters). This
   is the single source of the string.
2. **Pattern** — add a `PathExtractor` to `object Paths` in `routing/Routers.scala`, built from the `Urls`
   constant via `PathExtractor.cached`, so the route and the URL builder are the same string *by construction*:

   ```scala
   val Device: PathExtractor = PathExtractor.cached(Seq(s"${Urls.Devices}/", ""))
   ```

   An empty trailing part matches one decoded segment; a part beginning with `*` matches greedily (asset paths).
3. **Controller** — add the action to `EventsController` (UI) or `OpsController` (operational), returning a
   `Result`. Blocking JDBC work goes through `SearchService` on the `SearchExecutionContext`, never on Play's
   default dispatcher.
4. **Route** — add a `case` to `WebRouter.routes` or `OpsRouter.routes`. `AppRouter` composes
   web → assets → ops with `orElse`; ops comes last because its paths are exact and cannot collide.
5. **View** — Twirl templates live in `src/main/twirl/views/…`. A page and its htmx fragment are separate
   templates; `Hx.isFragment(request)` decides which to render, and a fragment response should set
   `Hx.PushUrlHeader` when it changes what the URL should say.
6. **Wiring** — a new injectable type only needs a line in `FerriteModule.bindings` if Play cannot construct it
   (abstract, or with a shutdown obligation). Constructor injection everywhere; no global state.
7. **Tests** — `UrlRoutingSuite` proves each builder's output is matched by its extractor;
   `EventsControllerSuite` constructs the controller directly. Play's test helpers stream the result, so a suite
   calling `Helpers.call` needs a `Materializer` in scope and `Helpers.writeableOf_AnyContentAsEmpty`.
8. CSRF is **enabled** (the filter bar is a browser form); the token reaches every htmx request through one
   `hx-headers` attribute on `<body>`. The CSP allows `script-src 'unsafe-eval'` only because Alpine 3 compiles
   its `x-` expressions with `new Function` — keep Alpine to presentational state.

---

## 6. Adding a database migration

Migrations live in `modules/persistence/src/main/resources/db/migration` and ship inside the module's jar, so the
schema and the code that queries it version together. **cobalt applies them on boot** — it is the write side, so
it must not start against a schema older than its own inserts. ferrite never migrates.

1. **Add a new file. Never edit an applied one.** `V2__add_alerts_view.sql`, sequential version, snake-case
   description. `validateOnMigrate` is on, so editing `V1__events.sql` after it shipped turns every subsequent
   boot into a failure — which is the point. `outOfOrder` is off and `baselineOnMigrate` is off.
2. **Follow the DDL conventions of `V1__events.sql`**, which are asserted by tests:
   - Partition bounds carry an explicit `+00` offset. A bare date is parsed in the session timezone and silently
     shifts every partition. Run Flyway with `-Duser.timezone=UTC`.
   - Every index gets a `COMMENT ON INDEX events.<name> IS '…'` naming the query shape it serves.
     `MigrationScriptSuite` fails without it — an index nobody can name is an index nobody can justify deleting.
   - Extraction functions used by generated columns must be `IMMUTABLE`, `PARALLEL SAFE` and non-throwing.
     A bare cast in a generated column aborts the entire 500-event batch on one malformed payload; return `NULL`
     instead so a bad payload is a missing dimension rather than an outage.
   - Storage parameters go on leaf partitions, never on the partitioned parent (Postgres rejects them there), and
     they are not inherited by new partitions.
   - Anything mirroring Scala — `events.severity_rank` vs `io.kzonix.kernel.search.Severity`, the reserved
     attribute list vs `Envelope.ReservedAttributes` — must stay identical, spellings and aliases included.
     `MigrationScriptSuite` checks the reserved list; keep new mirrors equally checked.
3. **Update the tests that pin the schema:**
   - `MigrationScriptSuite` (unit, no database) — text-level assertions about the script.
   - `MigrationIT` (integration, real Postgres) — `assertEquals(Migrations.appliedVersions(…), Vector("1"))` is a
     committed baseline. Add your version to it. That assertion is what catches a migration edited in place.
4. **Run both tiers:** `sbt verify` then `sbt verifyIt` (Docker required). The integration tier is where a
   generated column that Postgres refuses as non-`IMMUTABLE` actually fails — nothing about that is visible at
   compile time.
5. **Live tables need care.** Indexes on a partitioned parent cannot be built `CONCURRENTLY` and take
   `ACCESS EXCLUSIVE` on the whole hierarchy; see `docs/operations.md` §7.4 for the three-step alternative.
6. Partition creation deliberately does **not** belong in a migration: migrations are versioned and immutable,
   partitions are a rolling concern. See `docs/operations.md` §7.3.

---

## 7. Writing tests

- **munit leads**; scalatest is kept for Play's test helpers. scalacheck for properties — the wire and filter
  generators (`WireGenerators`, `FilterGenerators`, `Generators`) already exist, reuse them.
- **Unit tests must not need Docker.** Testcontainers is declared `% IT` only, exactly so the fast tier never
  needs a daemon.
- **Test the pure function, not the socket.** Every service splits its decisions out of its framework surface for
  this reason: `AdminHandlers` (cobalt), the logic functions behind `Endpoints` (wolfram), `Presenter`/`SearchQuery`
  (ferrite). New logic goes there, not inline in an annotated method or an `Action` body.
- `src/it/resources/logback-test.xml` sets `org.testcontainers`, `com.github.dockerjava` and `tc-java` to WARN.
  Without it, docker-java's wire logger buries every integration-test result.
- **Every IT suite provisions its own dependencies.** One shared lazy Testcontainers singleton per forked JVM,
  in a companion object rather than a field — munit builds a fresh suite instance per test, so a `lazy val` on the
  suite would start one container per *test*. Ryuk reaps them at JVM exit, so there is no teardown hook to forget.
  `PostgresSuite`, `KafkaWireIT`, `CobaltIT`, `WolframIngestIT` and `EventsPageIT` each own one.
- **A suite skips only when Docker is unreachable**, via `munitIgnore`. A red suite on a laptop with no Docker
  teaches people to ignore red suites. What this is *not* is the old behaviour: these suites used to skip on the
  absence of an environment variable nobody set, and `WolframIngestIT` did not even skip — it returned unit and
  reported three passing tests that had contacted no broker. If you add a fixture, make the no-dependency case
  loudly skipped, never quietly green.
- Environment variables the IT tier reads: `IT_POSTGRES_URL`, `IT_POSTGRES_USER`, `IT_POSTGRES_PASSWORD` and
  `KAFKA_BOOTSTRAP_SERVERS`. These **override** the containers rather than gating the suites — that is how CI
  points every module at one database and one broker instead of starting five of each, and it is why the fixtures
  `TRUNCATE` before they seed. Note that `deploy/docker-compose.yml` publishes neither Postgres nor Kafka to the
  host, so pointing the suites at a compose stack needs a temporary port mapping first:

  ```bash
  IT_POSTGRES_URL=jdbc:postgresql://localhost:5432/observatory \
  IT_POSTGRES_USER=observatory IT_POSTGRES_PASSWORD=… \
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 sbt verifyIt
  ```

---

## 8. Changing ferrite's stylesheet

`applications/ferrite/src/main/resources/public/css/app.css` is **committed CLI output**, not a hand-written file.
It is what the standalone Tailwind CSS CLI v4.3.3 produces from
`applications/ferrite/src/main/assets/css/app.css`, which imports Tailwind, imports the hand-written
`components.css`, declares the theme tokens, and declares the class scan with `@source`. That arrangement is
ADR §8.3's, and its whole purpose is that the runtime image never contains Node or Tailwind.

The cost of committing generated output is that it goes stale silently: a template gains `class="mt-4"`, the CSS
does not contain `.mt-4`, and the page renders without it. Nothing in a compile, a test or a page load says so.

```bash
sbt ferrite/tailwind        # regenerate the committed stylesheet in place
sbt ferrite/tailwindCheck   # fail if it differs from what the templates imply
```

**Run `ferrite/tailwind` in the same change as any template edit that touches a class.** Then `sbt verify` as
usual — the regenerated file is a source file like any other.

### Getting the binary

The CLI is a 112 MB self-contained binary from GitHub releases, **not** from Maven (`org.webjars.npm:tailwindcss`
ships the `@tailwindcss/oxide` native compiler and needs Node). `Tailwind.resolve` in `project/Tailwind.scala`
looks for it in this order — `TAILWIND_BIN`, then `~/.cache/kzonix/tailwindcss-<platform>-4.3.3`, then
`tailwindcss` on `PATH` — and if none is there, **it warns with the exact `curl` command and changes nothing.**

```bash
mkdir -p ~/.cache/kzonix
curl -fsSL -o ~/.cache/kzonix/tailwindcss-linux-x64-4.3.3 \
  https://github.com/tailwindlabs/tailwindcss/releases/download/v4.3.3/tailwindcss-linux-x64
chmod +x ~/.cache/kzonix/tailwindcss-linux-x64-4.3.3
```

`PATH` is searched last on purpose: a globally installed `tailwindcss` is very often a different major version,
and the pinned copy in the cache is the one that reproduces the committed file byte for byte. On a musl host
(Alpine) take the `-musl` asset from the same release and point `TAILWIND_BIN` at it; the glibc build will not
run there. In CI, cache `~/.cache/kzonix` beside `~/.cache/coursier`.

### Four decisions in that task worth not re-litigating

- **It is not a `Compile / resourceGenerators` entry**, which is what ADR §8.3 sketched. A generator must produce
  a file on every build, so on a machine without the CLI it would have to invent one — an empty file, an
  un-compiled copy of `components.css`, or a build failure. All three are worse than the checked-in file, which
  is already correct. An explicit task can do the one right thing: change nothing, and say why.
- **Absence is a warning, never an error, and never a rewrite.** The committed CSS is only ever replaced by bytes
  a successful, non-empty CLI run produced into a scratch file first — a CLI that died half-way through writing
  its `--output` would otherwise leave a truncated stylesheet behind.
- **`tailwindCheck` is not in `verify`.** `verify` must stay runnable with nothing but a JDK. Without the CLI the
  check warns and passes, exactly like `tailwind` does. That is the one gap left, and it is recorded as such in
  `docs/operations.md` §8.
- **Both tasks are wrapped in `Def.uncached`.** sbt 2 caches task results by declared inputs, and neither task
  declares the Twirl templates as one. Without it the first invocation is replayed for every later one and a
  template that gained a class reports "up to date" forever — the exact failure the tasks exist to catch. This
  was observed, not anticipated.

---

## 9. Gotchas worth remembering

- `sbt verify` is green at 531 unit tests. If your change makes the count *drop*, you probably renamed a suite
  into invisibility.
- OpenTelemetry's `Context` is `ThreadLocal`-backed and `Context.current()` returns root **silently** on any
  thread it was not entered on. Across a `Future`, a Pekko stream stage or a Vert.x event-loop hop, capture the
  `Context` and pass it explicitly. An orphaned span is not an error anything reports.
- Never add a `-javaagent:opentelemetry-javaagent.jar` "for free tracing": it duplicates every Kafka span and
  introduces a competing HTTP metric family.
- Micrometer happily registers the same meter name twice with different tag sets, and Prometheus renders that as
  two unrelated series. That is why each service has a typed metrics façade (`IngestMetrics`, `ConsumerMetrics`)
  where a meter's tag set is fixed in exactly one place. Add meters there, and add names to `Meters`.
- `for` over `Future` is *sequential*. `SearchService` starts its four queries as `val`s before awaiting any of
  them for exactly this reason; a `for` comprehension there turns one 40 ms page into four serial round trips.
- Scala 3 has known `-Wunused` false positives for givens resolved inside the `sql` macro. Keep given-imports at
  the narrowest scope; a targeted `@nowarn` is permitted **only** with a comment naming the false positive.
- Don't commit. The orchestrator commits.

---

## See also

- `docs/adr/0000-architecture.md` — the contract: dependency table, schema DDL, index rationale, risks.
- `docs/operations.md` — deployment, environment variables, runbooks, backup and retention.
