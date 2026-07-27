# ferrite — the web application

Play 3 with SIRD routing, server-rendered Twirl templates, htmx and Alpine over PostgreSQL. Source:
`applications/ferrite/`.

---

## What it is responsible for

- Serving the event observatory UI: a filtered, faceted, keyset-paginated list of events, a histogram over the
  matching window, and a detail view showing the decoded observation beside the raw CloudEvent.
- Turning a query string into a `kernel` `Filter` **totally** — a malformed permalink is rendered back into the
  filter bar with each bad value still in the input that produced it, never a 500 and never a silently dropped
  parameter.
- Producing every URL the application can emit (`Urls`), so links and routes are the same strings by construction.
- Serving its own Prometheus exposition and the two health probes.

## What it is explicitly NOT

- **Not a writer.** ferrite reads only. Both transactors in `EventRepositoryProvider` point at the *read* pool,
  whose sessions are `read-only = true` with `SET statement_timeout = '2s'`. `EventRepository.insertAll` is
  cobalt's half of the shared interface; wiring the write pool in here would give this service a capability
  nothing needs.
- **Not a migrator.** ADR §1 assigns the Flyway migrations to this service's *repository module*, but nothing in
  `applications/ferrite` calls `Migrations`. Three replicas starting together would race on the schema-history
  table, and a migration that fails halfway leaves a replica serving traffic against a schema it cannot describe.
  Migration is a deployment step; here it is cobalt's boot.
- **Not a Kafka client.** There is no Kafka on this classpath at all (`build.sbt`: ferrite depends on `kernel`,
  `persistence`, `observability` — not `eventing`).
- **Not a JSON API.** Every response is HTML: a full document or a fragment of one. There is no
  `Accept: application/json` path.
- **Not a client-side application.** Alpine is confined to presentational state and never fetches or renders user
  data; all rendering is server-side Twirl.
- **Not routed by `conf/routes`.** `Compile/routes/sources := Nil` and no routes file exist, so no
  `router.Routes` is generated and there is no reverse router.

---

## Public surface

| Route | Handler | Notes |
| --- | --- | --- |
| `GET /` | `EventsController.index` | `302` to `/events`. A redirect rather than a second copy of the list, so there is exactly one canonical URL for "all recent events" to share and cache. |
| `GET /events?…` | `EventsController.list` | The list. Full page or fragment — see below. |
| `GET /events/{eventUid}?at={rfc3339}` | `EventsController.detail` | One event. |
| `GET /assets/*file?v={version}` | `AssetsRouter` | Static assets from the classpath `/public`, cache-busted by the build version (from the jar manifest, else `SERVICE_VERSION`, else `dev`). |
| `GET /metrics` | `OpsController.metrics` | Prometheus text exposition. |
| `GET /health/live` | `OpsController.live` | Always `200 {"status":"UP"}`, `Cache-Control: no-store`. |
| `GET /health/ready` | `OpsController.ready` | `200`/`503 {"status","detail"}`. |

Composed as `web.routes.orElse(assets.routes).orElse(ops.routes)` in `AppRouter`, selected by
`play.http.router`. The three routers are separate because they have different audiences and different failure
modes; `AssetsRouter` is isolated because constructing `controllers.Assets` by hand needs an `HttpErrorHandler`,
an `AssetsMetadata` and a `FileMimeTypes`, so a test that only wanted to check a UI route would otherwise have to
assemble the whole asset pipeline first.

The path patterns are built from `Urls` via `PathExtractor.cached` rather than written as SIRD `p"…"`
interpolations, because the interpolator is a pattern-position construct whose literal parts have to be typed into
the pattern — putting `"/events"` in both the URL builder and the route, where a mismatch is invisible until
someone clicks a link. `UrlRoutingSuite` then only has to prove each builder's output is matched.

### Query parameters on `/events`

Split in `SearchQuery.parse`: three **control** parameters are consumed here, everything else is handed verbatim
to `FilterQuery` in `modules/kernel`.

| Parameter | Owner | Notes |
| --- | --- | --- |
| `limit` | ferrite | 1 … `SearchRequest.MaxLimit`; default `SearchRequest.DefaultLimit`. |
| `sort` | ferrite | `newest` (default) or `oldest`. |
| `cursor` | ferrite | Opaque keyset cursor. Rejected if minted for a different filter. |
| `v` | kernel | Grammar version. Required by `FilterQuery`… |
| `q`, `type`, `source`, `device`, `room`, `person`, `tag`, `severity`, `data`, `from`, `until` | kernel | The filter grammar. Repeats are meaningful (a facet is a multi-value selection). |

…with one carve-out: **an empty filter half means no filter, not an error.** `FilterQuery` demands an explicit
`v=1` precisely so a future grammar change is detectable, but the landing page `/events` legitimately carries no
filter and must not be greeted with "missing 'v' parameter". The filter bar renders `v=1` as a hidden input, so
every query the UI itself produces is versioned, and only a truly empty query takes the shortcut.

Parsing evaluates **every** part and reports **every** failure. Short-circuiting would show one broken parameter,
the user would fix it, and the next reload would show the next one.

Two invariants `SearchQuery.link` enforces so no call site has to remember them: the version is re-stamped on any
non-empty result, so a link the UI produced always round-trips; and **the cursor is never carried over** — a
cursor is a position inside a result set, not part of the query, so editing a filter always restarts at page one.
Default control values are omitted from generated links, keeping shared URLs short. `continuation` appends the
cursor last, so a hand-truncated URL degrades to page one rather than to a different filter.

### Why `at` is a query parameter and not a path segment

The detail route is `/events/{event_uid}?at={rfc3339}`, not `/events/{at}/{uid}`. `occurred_at` is half of the
primary key of a RANGE-partitioned table and must reach the repository verbatim; RFC 3339 is full of characters
(`:`, `+`) that a path segment has to percent-encode and that every intermediary is then free to re-normalise. As
a query parameter it survives proxies, `curl`, and a user editing the link. A missing or unparseable `at` is a
**400**, not a "search harder" — a lookup without it would have to scan every partition.

### Database

Reads `events.cloud_event` for the page, the facets (computed from a candidate CTE over the fact table), the
histogram (`generate_series` left-joined to bucket counts) and the detail row. `events.event_rollup_hourly` and
the `events.dim_*` / `events.device` autocomplete tables exist in the schema; the current repository queries the
fact table directly.

---

## HX-Request fragment selection

One URL, two representations (`Hx`, `EventsController`). `/events?…` answers a browser navigation with the whole
document and an htmx swap with just the region that changed. **The fragment templates are the same templates the
page wraps** — `views/pages/*` do nothing but put `views/fragments/*` inside the layout — so there is no second
copy of the table to keep in sync. That single decision is what makes htmx pay for itself here rather than
doubling the markup.

Two consequences fall out, and both are load-bearing:

- **Links stay shareable.** A permalink pasted into a new tab is an ordinary request and gets the whole page. The
  back button, `hx-boost` and crawlers all work with no special-casing.
- **`Vary: HX-Request` is mandatory.** Two representations of one URL differing on a request header is exactly
  what `Vary` is for; without it a shared proxy will serve a naked `<tbody>` to someone who typed the URL.
  `varyOnHx` is applied to **every** response from these routes, including the error ones.

**The negative half of `isFragment` matters as much as the positive half.** htmx re-fetches the full page when its
history cache misses, and marks that request `HX-History-Restore-Request: true` *while also* sending
`HX-Request: true`. Answering it with a fragment replaces the entire document with a `<tbody>` and poisons every
subsequent back-navigation — a bug that only appears after a user navigates away and returns, which is to say
never during development. So `isFragment` requires `HX-Request: true` **and not** `HX-History-Restore-Request`.

Which fragment is decided by the request, not by a separate endpoint:

| Request | Response |
| --- | --- |
| Not a fragment | `views.html.pages.events` — the whole document |
| Fragment **with** a cursor (paging) | `views.html.fragments.rows` — rows plus a fresh "load more" sentinel. No facets, no histogram, no count: the filter did not change, so neither did they |
| Fragment **without** a cursor (filter change) | `views.html.fragments.results` — the whole results region, plus an `HX-Push-Url` header carrying the canonical permalink so the address bar stays honest and the URL a user copies is the search they are looking at |
| Detail, either way | `views.html.fragments.detail` or `views.html.pages.detail` |
| Failure, either way | `views.html.fragments.failure` or `views.html.pages.failure` |

Giving paging its own path would have been simpler here and worse everywhere else: the "load more" link would then
not be a shareable position in the same search.

`SearchService.page` mirrors the split on the data side — a continuation runs the page query only, because
re-running facets, histogram and count on every scroll would triple the cost of paging for markup that is thrown
away.

Error responses come from a `Failure` value that carries **both** the status and the rendered body
(`Presenter.badQuery` → 400, `Presenter.rejected` → 400, `Presenter.notFound` → 404), so a 400 can never be served
with a body that reads like a 404. A rejected permalink additionally comes back *inside the filter bar*
(`SearchQuery.lenient` preserves the raw pairs) — an error page without the bar would leave the user holding a URL
they can neither see nor edit.

---

## The bounded search dispatcher

Every blocking JDBC call runs on `SearchExecutionContext`, a Play `CustomExecutionContext` bound to the
`database.search-dispatcher` block in `application.conf`:

```hocon
database.search-dispatcher {
  type = Dispatcher
  executor = "thread-pool-executor"
  thread-pool-executor { fixed-pool-size = 8 }
  throughput = 1
}
```

**Not Play's default dispatcher.** That is a fork-join pool sized to the cores and shared with request parsing,
the Pekko HTTP server's own work and every non-blocking `map` in the application. A blocking
`getConnection`/`executeQuery` parked on it does not merely occupy a thread, it removes a thread from the pool
that is supposed to be *accepting* requests — so a slow database stops the server answering `/health`, and the
symptom is a failing liveness probe rather than a slow search. Isolating the blocking work means a database stall
degrades exactly one feature.

**Not a virtual-thread executor** (ADR §0, decision 8). An unbounded executor over virtual threads does not remove
the queue, it relocates it from HikariCP — where it has a visible depth, a `connectionTimeout`, and therefore a
load-shedding point — into an invisible one with neither. JEP 491 makes virtual threads *safe* for blocking JDBC
on JDK 25; it does not make them *preferable* where the bound is the resource being protected.

**`fixed-pool-size` must equal `database.read.maximum-pool-size`, and both are 8.** More threads than connections
means the surplus block inside `getConnection` and the queue silently moves from the dispatcher (configured and
observable) to the pool (where waiting surfaces as a `connectionTimeout` *exception* rather than as backpressure).
Fewer threads means connections that can never be used. There is exactly one such dispatcher, because ferrite
never writes.

`throughput = 1`: one task per hand-off. These tasks block for milliseconds; a larger throughput only delays the
first of them.

**All four queries of a search are issued before any is awaited.** `SearchService.search` starts `page`, `facets`,
`histogram` and `countAtMost` as `val`s and only then sequences them in a `for`. Writing the `for` over the calls
directly would sequence them and turn one 40 ms page into four serial round trips — not a style choice, but the
classic way to make a fast page slow without anyone noticing.

Two bounded-answer decisions travel with it: the total is `countAtMost(filter, 10000)`, never `COUNT(*)`, and the
UI renders "10,000+" at the cap, because an exact total over a partitioned fact table is a full scan nobody reads
the end of; and a filter with no time bounds gets a default 24-hour histogram window rather than "all of history",
which would produce month-wide buckets in which nothing is visible and would prune no partitions.

---

## Configuration

Play's reference configuration and `modules/persistence`'s `reference.conf` supply the bulk;
`applications/ferrite/src/main/resources/application.conf` holds only overrides.

| Env var | Key | Default | Mandatory? |
| --- | --- | --- | --- |
| `APPLICATION_SECRET` | `play.http.secret.key` | `development-only-secret-do-not-use-in-production` | **Yes in every non-development environment** — the checked-in default is a placeholder, and leaving it in place means session cookies and CSRF tokens are signed with a publicly known key. The reference compose treats it as required (`${APPLICATION_SECRET:?…}`). |
| `ALLOWED_HOSTS` | `play.filters.hosts.allowed` | `["localhost", "127.0.0.1", ".local"]` | **Yes** wherever the service is browsed to under any other hostname — the allowed-hosts filter rejects everything else. |
| `DATABASE_URL` | `database.jdbc-url` | `jdbc:postgresql://localhost:5432/observatory` | Effectively yes. |
| `DATABASE_USER` | `database.username` | `observatory` | |
| `DATABASE_PASSWORD` | `database.password` | `""` | Wherever the server requires one. |
| `PLAY_HTTP_PORT` | — | `9000` | Read by the native-packager start script, not by `application.conf`. |
| `SERVICE_VERSION` | — | jar manifest, else `dev` | The `version` common tag, and the asset cache-busting `?v=`. |
| `HOSTNAME` | — | local host name | The `instance` common tag. |
| `OTEL_*` | — | SDK autoconfiguration | Traces only. There is deliberately no "tracing enabled" flag whose branches could drift: disabling is `OTEL_SDK_DISABLED=true`, the same switch the other two services use. |
| `LOG_LEVEL` | — | `INFO` | Root level of the JSON Logback config. |

Fixed in `application.conf`, no env var:

- `play.http.router = io.kzonix.ferrite.routing.AppRouter` — SIRD, not a compiled routes file.
- `play.modules.enabled += io.kzonix.ferrite.wiring.FerriteModule`.
- `play.filters.enabled += io.kzonix.ferrite.web.MetricsFilter`.
- **CSRF is enabled.** The `play.filters.disabled += CSRFFilter` line was *removed* rather than commented out — a
  disabled security filter with a comment explaining why is a filter that stays disabled. The token reaches every
  htmx request through one `hx-headers` attribute on `<body>`: htmx inherits it down the DOM, so every descendant
  request carries it, and unlike a hidden input it survives an `innerHTML` swap of the region the form lives in.
  `Csrf.token` returns `""` rather than throwing when no token is installed, because `views.html.helper.CSRF`
  throws and would turn a configuration mistake into a 500 on every page.
- A same-origin `Content-Security-Policy`. `'unsafe-eval'` is present because Alpine 3 compiles its `x-`
  expressions with `new Function`; that is the documented trade, and it is exactly why Alpine is confined to
  presentational state here.
- `database.read.read-only = true` and the `search-dispatcher` block above.

Boot-time bindings (`FerriteModule`): `Clock` is `systemUTC` — **not** the system default, because every timestamp
is a `timestamptz` rendered as RFC 3339 and a server in another zone would render the same instant differently
from its neighbour; `Databases` is **eager**, so an unreachable database fails the boot rather than the first user
request; `Telemetry` is a provider with a stop hook.

`Databases` reads its configuration through the running application's `Configuration`, not `ConfigSource.default`,
so a test or integration run can override `database.jdbc-url` with `GuiceApplicationBuilder.configure(...)` and
have it honoured.

---

## Failure modes and what it does about them

| Failure | Response |
| --- | --- |
| Malformed query string / invalid filter value | `400`, the filter bar re-rendered with every offending value still in its input and one `Problem` per error. Never a silent drop: a user who cannot see that their filter was ignored will trust the wrong numbers |
| Cursor malformed, or minted for a different filter | `400` from `SearchRequest.of`, rendered as a `Failure` with a link back to the unpaged search |
| `limit` out of range | `400` naming the bound |
| Event id not a UUID, or `at` missing/unparseable | `400` |
| Event not found | `404`, with a way back to the search the user came from (`backUrl` = this request's query string minus `at`) |
| Facet or histogram sub-query cannot be built | The page still renders: empty facets and an empty histogram rather than an error, so the page does not change shape depending on whether a sub-query succeeded |
| Database unreachable at boot | Boot fails — Hikari's default `initializationFailTimeout` is kept deliberately. A ferrite that starts without a database reports itself healthy and serves errors |
| Database unreachable later | Readiness → `503`; liveness unaffected |
| Readiness check throws | Reported as "not ready" with the message as `detail`, never a 500 — a probe endpoint that can fail with a stack trace is one an orchestrator cannot interpret |
| Statement runs long | Killed by the read pool's session default `SET statement_timeout = '2s'` |
| Shutdown | One `ApplicationLifecycle` stop hook closes the pools. Pools hold server-side sessions, and a restart loop that leaks them exhausts `max_connections` for every *other* service too |

---

## Metrics and health semantics

Common tags `service=ferrite`, `version`, `instance`.

| Meter | Type | Tags | Notes |
| --- | --- | --- | --- |
| `http.server.requests` | timer | `method`, `uri`, `status`, `outcome` | Recorded by `MetricsFilter` and by nothing else. |

Plus the JVM/system binders registered by `Telemetry`.

**Exactly one component times each request, and it is always Micrometer** (ADR §7.1) — that is what makes
`http.server.requests` carry identical names and tags in all three services so one Grafana dashboard works
everywhere. A Play-native or framework-supplied timer would fork the family for this service alone.

**`uri` is a route template, never a raw path.** ferrite serves server-rendered search over millions of events;
one timeseries per permalink is, in the ADR's words, the single most likely way to take down Prometheus in this
system. Because there is no `conf/routes`, Play attaches no `HandlerDef` and there is nothing to read a matched
route off, so `RouteTemplate.of` derives it from a fixed list: `/`, `/events`, `/events/{eventUid}`,
`/assets/*file`, else `other`. That is *stronger* than reading a template off a router — a new route nobody adds a
case for degrades to `other` instead of leaking a raw path. `Telemetry`'s `maximumAllowableTags` cap on this exact
tag is the backstop.

`/metrics` and the whole `/health` subtree are excluded (`Meters.UninstrumentedPaths`). A scrape endpoint that
times itself adds a request per scrape interval to every rate panel that reads it — a self-fulfilling traffic
graph.

**Liveness consults nothing.** A liveness probe failing because PostgreSQL is unreachable gets the container
killed, the replacement fails the same probe, and a recoverable database outage becomes a restart loop that also
discards every warmed connection pool in the fleet. Liveness answers one question: is this JVM still able to serve
an HTTP request.

**Readiness consults the database**, because a replica that cannot reach PostgreSQL has nothing to serve and
should leave the load-balancer rotation rather than return 500s. A failed readiness removes traffic; it does not
remove the process. The check is `Connection.isValid(1)` from the read pool, run **on the search dispatcher**
because `getConnection` blocks and a probe that parks a request thread takes the service down while reporting on
it. One second is deliberately shorter than the pool's `connectionTimeout`, so a saturated pool answers "not
ready" instead of hanging until the orchestrator's own timeout fires and reports something less specific.
`Connection.isValid` and not `SELECT 1`: a protocol-level check with a timeout the pool cannot swallow, consuming
no statement slot on a pool whose whole budget is a two-second `statement_timeout`.

Both probes send `Cache-Control: no-store`; `/metrics` is never cached, because a cached scrape is a lie about the
moment it describes.

---

## Running it locally

ferrite does not create its schema. Start PostgreSQL and let cobalt migrate, or run the migrations yourself.

```bash
docker compose -f deploy/docker-compose.yml up -d postgres
# then start cobalt once (it owns the migrations) — see docs/services/cobalt.md

DATABASE_URL=jdbc:postgresql://localhost:5432/observatory \
DATABASE_USER=observatory \
DATABASE_PASSWORD=... \
sbt ferrite/run                 # dev mode on :9000
```

Then:

```bash
open http://localhost:9000/events

# the fragment representation of the same URL
curl -s -H 'HX-Request: true' 'http://localhost:9000/events?v=1&severity=error' | head
curl -sI 'http://localhost:9000/events' | grep -i vary   # Vary: HX-Request

curl -s localhost:9000/health/ready
curl -s localhost:9000/metrics | grep http_server_requests
```

The Tailwind stylesheet is checked in as a build resource under `src/main/resources/public/css`, so **the runtime
image never needs Node**; htmx and Alpine are WebJars unpacked by sbt-web onto the same classpath prefix.

Full stack: `sbt ferrite/Docker/publishLocal` then `docker compose -f deploy/docker-compose.yml up -d`; ferrite is
published on host port **9000**. `APPLICATION_SECRET` and `ALLOWED_HOSTS` must be set there.

Tests: `sbt ferrite/test` needs no database — routing, URL building, query parsing and the presenters are all
tested as values, and jsoup makes the structural HTML assertions. `sbt "ferrite/IT/testFull"` runs `EventsPageIT`
against a Testcontainers PostgreSQL.
