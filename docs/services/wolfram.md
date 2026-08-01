# wolfram — the CloudEvents ingestion API

Tapir endpoint descriptions served by a Vert.x 5 HTTP server, publishing validated CloudEvents to Kafka with a
plain `KafkaProducer`. Source: `applications/wolfram/`.

---

## What it is responsible for

- Accepting CloudEvents 1.0 over HTTP in **both** content modes (binary and structured) on one resource, plus
  `application/cloudevents-batch+json` documents on a second path.
- Deciding whether an event is publishable *before* it becomes durable: size, spec conformance, and a
  plausibility window on `time`.
- Publishing the accepted event to `events.cloudevents.v1` in binary content mode, keyed by
  `Envelope.partitionKey`, and returning the created resource — including a `destination` naming the `topic`,
  `partition` and `offset` the broker wrote it to.
- Verifying a JWT bearer token on every `/v1` operation, before anything else looks at the body.
- Injecting the W3C `traceparent` into the Kafka record headers so the trace that started at this HTTP ingress
  continues into cobalt.
- Serving its own Prometheus exposition, two health probes, a generated OpenAPI document and the Swagger UI that
  renders it.

## What it is explicitly NOT

- **Not a store.** It owns no database, no Flyway migration and no state beyond the producer's accumulator and its
  belief about broker reachability. `applications/wolfram` has no `modules/persistence` dependency.
- **Not an assigner of identity.** It never mints `id`, `source`, `time` or a partition key of its own.
  CloudEvents makes `(source, id)` the *producer's* contract, and the whole downstream deduplication story
  (`cloud_event_identity_uk`) depends on this service not touching it. `partitionKey` is read off
  `Envelope.partitionKey` in `modules/kernel`, never recomputed locally (`IngestionService`).
- **Not a repairer of bad input.** Every threshold in `IngestConfig` is a rejection threshold; nothing is clamped
  into range and no default is invented (ADR §4.3, "reject; never invent defaults").
- **Not a Pekko service.** Every produce belongs to exactly one in-flight HTTP request, so there is no stream and
  no materializer; Pekko is off this classpath entirely (ADR §1, `KafkaEventPublisher`).
- **Not a query API.** There is no read path: no `GET /v1/events`, no `GET /v1/events/{event}`, no replay
  endpoint. Reading is ferrite's job. `events:validate` is the one operation that answers without publishing, and
  it reads nothing — it re-runs the checks against the body in the request.
- **Not a batch transaction.** A batch is a convenience for the producer, not an atomic unit — see the 207 below.

---

## Public surface

The surface follows Google's [API Improvement Proposals](https://google.aip.dev), because they answer the
questions a hand-rolled HTTP API answers differently every time — and answer them the way most tooling already
expects. Concretely:

| AIP | What it means here |
| --- | --- |
| 133 / 135 | The resource is `events`. Creating one is `POST /v1/events`, and the response **is the created resource**, not a bespoke receipt. |
| 122 | Every response carries `name: "events/{event}"`. `{event}` is the percent-encoded CloudEvents `(source, id)` pair, because that pair and not `id` alone is what the spec makes unique. |
| 136 | Operations that are not standard methods are **custom methods** and use a **colon**: `POST /v1/events:batchCreate`, `POST /v1/events:validate`. |
| 185 | The `/v1` prefix is on the path, so a proxy can route on it and a browser can be pointed at it. |
| 193 | Every failure is one `{"error": {code, message, status, details}}` envelope. |

**The colon in a custom method is load-bearing, not decoration.** `POST /v1/events/batch` is indistinguishable
from acting on a resource whose id is `batch`, and the day a `GET /v1/events/{event}` is added those two routes
collide. `:batchCreate` can never collide with a resource id, because `:` is reserved out of the identifier
segment.

**Everything under `/v1` requires a bearer token.** See [Authentication](#authentication).

### `POST /v1/events` — Create

One CloudEvent, in either HTTP content mode. Any `Content-Type` is accepted, because the header is an input to the
mode decision and not a routing key; the generated document declares the single media type `byteArrayBody`
implies, `application/octet-stream`.

**Mode is chosen by the presence of the `ce-specversion` header, never by `Content-Type` alone**
(`HttpBinding.modeOf`). This is load-bearing: in binary mode `Content-Type` describes the *payload*, and a payload
may itself legitimately be a CloudEvents document (a forwarder, a replay tool, an event quoting another event).
Deciding on the media type first would misread exactly those requests as structured and discard every attribute in
the headers. A request that declares neither is a 400 rather than a guess.

The body is taken as `byteArrayBody` — not `stringBody`, not `jsonBody`. Binary mode's payload is arbitrary bytes
and must reach Kafka byte-identical; decoding as `String` would corrupt non-UTF-8 payloads, and decoding as JSON
would reject the very events binary mode exists to carry. In binary mode, a body whose declared type is JSON (by
the RFC 6839 `+json` suffix rule) is spliced into the `data` slot as JSON; anything else goes to `data_base64`.
That is the same choice the CloudEvents JSON format makes, which is what makes
`decode(binary) == decode(structured)` for the same event.

| Status | `error.status` | Meaning |
| --- | --- | --- |
| `200 OK` | — | Durable: the broker acknowledged it under `acks=all`. Body is the created resource. |
| `400 Bad Request` | `INVALID_ARGUMENT` | Not a CloudEvent this service accepts: unparseable body, no declared content mode, or a missing/ill-typed attribute. |
| `400 Bad Request` | `OUT_OF_RANGE` | Well-formed, but `time` is outside the plausibility window. |
| `401 Unauthorized` | `UNAUTHENTICATED` | No bearer token, or one that failed signature, `exp`, `nbf`, `iss` or `aud`. |
| `403 Forbidden` | `PERMISSION_DENIED` | A verified token without the `events:write` scope. Retrying will not help. |
| `413 Payload Too Large` | `INVALID_ARGUMENT` | Body over `max-event-bytes`, or batch over `max-batch-events`. `details[0].metadata` carries `limit`, `actual`, `unit`. |
| `503 Service Unavailable` | `UNAVAILABLE` | The event was fine, the broker was not. **The only retryable failure here.** |

**200, not 201, and not 202.** AIP-133 asks a Create to return the resource; 201 would oblige a `Location` header
pointing at a `GET /v1/events/{event}` this service cannot serve, because it owns no storage. Promising a URL that
answers 404 is worse than not promising one. And 202 would be a lie in the other direction: the response is only
written after the broker has acknowledged the record, so nothing about it is pending.

Success body — `Event`, and `name` is the field to store:

```json
{ "name": "events/%2Fgateway%2Fkitchen:evt-1", "id": "evt-1", "source": "/gateway/kitchen",
  "eventType": "com.worxbend.iot.telemetry", "time": "2026-07-01T11:59:00Z",
  "partitionKey": "/gateway/kitchen#kitchen-thermostat",
  "destination": { "topic": "events.cloudevents.v1", "partition": 3, "offset": 42 } }
```

Failure body — one AIP-193 envelope for every status above:

```json
{ "error": { "code": 400, "message": "…", "status": "INVALID_ARGUMENT",
             "details": [ { "reason": "malformed", "domain": "wolfram.worxbend.com", "metadata": {} } ] } }
```

**Branch on `error.status`, not on the HTTP code** — 400 covers three causes, and the canonical `google.rpc.Code`
name is what a generated client switches on. `details[0].reason` is always a value from the closed set
`Meters.Reasons`; it is literally the Prometheus tag value, so a client's error body and an operator's dashboard
use the same vocabulary.

The HTTP status is chosen by the *runtime class* of the failure value (Tapir `oneOf`), so a 413 body can never be
served with a 400 status. `ApiModel.status` restates the mapping so a test can assert the two agree.

### `POST /v1/events:batchCreate` — Batch Create

An `application/cloudevents-batch+json` document: a JSON array of structured-mode CloudEvents. A request whose
`Content-Type` is not that media type is rejected 400 by `IngestApi.batchCreate` before the service is called.

This is a **distinct path rather than a third content mode on `/v1/events`**, because a batch has a different
response shape and OpenAPI keys operations by `(path, method)` — one path serving two response schemas selected by
request media type is a document no generated client can express.

| Status | Meaning |
| --- | --- |
| `200 OK` | Every element was published. |
| `207 Multi-Status` | Some published, some refused; `entries` reports each by index. |
| `400` / `401` / `403` / `413` / `503` | The *document* was rejected as a whole (not JSON, not an array, oversize body, too many elements), or the credential was. |

Body: `BatchCreateResponse { events: [Event], entries: [ { index, event?, error? } ], created, failed }`. `events`
holds the successes in request order — that is the AIP-233 response shape — and `entries` holds every element, so
an index is still resolvable. Exactly one of `event` and `error` is present on each entry.

**This batch is not atomic, which is a deviation from AIP-233 and is deliberate.** AIP-233 specifies that a batch
either wholly succeeds or wholly fails. That guarantee is not available here: the events go to Kafka one at a time
and an acknowledged event cannot be unpublished, so "roll back the successes" is not an operation this service can
perform.

**Why 207 and not 4xx for a partial failure** (`ApiModel.batch`). A batch in which some elements were refused is
not a failed request: the accepted events are already durable and must not be resent. A 400 would be actively
harmful — a client applying the ordinary "retry on 4xx" rule would duplicate every event that succeeded. 207
Multi-Status says exactly "look inside", which is the only truthful answer. The documented client contract is
therefore: **retry only the entries that carry an `error`.**

Entries correlate **by index, not by `id` or `name`**: a CloudEvents `id` is only unique within a `source`, so one
batch may legitimately carry two elements with the same id, and a malformed element has no name at all — that is
what makes it malformed.

Elements are published **sequentially**, not with `Future.traverse`. Concurrency would let two events for the same
device reach the producer's accumulator out of order, discarding the per-key ordering that `partitionKey` exists
to provide. The serial cost is bounded because the batch itself is bounded by `max-batch-events`.

### `POST /v1/events:validate` — Validate

The same request shape as Create, and nothing is published. It runs `IngestionService.validate` — the identical
pure function Create runs — so the two cannot drift: a check added to one is a check added to both.

Body: `ValidateResponse { valid, event?, error? }`. On `valid: true`, `event` is the resource Create *would* have
returned, with an empty `destination` (`topic: ""`, `partition: -1`, `offset: -1`) because nothing was written.
The empty destination is deliberate rather than an `Option`: a client diffing a validate response against a later
create response should see one field's contents change, not the whole message change shape.

**Always 200 when the request itself is usable.** A rejected event is a *successful* validation with
`valid: false` and the AIP-193 `error` body inline — the question was "would you accept this", and "no, because
`time` is 200 days old" is an answer, not a failed request. The 4xx arm is reserved for the request being
unusable: an unverifiable token (401/403), or a body over `max-event-bytes` (413), which cannot be validated
because it is refused before it is read.

This is the textbook AIP-136 custom method: it acts on the collection, it is not one of the five standard
methods, and it has no side effect. It exists because the time window is *configuration* — a payload that
validated during development can start failing in production, and no schema can express that. A producer's own
test suite can point at this endpoint and find out from the running build.

### Operational routes

| Route | Notes |
| --- | --- |
| `GET /metrics` | Prometheus text exposition, `text/plain; version=0.0.4; charset=utf-8`. |
| `GET /health/live` | Always `200 {"status":"UP"}`. |
| `GET /health/ready` | `200 {"status":"UP","broker":"reachable"}` / `503 {"status":"OUT_OF_SERVICE","broker":"unreachable"}`. |
| `GET /openapi.json` | The OpenAPI 3.1 document, generated from the endpoint values by `tapir-openapi-docs` and served rather than published as a file, so it can never describe a build other than the running one. |
| `GET /docs` | **Swagger UI**, on the service's own port. "Try it out" is therefore a same-origin request: no CORS to configure, no second listener to expose. |
| `GET /docs/openapi.yaml` | The same document as YAML. Generators and `jq` want the JSON; humans and `git diff` want this. |

None of these is authenticated, and none is in the OpenAPI document: the document is generated from
`Endpoints.all`, which is the three `/v1` operations and nothing else.

`/metrics`, `/health/*` and `/openapi.json` are **plain Vert.x routes, not Tapir endpoints** (`AdminRoutes`). That
makes their exclusion from `http.server.requests` structural: the metrics interceptor lives inside the Tapir
interpreter, so it never sees them and no exclusion list can fall out of date. `/docs` and `/docs/openapi.yaml`
are the exception — they are `SwaggerUI` server endpoints mounted through the same interpreter as the API, which
is what makes "Try it out" same-origin, and it means Swagger UI traffic *is* timed into `http.server.requests`
under its own route templates. That is a handful of bounded template values on a route a human opens by hand, not
the scrape loop the ADR's exclusion exists for.

### Kafka

| | |
| --- | --- |
| Topic | `events.cloudevents.v1` (configurable), 12 partitions in the reference deployment |
| Content mode | binary — `ce_*` headers, value is the raw payload |
| Key | `Envelope.partitionKey` = `source` or `source#subject` |
| Serializers | `StringSerializer` / `ByteArraySerializer` — **never** `CloudEventSerializer` |
| Fixed producer settings | `enable.idempotence=true`, `acks=all`, `compression.type=zstd`, `linger.ms=5` (from `KafkaCodecs.producerDefaults`) |
| Headers added | binary-mode `ce_*` attributes plus W3C `traceparent` |

The record is fully formed by `KafkaCodecs.producerRecord`, which writes the binary-mode headers itself, because
the SDK's serializer renders `time` in a shape that is not RFC 3339. Letting the SDK re-encode here would
reintroduce the exact defect `modules/eventing` exists to avoid. Free-form `properties` are merged *under* the
correctness settings, so no deployment can switch idempotence or `acks=all` off from a config file.

---

## The stricter-than-spec `time` requirement

`TimeClamp` rejects an event whose `time` is absent, more than `max-future-skew` ahead of the ingest clock, or
more than `max-past-skew` behind it. Three separate decisions, each worth stating because none is inferable:

**`time` is required even though CloudEvents makes it OPTIONAL.** `events.cloud_event` is RANGE-partitioned on
`occurred_at`, which *is* the event's `time`, and the column is `NOT NULL`. An event with no `time` therefore has
no row it could ever become. `modules/kernel` still models `time` as `Option`, deliberately — the codec has to
stay total or a DLQ record could not be parsed back — so the strictness lives at the ingestion edge, where the
rejection sentence can name the missing attribute. The alternative is accepting it here and having cobalt
dead-letter it minutes later, with no client left to tell.

**It rejects rather than clamps, despite the name.** Rewriting an implausible `time` to `now` produces a row that
is *silently wrong*: indistinguishable from a correct one, and unrepairable because the original value is gone.
The name "clamp" is the ADR's and is kept so both documents talk about the same thing.

**The window is asymmetric — 24 h ahead, 90 d behind — because the two directions fail differently.** A future
timestamp is almost always a broken clock, and a garbage one creates the need for a partition years ahead; once
rows land in `cloud_event_default`, creating an overlapping partition takes `ACCESS EXCLUSIVE` and scans it, i.e.
one bad device clock becomes a maintenance outage. A past timestamp, by contrast, is usually a gateway draining a
buffer after an outage — legitimate, and it must keep working, so the past window is the backfill window the
partition-maintenance job keeps open.

Order of validation in `IngestionService.validate` is also deliberate: **size first** (a 2 GB body must be refused
before anything decodes it), **then decode** (everything after needs an envelope), **then the time clamp** (the
only check that is a policy rather than a spec violation, and it reads better on an otherwise known-good event).

---

## Authentication

Every `/v1` operation requires a JWT bearer token. Verification is `JwtVerifier`, and it covers the signature,
`exp`, `nbf`, and — when the deployment sets them — `iss` and `aud`. A token that passes must also carry the
`events:write` scope.

**The algorithm is pinned by configuration and the token's own `alg` header is checked against it, never
trusted.** Accepting the header's choice is the `alg: none` family of attacks, and the
RSA-public-key-used-as-an-HMAC-secret confusion that follows from letting a caller move a key between algorithm
families. `AuthSuite` asserts an `alg: none` token is refused.

**401 and 403 are different answers and the code keeps them apart.** 401 says "I do not know who you are, try
again with a credential"; 403 says "I know exactly who you are and the answer is still no". Collapsing them sends
a client with an expired token into a permissions investigation and a client missing a scope into a token-refresh
loop that can never succeed.

| Variable | Default | Effect |
| --- | --- | --- |
| `AUTH_ENABLED` | `true` | **Defaults on.** A security layer whose default is "off" ships off: nothing fails when it is, the tests pass, and the first evidence is an unauthenticated write in production. Turning it off has to be said out loud. |
| `AUTH_ALGORITHM` | `HS256` | `HS256`/`HS384`/`HS512` (needs `AUTH_SECRET`) or `RS256`/`RS384`/`RS512` (needs `AUTH_PUBLIC_KEY`). |
| `AUTH_SECRET` | — | HMAC secret, minimum 32 characters. Shorter is refused **at boot**, not at the first request. |
| `AUTH_PUBLIC_KEY` | — | Base64 X.509 RSA public key. Armour and `\n` escapes are both accepted, because environment variables cannot hold real newlines portably. |
| `AUTH_ISSUER` | unset | When set, `iss` must equal it. Unset means the claim is not checked — correct for a single-issuer deployment, wrong the moment a second issuer can reach this port. |
| `AUTH_AUDIENCE` | unset | When set, `aud` must contain it. |
| `AUTH_REQUIRED_SCOPE` | `events:write` | Empty disables the scope check while leaving signature verification on. |
| `AUTH_LEEWAY` | `30 seconds` | Skew tolerance on `exp`/`nbf`. Small on purpose: this is the window in which a revoked token still works. |

**Misconfiguration is a boot failure with a sentence naming the field**, never a runtime 500 on the first
authenticated request: an unknown algorithm, an HMAC algorithm with no secret, a secret under 32 characters, an
unparseable public key. A service configured to verify but given nothing to verify against refuses to start
rather than accepting everything.

Minting a token for a smoke test is in `docs/operations.md` §2.

## Configuration

Namespace `wolfram` in `applications/wolfram/src/main/resources/application.conf`. Every value below has a default
and none of them is mandatory. **The exception is the key material in `auth`**: `AUTH_ENABLED` defaults to `true`
and there is no default secret, so a process started with neither `AUTH_SECRET` (or `AUTH_PUBLIC_KEY`) nor
`AUTH_ENABLED=false` **refuses to boot** — see [Authentication](#authentication). A configuration that cannot be
parsed at all aborts the boot for the same reason (`Main` throws rather than starting a service that would fail
every request).

| Env var | HOCON key | Default | Notes |
| --- | --- | --- | --- |
| `HTTP_HOST` | `wolfram.server.host` | `0.0.0.0` | |
| `HTTP_PORT` | `wolfram.server.port` | `8080` | `0` binds an ephemeral port; `WolframApp.port` reports the real one. |
| `INGEST_MAX_EVENT_BYTES` | `wolfram.ingest.max-event-bytes` | `1048576` (1 MiB) | Deliberately below Kafka's default `message.max.bytes`: the API must be the thing that says no, or a 413 becomes a 503 after the request was already accepted. |
| `INGEST_MAX_BATCH_EVENTS` | `wolfram.ingest.max-batch-events` | `256` | A batch is published event-by-event, so an unbounded batch is an unbounded number of in-flight sends from one request. |
| `INGEST_MAX_FUTURE_SKEW` | `wolfram.ingest.max-future-skew` | `24 hours` | See above. |
| `INGEST_MAX_PAST_SKEW` | `wolfram.ingest.max-past-skew` | `90 days` | See above. |
| `KAFKA_BOOTSTRAP_SERVERS` | `wolfram.publisher.bootstrap-servers` | `localhost:9092` | **Effectively mandatory in any deployment.** |
| `KAFKA_TOPIC` | `wolfram.publisher.topic` | `events.cloudevents.v1` | Must match cobalt's `KAFKA_TOPIC`. |
| `KAFKA_MAX_BLOCK` | `wolfram.publisher.max-block` | `2 seconds` | Kafka's own default is 60 s, which parks the calling thread for a minute when the broker is unreachable. |
| `KAFKA_DELIVERY_TIMEOUT` | `wolfram.publisher.delivery-timeout` | `10 seconds` | Bounds the whole send including retries. Kafka requires `delivery >= linger + request`. |
| `KAFKA_REQUEST_TIMEOUT` | `wolfram.publisher.request-timeout` | `5 seconds` | One round trip. |
| `KAFKA_CLOSE_TIMEOUT` | `wolfram.publisher.close-timeout` | `10 seconds` | How long graceful shutdown waits for the accumulator to drain. |
| `KAFKA_QUEUE_CAPACITY` | `wolfram.publisher.queue-capacity` | `1024` | Depth of the sender hand-off queue — the explicit load-shedding point. |
| — | `wolfram.publisher.properties` | `{}` | Free-form producer overrides. File/`-D` only; merged *under* the correctness settings. |

Cross-cutting (from `modules/observability`):

| Env var | Default | Notes |
| --- | --- | --- |
| `SERVICE_VERSION` | `0.0.0-unknown` | The `version` common tag on every meter. |
| `HOSTNAME` | reverse-DNS of the local host, else `unknown` | The `instance` common tag. |
| `OTEL_*` | SDK autoconfiguration | Traces only; `OTEL_SDK_DISABLED=true` turns tracing off. `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` are the ones the reference compose sets. |
| `LOG_LEVEL` | `INFO` | Root level of the JSON Logback config. |

---

## Failure modes and what it does about them

| Failure | Detection | Response |
| --- | --- | --- |
| No token, or one that does not verify | `JwtVerifier.verify`, before the body is looked at | `401`, `status=UNAUTHENTICATED`, with the check that failed named |
| A verified token without `events:write` | ditto | `403`, `status=PERMISSION_DENIED` — retrying is pointless and the code says so |
| Body over the size ceiling | `IngestionService.validate`, before any decoding | `413`, `reason=too-large`, counted on `ingest.events.rejected` |
| Neither content mode declared | `HttpBinding.decode` | `400` with a sentence naming both headers it looked for, rather than a downstream "missing id" that sends the client looking in the wrong place |
| Malformed JSON / not a CloudEvent | `HttpBinding.classify` splits kernel's message | `400`, `reason=malformed` |
| Missing or ill-typed attribute | ditto | `400`, `reason=invalid-attributes` |
| `time` missing or implausible | `TimeClamp.check` | `400` `status=OUT_OF_RANGE`, `reason=invalid-attributes`, with the rendered time, the window, and the ingest clock in the message |
| Broker refuses / times out the record | producer callback, or a synchronous throw from `send` (metadata timeout, full accumulator, serialization) | `503`, `reason=unpersistable`; `kafka.produce.latency{outcome=failure}`; broker marked unreachable |
| **Publish queue full** | `RejectedExecutionException` from the bounded sender | `503` "the publish queue is full"; broker marked unreachable |
| Validated envelope cannot be Kafka-encoded | `KafkaCodecs.producerRecord` returns `Left` | Logged at **error** (it is a validation bug, not a client error) and answered `400` — the client's event is genuinely unrepresentable on this wire |
| Unparseable configuration | `WolframConfig.load` in `Main` | Process aborts rather than booting and failing every request |
| Unusable auth configuration | `JwtVerifier.from` in `WolframApp.start` | Process aborts with a sentence naming the field — an unknown algorithm, an HMAC algorithm with no secret, a secret under 32 characters, an unparseable public key |

**Backpressure is the single-threaded bounded sender** (`KafkaEventPublisher`). `KafkaProducer.send` is only
*mostly* asynchronous — it blocks the caller while topic metadata is unknown or the accumulator is full, for up to
`max.block.ms`. On a Vert.x event loop that is not a latency problem but an availability one, because the loop
serves every other connection too. So sends are handed to a `ThreadPoolExecutor(1, 1, ArrayBlockingQueue(n),
AbortPolicy)`:

- *single-threaded* because the accumulator does the batching, and one thread calling `send` in order preserves
  the per-key ordering `partitionKey` exists to provide;
- *bounded with an abort policy* because that **is** the backpressure. An unbounded queue converts broker
  unavailability into heap exhaustion and turns a shed request into an OOM kill several minutes later.

**Trace context is captured, never inherited.** `Context.current()` is read on the calling thread at the moment
`publish` is invoked and passed explicitly across the hand-off; OTel's context is ThreadLocal-backed, so reading it
on the sender thread would return root and orphan every produce span. The produce span ends in the broker
callback, not when `send` returns, so the span duration and `kafka.produce.latency` measure the same thing:
acknowledgement.

**Shutdown order** (`WolframApp.close`, run from a JVM shutdown hook): HTTP server → publisher → telemetry →
Vert.x. The publisher itself shuts the sender queue down first (no new records), then `flush()`es — those are
records a client is *already holding a 200 for*, and dropping them would make the API a liar — then closes the
producer with a bounded timeout so an unreachable broker cannot hold a rolling deploy open. Telemetry closes after
the drain, not before, so the drain is the one part of shutdown that still has metrics and spans. Each step is
bounded at 10 s. If the sender queue cannot drain in 5 s the remaining tasks are abandoned and the count is
written to **stderr** deliberately — records have been lost and the operator needs to know even if logging is
already torn down.

---

## Metrics and health semantics

All meters carry the common tags `service=wolfram`, `version`, `instance`. Names and tag values come from
`Meters` in `modules/observability`; nothing is invented locally.

| Meter | Type | Tags | Meaning |
| --- | --- | --- | --- |
| `ingest.events.received` | counter | `type`, `mode` (`binary`/`structured`) | **Durable** events — incremented after the broker acknowledged, not on arrival. Attempts are already counted by `http.server.requests`. |
| `ingest.events.rejected` | counter | `reason` (closed set) | Every rejection. Enforced by construction: `IngestionService.record` is the only way to produce a `Rejection` out of that class. |
| `kafka.produce.latency` | timer | `topic`, `outcome` (`success`/`failure`) | Acknowledgement latency. **Broker errors have no separate counter** — they are the `outcome=failure` count of this timer, so the two can never disagree. |
| `http.server.requests` | timer | `method`, `uri`, `status`, `outcome` | Timed by the Micrometer interceptor and by nothing else (`tapir-prometheus-metrics` is rejected in ADR §3.6 because it forks the metric family). `uri` is the **matched route template**, never the raw path; `Telemetry` caps that tag at 100 values as a backstop. |

Plus the JVM/system binders registered by `Telemetry`. `/metrics`, `/health/*` and `/openapi.json` never appear in
`http.server.requests`, because they are not Tapir endpoints; `/docs/**` does, because it is.

**Liveness** is constant: the process is up and the event loop answers. It deliberately consults nothing. A
liveness probe that checked Kafka would fail on every replica at once during a broker outage and restart them all,
turning a recoverable dependency failure into a crash loop that outlives it — restarting a stateless process does
not repair someone else's broker.

**Readiness** reflects broker reachability, because a wolfram that cannot publish would answer 503 to every
request it accepts, so removing it from the load balancer is correct. The answer is read from `BrokerHealth`, a
single `AtomicReference` — **readiness never probes inline**, because a blocking metadata call on the probe path
means a *slow* broker makes readiness *time out*, failing for a reason unrelated to the question. Evidence comes
from two places: every real produce (success or failure) updates it, and a daemon prober calls
`producer.partitionsFor(topic)` every 5 s so an idle service still notices a broker that went away. The initial
state is `Unknown`, which fails readiness — "no evidence" is not good news — and the composition root probes once
synchronously at boot so the first answer is evidence rather than pessimism. A topic that exists but reports no
partitions also counts as unreachable.

---

## Running it locally

Needs a Kafka broker with `events.cloudevents.v1` created (auto-creation is off in the reference compose).

```bash
# broker + topics only
docker compose -f deploy/docker-compose.yml up -d kafka kafka-init

# AUTH_ENABLED defaults to true and there is no key default, so a local run needs
# either a secret or an explicit opt-out. Pick one:
AUTH_SECRET=a-local-development-secret-of-32-chars sbt wolfram/run   # :8080; override with HTTP_PORT
AUTH_ENABLED=false sbt wolfram/run                                   # unauthenticated, said out loud
```

Smoke test both content modes. With `AUTH_ENABLED=false` drop the `Authorization` header; otherwise mint a token
against `AUTH_SECRET` the way `docs/operations.md` §2 does.

```bash
TOKEN=…   # HS256 over AUTH_SECRET, with "scope":"events:write" and an "exp" in the future

# binary mode
curl -i -X POST localhost:8080/v1/events \
  -H "authorization: Bearer $TOKEN" \
  -H 'ce-specversion: 1.0' \
  -H 'ce-id: 1' \
  -H 'ce-source: urn:dev:kitchen' \
  -H 'ce-type: com.worxbend.iot.telemetry' \
  -H "ce-time: $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -H 'content-type: application/json' \
  -d '{"deviceId":"kitchen-1","value":21.5,"unit":"C"}'
# 200 with the created resource; its `destination` says where on the log it landed.

# structured mode
curl -i -X POST localhost:8080/v1/events \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/cloudevents+json' \
  -d "{\"specversion\":\"1.0\",\"id\":\"2\",\"source\":\"urn:dev:kitchen\",\
\"type\":\"com.worxbend.iot.telemetry\",\"time\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\
\"data\":{\"deviceId\":\"kitchen-1\",\"value\":21.6}}"

# would this be accepted? — no broker traffic either way
curl -s -X POST localhost:8080/v1/events:validate \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/cloudevents+json' \
  -d '{"specversion":"1.0","id":"3","source":"urn:dev:kitchen","type":"x","time":"2001-01-01T00:00:00Z"}' | jq

curl -s localhost:8080/openapi.json | jq '.paths | keys'   # unauthenticated, like every operational route
curl -s localhost:8080/health/ready
curl -s localhost:8080/metrics | grep ingest_events
open localhost:8080/docs                                   # Swagger UI, same port
```

Full stack: `sbt wolfram/Docker/publishLocal` then `docker compose -f deploy/docker-compose.yml up -d`, which
publishes wolfram on host port **8081**.

Tests: `sbt "wolfram/Test/testFull"` (no Docker needed — the broker is stubbed behind `EventPublisher`, which is
the entire reason that trait exists) and `sbt "wolfram/IT/testFull"` for the Testcontainers integration suite.
`sbt verify` and `sbt verifyIt` run the fast and slow tiers for the whole build. Note the sbt 2 inversion: bare
`test` is *incremental*, so `testFull` is the one that runs everything.
