# The event model

Everything in this system is a [CloudEvent 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md).
Not "an internal type that can be exported as a CloudEvent" — the CloudEvent *is* the record. It is what wolfram
validates, what travels on Kafka, what is stored verbatim in `raw jsonb`, and what ferrite renders. There is no second
canonical shape anywhere in the pipeline, and that is the single decision the rest of this page elaborates.

The consequence worth stating up front: **an event type this build has never seen still reaches Postgres, still
appears in search results, and still renders in the UI.** Nothing on the write path branches on `type`.

---

## The domain type

`io.cloudevents.CloudEvent` — the SDK type — is an adapter, not a domain type. It is a Java interface with nullable
getters, a throwing mutable builder and a byte-oriented data model; it does not pattern-match. It is confined to
`modules/eventing`. The type the three services agree on is `com.worxbend.kernel.event.Envelope`:

```scala
final case class Envelope(
  id: EventId,
  source: Source,
  eventType: EventType,
  time: Option[OffsetDateTime],
  subject: Option[Subject],
  dataContentType: Option[ContentType],
  schema: Option[SchemaRef],
  extensions: Map[String, AttrValue],
  payload: Payload
)
```

Four things in that signature are load-bearing.

**`time` is `Option[OffsetDateTime]`.** `OffsetDateTime` and never `Instant`, because CloudEvents `time` is RFC 3339
*with an offset* and `Instant` normalises to UTC, discarding the producer's local offset — which for a smart home is
diagnostic information, not noise. `Option` because the spec makes `time` OPTIONAL and the codec has to stay total: a
DLQ record must parse back. The *ingestion API* is stricter than the codec and rejects an event with no `time`
(`TimeClamp`), because `occurred_at` is the `NOT NULL` partition key and there is no month to file a timeless event in.

**`payload` is an ADT, not two nullable fields.** JSON Format 1.0 offers `data`, `data_base64`, or neither, and they are
mutually exclusive on the wire:

```scala
enum Payload:
  case Structured(json: Json)
  case Opaque(bytes: Binary, mediaType: ContentType)
  case Empty
```

"Both present" — which the spec forbids — is unrepresentable rather than a validation rule someone forgets. `Structured`
holds an arbitrary `Json` and not a decoded domain type; that is what lets an unknown payload shape survive.

**`extensions` are typed.** CloudEvents defines an attribute type system, so `AttrValue` models it rather than
collapsing everything to `Map[String, String]`:

```scala
enum AttrValue:
  case Text(v: String); case Num(v: Int);   case Flag(v: Boolean)
  case Time(v: OffsetDateTime); case Ref(v: URI); case Bytes(v: Binary)
  case Other(v: Json)
```

`Other` is not in the ADR. It exists because losslessness is a hard requirement and the JSON format cannot be made
total without it: a non-integral number, an array or an object as an extension value is out of spec, but dropping it —
or failing the whole event over it — loses data this system promised to keep. `Envelope.canonical` makes the format's
one *type-erasing* direction explicit and testable: `Time`, `Ref` and `Bytes` all serialise to plain JSON strings and
come back as `Text`, because JSON Format 1.0 carries no per-extension type information.

**The codec is hand-written, not derived.** Derivation cannot express either of the two things the format actually
does: extensions are *flattened* into the top-level object rather than nested under a field, and the data slot is one
of two differently named keys chosen by the payload's shape. Attribute order is fixed so a content hash over a
rendered event is stable. The property that holds is `decode(encode(e)) == e.canonical`, for every envelope.

## Opaque identifiers

```scala
opaque type EventId     <: String = String   // CloudEvents `id`
opaque type Source      <: String = String   // RFC 3986 URI-reference
opaque type EventType   <: String = String   // reverse-DNS
opaque type Subject     <: String = String   // device/entity within the source
opaque type ContentType <: String = String   // RFC 2046 media type
```

`opaque type … <: String` rather than a wrapper case class. The upper bound gives **one-way assignability**: an
`EventId` flows into a JDBC setter, a circe encoder or a log statement with zero allocation and zero unwrapping, while
a `Source` can still never be passed where an `EventId` is expected.

That asymmetry is not stylistic. `(source, id)` is the deduplication key of the entire pipeline — see
[the dedup constraint](data/schema.md#the-dedup-contract) — and silently swapping the two arguments would produce a
system that looks correct and deduplicates nothing. `Subject` is a distinct type for the same reason: it is half the
Kafka partition key, and appending the wrong attribute there destroys per-device ordering invisibly, months before
anyone plots a device timeline and notices.

Smart constructors returning `Either[String, X]` are the only way in, and the validation is deliberately anaemic — the
kernel's job is to reject what the spec calls invalid, not to impose a house dialect that would make this build unable
to read events other conformant tools produce. `id`, `type` and `subject` must be non-blank; `source` and `dataschema`
must parse as URI-*references*, so relative forms like `/sensors/kitchen-1` are accepted, because the spec allows them
and MQTT-style gateways use them constantly.

## Why an unknown event type still works

`Envelope.decoder` is **total over anything spec-valid and never inspects `type`.** It checks `specversion == "1.0"`,
refines the required attributes, treats an attribute explicitly set to `null` as absent (the spec forbids null
attributes, so nothing is lost, and rejecting instead would fail a whole batch over a producer's serialiser default),
sweeps every non-reserved key into `extensions`, and puts whatever was in `data` into `Payload.Structured` unexamined.

Refinement into the known-type ADT is a **separate, separately total** step. That split is the load-bearing decision of
the whole model:

- **wolfram** validates spec conformance and `time` plausibility. It does not know what a telemetry event is.
- **cobalt** persists `envelope.toJson` into `raw jsonb`. It does not know what a telemetry event is either.
- **Postgres** derives every queryable column from `raw` with `GENERATED ALWAYS AS … STORED`, so a new event type gets
  indexed dimensions for free the moment it lands.
- **ferrite** renders the stored `raw` as JSON when it has nothing more specific to show.

So the failure mode of a firmware update that ships a new event type is a slightly less pretty detail page — never data
loss, never a poison record, never a stalled consumer.

## The `Observation` ADT

```scala
enum Observation:
  case Telemetry(device: Subject, metric: String, value: Double, unit: String)
  case StateChanged(device: Subject, from: String, to: String)
  case Alarm(device: Subject, severity: Int, message: String)
  case Unrecognised(eventType: EventType, payload: Payload, reason: Option[String])

def from(envelope: Envelope): Observation   // total: never throws, never Either
```

`Unrecognised` is **not** an error case. It is the total fallback, and without it persistence and the UI would depend
on this enum being complete. An `Either` here would push the decision onto every call site and the answer would be the
same every time: keep the event. Failure is therefore *data*, not control flow.

Totality is defended at three levels:

1. the registry lookup returns `Unrecognised` for an unregistered `(type, major)`;
2. a circe `Left` becomes `Unrecognised(…, Some(failure.message))`;
3. a `NonFatal` catch wraps the lot — belt and braces around third-party decoders, because totality is a promise made
   to the ingest path, and a promise that depends on a library's internal discipline is not a promise.

Refinement is lossy by construction: `Unrecognised` keeps the payload but not the source, extensions or time. Anything
that both routes on the observation and persists the event therefore carries `Observed(envelope, observation)` — the
pair — rather than choosing one.

Device identity resolves from `subject` first, falling back to `data.deviceId`. `subject` wins because it is the
attribute the partition key is built on, so a disagreement between the two must resolve the same way here as it does in
the database.

**The guardrail:** every `Unrecognised` increments `event.unrecognised{type,reason}`. `Observation.knownTypes` exposes
the `(type, major)` pairs the registry answers for, because that alert is only meaningful against a known list — a
nonzero rate for a type the registry *claims* to know is a broken decoder, and without the tag pair it is
indistinguishable from a new device.

## Versioned schemas

**Versions hang off `dataschema`, never off the `type` string.** The recognised types are bare reverse-DNS:
`com.worxbend.iot.telemetry`, `com.worxbend.iot.state-changed`, `com.worxbend.iot.alarm`. A type string carrying its own version
forks the registry on every additive change and turns "give me all telemetry" from an equality match into a prefix
match.

`SchemaRef` wraps the raw `dataschema` URI and derives `name` and `version` as *views* over its path segments:

```
https://schemas.worxbend.io/iot/telemetry/1.2.0
                              ^^^^^^^^^ ^^^^^
                              name      SemVer(1,2,0)
```

The URI is the single field and equality is on the URI alone — which is what makes the envelope round-trip exact, and
what keeps a five-year-old event explainable: the raw `dataschema` is stored verbatim in `raw` and projected into
`ce_dataschema`. A URI that does not end in `…/<name>/<major.minor.patch>` is still a perfectly valid `SchemaRef` with
`name` and `version` absent.

Only **major** participates in dispatch. The registry is keyed on `(EventType, major)`; minor and patch bumps are
required to be additive and the decoders ignore unknown fields, so one entry serves every 1.x. An event with no
`dataschema` at all assumes `DefaultMajor = 1` — the common case for a device that has only ever emitted one shape.

There is deliberately **no fallback from an unregistered major to the default one.** A major bump means the payload
changed incompatibly, so decoding a 2.x payload with the 1.x decoder would produce a confidently wrong reading instead
of an honest `Unrecognised`. `SemVer.parse` is likewise not a full SemVer 2.0.0 parser: pre-release and build metadata
have no meaning for a registry keyed on major, and accepting them would invite `1.2.0-rc1` and `1.2.0` to be treated as
the same schema.

## Content modes: binary on the wire, structured in the DLQ

The CloudEvents Kafka binding lays an event onto a record in one of two ways, and this build uses both — for opposite
reasons.

| | `events.cloudevents.v1` | `events.cloudevents.v1.dlq` |
|---|---|---|
| Mode | **Binary** | **Structured** |
| Attributes | `ce_*` Kafka headers | inside the JSON value |
| Value | the payload, untouched | the whole event as one blob |
| Media type header | describes the *payload* | `application/cloudevents+json` |

**Binary on the main topic.** Brokers, single-message transforms and `kcat` route on `ce_type` / `ce_source` without
deserialising anything. The payload is never re-encoded, so an event whose schema this build has never seen round-trips
byte-identically. Binary data is not double-base64'd, and the records are smaller. The cost — extension attributes lose
their CE type, because a header is bytes — is named explicitly by `CloudEventAdapter.binaryCanonical` rather than
discovered later.

**Structured on the DLQ.** A poison record is read by a human under time pressure with `kcat`, and reassembling an
event from a dozen headers at that moment is exactly the wrong task. Self-containment also makes replay a copy rather
than a reconstruction. The dead letter is *itself* a CloudEvent (`com.worxbend.eventing.dead-letter`) so the DLQ holds the
same kind of thing as every other topic and needs no second reader that nobody exercises until the day it matters.

Two implementation notes that are easy to get wrong and are therefore fixed in `ContentMode`:

- **Mode detection checks `ce_specversion` first, and that order is load-bearing.** In binary mode the `content-type`
  header describes the *payload*, and a payload may perfectly well be `application/cloudevents+json` — a forwarder, a
  replay tool, an event that quotes another event. Deciding on `content-type` first, as the SDK's own
  `MessageUtils.parseStructuredOrBinaryMessage` does, misreads exactly those records as structured and loses every
  attribute in the headers. A genuine structured record never carries `ce_specversion`; its specversion is inside the
  JSON. So that header's presence is the unambiguous signal and the media type is only the fallback.
- **Binary `time` is rendered by `com.worxbend.kernel.Rfc3339`, not by the SDK serializer.** The SDK uses
  `DateTimeFormatter.ISO_OFFSET_DATE_TIME`, which omits the seconds field when it is zero — `2024-01-01T17:31Z` — and
  that is not RFC 3339. It parses back fine, so the defect is invisible in any Java-only round trip and surfaces only
  against a stricter consumer in someone else's stack.

Reading is asymmetric with writing on purpose: binary decode goes through the SDK's header reader, because writing is
where this build is the author and must get RFC 3339 right, while reading is where it must accept whatever a
conformant third-party producer emits. That also makes the round-trip property prove *interoperability* rather than
merely proving two functions in one file are inverses.

## The partition key

```scala
def partitionKey: String = subject.fold[String](source)(s => s"$source#$s")
```

One definition, in `modules/kernel`, used by the producer and asserted by kernel's own tests.

`source#subject` and not `subject` alone, because **subjects are producer-local**. `kitchen-1` means different things
behind two gateways; keying on the subject would interleave two devices' timelines onto one partition and destroy the
per-key ordering the entire design rests on. Events with no subject key on the source, which keeps a gateway's
aggregate stream ordered.

Kafka's default partitioner hashes the key, so all records for one `(source, subject)` pair land on one partition and
are therefore totally ordered. **Changing this function is as breaking as changing the partition count** — both rehash
every key and interleave a device's timeline across the transition. `Topics.CloudEventsPartitions = 12` is chosen
generously up front and documented as a one-way door for the same reason.

The DLQ keys differently: `Topics.dlqKey(topic, partition, offset)`. A record that failed to *decode* has no
CloudEvents `id`, because parsing the `id` is what failed; the origin coordinates are its only identity. Keying on them
means a replayed poison record *overwrites* its predecessor under compaction instead of accumulating a copy per retry —
and `DeadLetter.toEnvelope` derives the dead letter's `id` from the same coordinates, so a replayed poison record is
idempotent at the database in exactly the way a replayed good record is.

## Searching inside the payload and the extensions

Two of the filter grammar's leaves reach past the fixed dimensions into the parts of an event this build was never
told the shape of:

```scala
case PayloadCmp(path: JsonPath, op: NumOp, value: NumLit)   // data.sensor.temperature = >21
case ExtensionEq(name: ExtName, value: ExtValue)            // ext.tenantid            = acme
```

They are the only leaves whose *parameter name* comes from the user, so they are the only ones spelled as a key
prefix in a permalink:

```
/events?v=1&from=2026-07-01T00:00:00Z&data.sensor.temperature=%3E21&ext.tenantid=acme
```

`data.<path>` takes an operator-led number — `>21`, `>=18`, `<>0`, or a bare `21`, which means equality. The
operators are the *jsonpath* spellings (`==`, `!=`) because that is where they end up, with `=` and `<>` accepted on
the way in because that is what a person types. `ext.<name>` takes the value verbatim.

The rejected alternative was a fixed, repeatable key carrying the whole predicate in its value —
`payload=sensor.temperature>21`. It would let a plain HTML form post one from a single text input, which the prefixed
form cannot do without JavaScript. It loses on everything else: the codec would have to find the operator inside a
string that may legitimately contain `>` or `:`, and every URL edit the UI performs (`Query.remove(pairs, key,
value)` behind a chip, `Query.toggle` behind a facet) would need a second parser in the presentation layer. Neither
family has a visible input in the filter bar — nor do `type`, `device`, `room`, `person` or `tag` — so the
form-friendliness bought nothing that was not already being paid for.

### What the codec guarantees

`FilterQuery.decode` is total, and it is the only way a filter reaches the application from a browser. Three rules
apply to these two families specifically, all for the same reason: a parameter that is quietly ignored produces a
**wider** result set than the URL describes, and a user who cannot see that their filter was dropped will trust the
number in front of them.

- A key that starts with `data.` or `ext.` is always one of these leaves or a `FilterError`, never an unknown
  parameter and never a silent skip. `data.a b=>1` is reported against `data.a b`; `ext.Tenant=acme` against
  `ext.Tenant`.
- **Repeating `ext.<name>` is an error.** The grammar has only equality on an extension, so two values for one name
  conjoin into something no row can satisfy; reporting it beats an empty page that reads as "nothing matched".
  Repeating `data.<path>` is *not* an error — `data.t=>18&data.t=<24` is how the grammar spells a range.
- Every value crosses into SQL through a smart constructor. `NumLit` bounds the comparison value to 38 significant
  digits and 18 decimal places, and canonicalises it to its plain form; `ExtValue` requires 1–256 characters. The
  numeric bound is not tidiness: `jsonPathPredicate` renders with `toPlainString`, whose length is `precision +
  |scale|`, and `?v=1&data.t=>1E%2B2000000000` is a valid `BigDecimal` with one significant digit that would render
  as a two-gigabyte string. Canonicalising also keeps the AST canonical — `1E+8` and `100000000` are `==` with
  different `toString`s, and `Filter.sortKey` breaks ties on `toString`.

### What the database can and cannot do with them

Both leaves are correct at any table size. Only one of them is *fast* at any table size, and the difference is worth
knowing before an incident makes you find out.

| Filter | Compiles to | Index | Access path |
| --- | --- | --- | --- |
| `ext.tenantid=acme` | `extensions ?? ? AND extensions ->> ? = ?` | (8) `gin (extensions)`, jsonb_ops | Bitmap index scan on the existence conjunct, heap recheck on the value |
| `data.value=21` | `data @?? ?::jsonpath` → `$.value ? (@ == 21)` | (7) `gin (data jsonb_path_ops)` | Bitmap index scan on the extracted key `$.value = 21` |
| `data.value=>21` | `data @?? ?::jsonpath` → `$.value ? (@ > 21)` | (7), **with no search key** | Full index scan, then a recheck of everything it returned |

**The extension filter is compiled as two conjuncts on purpose.** `extensions ->> $1 = $2` is what it used to be, and
`->>` is in no GIN operator class — so the predicate had no access path at all and every extension filter was a
sequential scan of the fact table, correct and unbounded. `extensions ? $1` (written `??`, pgjdbc's escape, exactly
as `@?` is written `@??`) *is* in jsonb_ops. The second conjunct is redundant in meaning — `->>` is NULL when the key
is absent, so the `AND` narrows nothing — and decisive in plan. The `->>` half stays because it is what defines the
answer: replacing the pair with `extensions @> jsonb_build_object(?, ?)`, also indexable, would match only JSON
*strings*, so `ext.sequence=7` would stop matching `"sequence": 7`. Note that the catalog `COMMENT` on index (8)
writes the existence test as `jsonb_exists(extensions, $1)`; the function spelling means the same thing and is *not*
in the operator class, so it would put the scan back.

**A payload range comparison cannot use index (7), and no rewriting fixes it.** A GIN index answers `@?` by pulling
clauses of the form `accessors_chain = constant` out of the jsonpath. `$.value ? (@ == 21)` yields one and plans as a
selective bitmap index scan; `$.value ? (@ > 21)` yields none, so the index is scanned in full and every entry is
handed to the recheck. There is no indexable existence conjunct to bolt on, because `jsonb_path_ops` supports
neither the `?` operator nor a valueless `@?` — that is the price the schema deliberately paid for an index half the
size of `jsonb_ops`.

The practical consequence, and the thing to tell an operator: **a payload range filter is a refinement, not a
selection.** Combine it with a time window (which prunes whole partitions) or a device, and the range is applied to a
few thousand candidate rows. Run it alone across all of retention and it costs a full pass. The equality form has no
such caveat.

`FilterAccessPathIT` asserts all of this against a real PostgreSQL rather than leaving it as a claim: it plans each
predicate with `enable_seqscan = off`, so a fallback to a sequential scan means "this predicate has no index path"
rather than "the planner thought scanning was cheaper", and it measures the rows the bitmap index scan returns to
tell the extractable jsonpath from the unextractable one. If a future PostgreSQL learns to extract range clauses,
that test fails — which is the right way to find out.

### In the UI

Both families are chipped in the filter bar, labelled with the parameter name and valued with the raw text, so the
chip is a legend for the URL. They travel through a form submit as hidden fields — without which typing in the
search box would silently drop every one of them — and they are removed the same way a facet selection is, by
editing the query string.

---

## End to end

```mermaid
sequenceDiagram
    autonumber
    participant D as Device / gateway
    participant W as wolfram<br/>(Tapir + Vert.x 5)
    participant K as Kafka<br/>events.cloudevents.v1
    participant Q as Kafka<br/>…v1.dlq
    participant C as cobalt<br/>(Pekko Streams)
    participant P as PostgreSQL<br/>events.cloud_event
    participant F as ferrite<br/>(Play 3 + HTMX)

    D->>W: POST /events<br/>binary (ce-* headers) or structured
    activate W
    Note over W: Envelope.decoder — total, never inspects `type`<br/>specversion == 1.0, attributes refined
    Note over W: TimeClamp: `time` present and plausible<br/>(hours ahead, months behind) — reject, never invent
    alt invalid
        W-->>D: 400 with the failing attribute named
    else accepted
        W->>K: send(key = source#subject, binary mode)<br/>+ traceparent injected into headers
        Note over W,K: acks=all, enable.idempotence=true,<br/>compression=zstd, linger.ms=5
        K-->>W: ack
        W-->>D: 202 Accepted
    end
    deactivate W

    K->>C: committableSource<br/>(StringDeserializer / ByteArrayDeserializer)
    activate C
    Note over C: never CloudEventDeserializer — a throwing<br/>deserializer throws inside poll(), before the<br/>connector sees the record, and the restart<br/>replays it forever
    Note over C: ContentMode.read → Either[DecodeFailure, Envelope]<br/>extract traceparent → CONSUMER span
    alt undecodable
        C->>Q: DeadLetter.toEnvelope, structured mode<br/>key = topic/partition/offset
        Note over C: offset still committed — the record is durable in the DLQ
    else decoded
        C->>C: groupedWithin(500, 250ms) → mapAsync(1)
        C->>P: INSERT … ON CONFLICT<br/>(occurred_at, ce_source, ce_id) DO NOTHING
        P-->>C: rows written (batch size − written = duplicates)
        Note over C,P: Committer.flow is strictly downstream:<br/>an offset is a receipt for a durable effect
    end
    deactivate C

    Note over P: raw jsonb stored verbatim;<br/>every projection GENERATED ALWAYS AS … STORED

    F->>P: keyset SELECT / facets / histogram / detail
    P-->>F: page + nextCursor
    Note over F: ferrite has no Kafka on its classpath at all
```

### Stage notes

**1 — Ingest (wolfram).** Tapir endpoint *descriptions* cover both content modes, so OpenAPI is generated for free.
The body is `byteArrayBody` and not `stringBody` or `jsonBody`, because binary mode's payload is arbitrary bytes.
`POST /events/batch` accepts `application/cloudevents-batch+json` on a distinct path rather than as a third mode on
`/events`. Validation rejects and never invents defaults — including for `time`, whose plausibility window is
asymmetric (hours ahead, months behind) because the two directions fail differently: a future timestamp is almost
always a broken clock, while a past one is usually a gateway draining a buffer after an outage, which is legitimate and
must keep working.

**2 — Produce (wolfram).** A single plain `KafkaProducer`, not a pool: two producers could reorder two records for one
key. `KafkaCodecs.producerDefaults` (`enable.idempotence`, `acks=all`, `compression.type=zstd`, `linger.ms=5`) is
merged *over* deployment config, so no config file can switch idempotence off. Sends go through a bounded hand-off
queue, so saturation surfaces as a 503 rather than as parked event-loop threads — `KafkaProducer.send` blocks while
metadata is unknown or the accumulator is full, and its `max.block.ms` default of 60 s on an event loop is an outage.

**3 — Consume (cobalt).** `ByteArrayDeserializer`, decode inside a stream stage, `Either` out. A bad record is an
ordinary value that goes to the DLQ and *still has its offset committed* — the one case where an offset is committed
without the event having been processed, which is only defensible because the DeadLetter carries why (a bounded
`reason` tag plus unbounded `detail`), where (the origin coordinates) and what (the original value bytes verbatim,
plus the headers as text).

**4 — Persist (cobalt).** `mapAsync(1)`, not a larger parallelism: `mapAsync(n)` preserves *emission* order but starts
n batches concurrently, so two batches from one partition would be in flight at once. The insert is idempotent, so
at-least-once redelivery plus CloudEvents' own `(source, id)` uniqueness contract is observationally exactly-once at
the database — and the shortfall between batch size and rows written is `consume.records.duplicate`, the direct
evidence that redelivery is being absorbed rather than duplicated. The whole inner graph sits inside
`RestartSource.onFailuresWithBackoff`.

**5 — Read (ferrite).** Keyset-paginated queries, facet counts, histogram and detail over `events.cloud_event`. See
[the schema](data/schema.md) for the access paths.

---

## Where each piece lives

| Concern | Module | Note |
|---|---|---|
| `Envelope`, `Payload`, `AttrValue`, `SchemaRef`, opaque ids, `Observation`, `partitionKey`, `Topics` | `modules/kernel` | circe and the stdlib only — asserted at build load |
| SDK adapter, content modes, `CloudEventHeaders`, `DeadLetter`, traceparent inject/extract | `modules/eventing` | the only place that knows both the domain envelope and the wire format |
| `raw jsonb`, generated projections, the idempotent insert | `modules/persistence` | see [data/schema.md](data/schema.md) |

`modules/eventing` being the *only* place that knows both halves is what makes it impossible for wolfram's producer and
cobalt's consumer to disagree about the encoding. That is the entire argument for the module existing.
