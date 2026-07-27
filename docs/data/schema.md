# The database schema

PostgreSQL 18. One migration, `V1__events.sql`, in
`modules/persistence/src/main/resources/db/migration/`, owned by ferrite and run by Flyway with
`-Duser.timezone=UTC`. Everything on this page is transcribed from that file; where it and ADR-0000 §5 differ, the
migration is right and the differences are called out.

The schema is `events`. `pg_trgm` is installed for the dimension tables **only**.

| Object | Kind | Rows |
|---|---|---|
| `events.cloud_event` | RANGE-partitioned fact table | the whole event log |
| `events.cloud_event_2026_07`, `…_2026_08`, `…_default` | partitions | one month each, plus a safety net |
| `events.device`, `events.room`, `events.person`, `events.dim_event_type` | dimension tables | thousands |
| `events.event_rollup_hourly` | materialized view | one row per (hour, type, source, severity) |
| `events.saved_search` | table | content-addressed filter ASTs |

---

## Raw JSONB plus generated columns

```sql
raw                jsonb NOT NULL,          -- the CloudEvent, verbatim
payload_sha256     bytea NOT NULL,

ce_id              text  GENERATED ALWAYS AS (raw ->> 'id')     STORED,
ce_source          text  GENERATED ALWAYS AS (raw ->> 'source') STORED,
ce_type            text  GENERATED ALWAYS AS (raw ->> 'type')   STORED,
device_id          text  GENERATED ALWAYS AS (raw #>> '{data,deviceId}') STORED,
…
```

**There is no second write.** A projection cannot drift from the payload it claims to describe because it *is* the
payload — PostgreSQL recomputes every generated column from `raw` on every insert and update, in the same statement,
inside the same transaction. There is no application code that could forget to update `device_id` when it updates
`raw`, no backfill that could half-finish, and no consumer redelivery that could write the columns from one version of
an event and the JSON from another. The class of bug where a search result and its detail page disagree is not
mitigated here; it is unrepresentable.

The full set of projections:

| Column | Expression | For |
|---|---|---|
| `ce_specversion`, `ce_id`, `ce_source`, `ce_type`, `ce_subject`, `ce_dataschema`, `ce_datacontenttype` | `raw ->> '<attr>'` | CloudEvents context attributes |
| `data` | `raw -> 'data'` | payload search, detail |
| `extensions` | `raw - '{specversion,id,source,type,subject,time,dataschema,datacontenttype,data,data_base64}'::text[]` | custom CE extensions |
| `device_id`, `room_id`, `person_id`, `site_id` | `raw #>> '{data,<key>}'` | smart-home dimensions |
| `severity` | `lower(raw #>> '{data,severity}')` | facet value |
| `severity_rank` | `events.severity_rank(raw #>> '{data,severity}')` | ordered comparison |
| `metric_value` | `events.jsonb_num(raw, '{data,value}')` | charting |
| `tags` | `events.jsonb_text_array(raw, '{data,tags}')` | tag filter |
| `search_doc` | weighted `tsvector`, below | free text |

### Three constraints on what may go into a generated column

**Extraction helpers must be non-throwing.** A bare `(raw #>> '{data,value}')::float8` aborts the *entire insert* — a
whole 500-event batch from cobalt — on one malformed payload from one device. `events.jsonb_num` and
`events.jsonb_text_array` are `plpgsql`, `IMMUTABLE PARALLEL SAFE`, and return `NULL` on any exception. A bad payload
becomes a missing dimension rather than an outage.

**Expressions must be `IMMUTABLE`.** This is what forbids `occurred_at` from being generated: `text::timestamptz` is
only `STABLE` (it depends on `TimeZone`), and PostgreSQL forbids generated columns in a partition key regardless. It is
also why `search_doc` uses the **two-argument** `to_tsvector('simple', …)` form — the one-argument form reads
`default_text_search_config` and is only `STABLE`, so PostgreSQL rejects it outright here.

**The reserved-attribute list must match the domain.** The `raw - '{…}'::text[]` literal is written on one line on
purpose: unquoted array elements are whitespace-trimmed, and wrapping that literal is exactly the kind of edit that
silently changes which keys count as extensions. It must stay identical to
`io.kzonix.kernel.event.Envelope.ReservedAttributes`.

### `severity_rank` and the domain

`events.severity_rank(text)` maps `debug`→10 … `emergency`/`panic`→80, `btrim(lower(…))`, aliases included
(`warn`/`warning`, `err`/`error`, `crit`/`critical`, `emerg`/`emergency`/`panic`). Text alone cannot be range-compared;
the rank can, which is what makes "at least warning" an index scan instead of an `IN` list.

**These are the same numbers and the same spellings as `io.kzonix.kernel.search.Severity`, and they must stay that
way.** If they drift, the UI's alert filter and partial index (11) disagree about what an alert is: a `crit` event
would be an alert to the domain and a `NULL` rank to the database — missing from the alert feed and from
`severity >= warn` searches, while still rendering as critical in the detail view.

### `search_doc`

```sql
setweight(to_tsvector('simple',  coalesce(raw ->> 'type', '')),            'A') ||
setweight(to_tsvector('simple',  coalesce(raw ->> 'source', '') || ' ' ||
                                 coalesce(raw ->> 'subject', '')),         'B') ||
setweight(to_tsvector('english', coalesce(raw #>> '{data,message}', '')),  'C') ||
setweight(jsonb_to_tsvector('simple', coalesce(raw -> 'data', '{}'::jsonb),
                            '["string"]'),                                 'D')
```

`'simple'` for identifiers — device ids, MQTT topics, reverse-DNS type strings — because English stemming mangles them.
`'english'` only for the one field that is actually prose, `data.message`. The `D`-weight pass sweeps every string
value anywhere in `data`, which is what makes an unknown payload shape searchable without anyone registering its
fields.

### `payload_sha256`

SHA-256 over the **canonical** JSON rendering (`Json.noSpaces`), not over the wire bytes, and it does not pretend
otherwise. `jsonb` already discards key order, insignificant whitespace and duplicate keys, so hashing the received
octets would produce a digest that could never be recomputed from what was stored. Hashing the canonical form gives a
digest that *can* be recomputed from the stored row — which is what makes it useful for detecting silent corruption and
cross-partition duplicates.

### Table constraints

```sql
CONSTRAINT cloud_event_pk PRIMARY KEY (occurred_at, event_uid),
CONSTRAINT cloud_event_specversion_ck CHECK (raw ->> 'specversion' = '1.0'),
CONSTRAINT cloud_event_required_ck    CHECK (raw ? 'id' AND raw ? 'source' AND raw ? 'type')
```

The primary key includes the partition key because PostgreSQL requires it. `event_uid` is a surrogate
(`DEFAULT gen_random_uuid()`); `ce_id` is the CloudEvents `id`. Keeping them distinctly named is why nothing in this
schema is ambiguous about which identity is meant.

---

## Monthly partitioning

```sql
) PARTITION BY RANGE (occurred_at);

CREATE TABLE events.cloud_event_2026_07 PARTITION OF events.cloud_event
    FOR VALUES FROM ('2026-07-01 00:00:00+00') TO ('2026-08-01 00:00:00+00');
CREATE TABLE events.cloud_event_2026_08 PARTITION OF events.cloud_event
    FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');
CREATE TABLE events.cloud_event_default PARTITION OF events.cloud_event DEFAULT;
```

Three things follow from range-partitioning on `occurred_at`:

**Retention is metadata, never a `DELETE`.** `ALTER TABLE … DETACH PARTITION … CONCURRENTLY` followed by `DROP TABLE`
removes a month in constant time and generates no dead tuples. A `DELETE` of a month of events would be hours of
vacuum work on a table that is otherwise never vacuumed for dead tuples at all.

**Every time-bounded query prunes.** `occurred_at >= $1 AND occurred_at < $2` — which `Filter.Occurred` compiles to and
which the histogram always applies — eliminates whole partitions at plan time.

**Bounds carry an explicit `+00`.** Partition bounds are parsed in the *session* timezone, so a bare date silently
shifts every partition by the server's offset. This is why Flyway runs with `-Duser.timezone=UTC` as well.

### The default partition is a tripwire, not a fallback

It exists so a clock-skewed producer cannot fail an insert outright, and it **must stay empty**. `partition.default.rows`
is gauged and alerted on, because once the default partition holds rows, creating an overlapping partition takes
`ACCESS EXCLUSIVE` and scans it — a maintenance outage caused by one bad clock. The alarm firing is the cheap moment to
act.

The actual prevention is at the edge: wolfram's `TimeClamp` rejects an event whose `time` is implausible (asymmetric
window — hours ahead, months behind) or absent, rather than clamping it, because a rewritten timestamp produces a row
that is silently wrong and unrepairable.

### Partition maintenance is not in this migration

Deliberately. Migrations are versioned and immutable; partitions are a rolling concern. The migration ships two months
plus the default, and creating months N+3 ahead with `CREATE TABLE IF NOT EXISTS … PARTITION OF` under a
`pg_try_advisory_lock` is a scheduled job's responsibility (ADR §5). *That job is not implemented yet* — see
[open items](#open-items).

### Autovacuum settings go on the leaves

```sql
ALTER TABLE events.cloud_event_2026_07 SET (
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold    = 50000);
-- repeated for _2026_08 and _default
```

Append-only tables are never touched by dead-tuple autovacuum, so insert-driven vacuums have to be forced or the
visibility map goes stale and index-only scans stop being index-only.

**This differs from ADR §5, which sets the reloptions on the parent.** That does not work: a partitioned table stores no
tuples, and PostgreSQL rejects storage parameters on it outright (*"cannot specify storage parameters for a partitioned
table"*). Nor are these settings inherited, which is the sharp edge for the rolling partition job: it must repeat both
reloptions on every partition it creates, or the newest month — the only one actually being written to — is the one
month that never gets an insert-driven vacuum.

Planner statistics targets *are* set on the parent, where they are inherited:

```sql
ALTER TABLE events.cloud_event ALTER COLUMN ce_type   SET STATISTICS 1000;
ALTER TABLE events.cloud_event ALTER COLUMN device_id SET STATISTICS 1000;
```

Both are high-cardinality and heavily skewed — a handful of chatty devices produce most rows — and the default 100
buckets is not enough for the planner to tell a selective device from a firehose.

---

## The dedup contract

```sql
CREATE UNIQUE INDEX cloud_event_identity_uk
    ON events.cloud_event (occurred_at, ce_source, ce_id);
```

This one index is what makes at-least-once delivery survivable. Kafka redelivers; cobalt replays after a crash; the
insert is:

```sql
INSERT INTO events.cloud_event (occurred_at, raw, payload_sha256)
VALUES (?, ?, ?)
ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING
```

CloudEvents guarantees that `(source, id)` is unique per producer. So a redelivered record collides, the insert does
nothing, and the write is idempotent — **at-least-once delivery becomes observationally exactly-once at the database,
and nowhere else in the pipeline.** No consumer-side dedup cache, no transactional Kafka, no exactly-once semantics to
configure and misconfigure.

Three details:

- **Two of the three conflict columns are generated from `raw`.** `ce_source` and `ce_id` are not values the writer
  supplies; they are projections of the document it is inserting. The dedup key therefore cannot disagree with the
  payload it deduplicates, which is the same argument as [the generated-column design](#raw-jsonb-plus-generated-columns),
  applied to correctness of delivery rather than correctness of search.
- `occurred_at` is in the index because a unique index on a partitioned table **must** contain the partition key. That
  is a PostgreSQL requirement, not a design choice, and it is also why the row's timestamp is passed to the insert
  separately even though it is already inside `raw`.
- The batch's `written` count is the number of rows the insert actually created; `batch.size - written` is
  `consume.records.duplicate`. That metric is the direct evidence that redelivery is being *absorbed* rather than
  duplicated, and it can only under-report, never claim a write that did not happen.

Because `Committer.flow` sits strictly downstream of the write, an offset is a receipt for a durable effect. The
combination — commit after the write, dedup on the CloudEvents identity — is the whole delivery story.

---

## Every index, and the query it serves

### Fact table

| # | Index | Definition | Query shape |
|---|---|---|---|
| — | `cloud_event_pk` | `(occurred_at, event_uid)` | detail lookup by `EventRef`; both columns bound, so it prunes to one partition |
| 1 | `cloud_event_identity_uk` | `UNIQUE (occurred_at, ce_source, ce_id)` | `INSERT … ON CONFLICT DO NOTHING` — [above](#the-dedup-contract) |
| 2 | `cloud_event_type_time_ix` | `(ce_type, occurred_at DESC, event_uid DESC)` | `WHERE ce_type = ANY($1) ORDER BY occurred_at DESC, event_uid DESC LIMIT 50`, plus the keyset seek |
| 3 | `cloud_event_device_time_ix` | `(device_id, occurred_at DESC, event_uid DESC) WHERE device_id IS NOT NULL` | device drilldown |
| 4 | `cloud_event_source_time_ix` | `(ce_source, occurred_at DESC, event_uid DESC)` | per-integration timeline (one CE `source` URI) |
| 5a | `cloud_event_room_time_ix` | `(room_id, occurred_at DESC) WHERE room_id IS NOT NULL` | room facet drilldown |
| 5b | `cloud_event_person_time_ix` | `(person_id, occurred_at DESC) WHERE person_id IS NOT NULL` | person facet drilldown |
| 6 | `cloud_event_ingested_brin` | `brin (ingested_at) WITH (pages_per_range = 32, autosummarize = on)` | `WHERE ingested_at >= now() - $1` — ingestion-lag dashboards, backfill windows |
| 7 | `cloud_event_data_gin` | `gin (data jsonb_path_ops)` | `data @> $1::jsonb`, and partially `data @? $1::jsonpath` |
| 8 | `cloud_event_extensions_gin` | `gin (extensions)` | `jsonb_exists(extensions, $1)`, `extensions ->> $1 = $2` |
| 9 | `cloud_event_tags_gin` | `gin (tags array_ops) WHERE tags IS NOT NULL` | `tags @> $1::text[]` |
| 10 | `cloud_event_search_gin` | `gin (search_doc)` | `search_doc @@ websearch_to_tsquery($1::regconfig, $2)` |
| 11 | `cloud_event_alerts_ix` | `(occurred_at DESC) WHERE severity_rank >= 50` | `WHERE severity_rank >= 50 ORDER BY occurred_at DESC LIMIT 20` |
| 12 | `cloud_event_metric_ix` | `(device_id, occurred_at DESC) INCLUDE (metric_value) WHERE metric_value IS NOT NULL` | `WHERE device_id = $1 AND metric_value IS NOT NULL ORDER BY occurred_at DESC` |

Why each is shaped the way it is:

**(2)–(5) put the equality key first and the sort key second.** That single ordering lets one index supply the filter,
the `ORDER BY` *and* the keyset range scan. The keyset predicate is the row-value form
`(occurred_at, event_uid) < ($1, $2)` — never the expanded `a < x OR (a = x AND b < y)`, which the planner cannot turn
into a seek and which would scan every row with `a < x` regardless of `b`. Both columns are `NOT NULL`, which avoids
the `NULLS FIRST/LAST` keyset trap, and `event_uid` is the total-order tiebreaker. Page 10 000 costs what page 1 costs;
`OFFSET` is never used.

**(3), (5), (9), (11), (12) are partial.** System and aggregate events carry no `deviceId` and never appear in device
views, so excluding them shrinks the index rather than bloating it with a NULL run. `severity_rank >= 50` selects well
under 1 % of rows, so the alert feed's index is orders of magnitude smaller than the table.

**(6) is BRIN, not btree.** The table is append-only, so `ingested_at` is almost perfectly correlated with physical
order — the exact case BRIN is for. A few KB replaces a multi-GB btree. `autosummarize` matters because the newest
range is the one every lag query reads and it would otherwise stay unsummarised until the next vacuum.

**(7) is `jsonb_path_ops`, (8) is default `jsonb_ops`.** `path_ops` is about half the size and materially faster for
`@>` and `@?`, at the cost of the `?` / `?|` / `?&` key-existence operators — which the UI does not use on `data`.
Extension filtering *is* key-existence based, so `extensions` needs `jsonb_ops`; it is a tiny column, so the size cost
is irrelevant. (In SQL, key existence is always spelled `jsonb_exists(extensions, ?)` and never with the bare `?`
operator, which collides with the JDBC placeholder.)

**(12) uses `INCLUDE`.** `metric_value` rides along as a non-key payload column, so plotting a chart is an index-only
scan with no heap fetch per point.

**Free text is `websearch_to_tsquery`, never `to_tsquery`.** It accepts `"quoted phrase" -excluded or` and, critically,
does not raise a syntax error on malformed input — so a stray `&` typed into the search box is not a 500.

### Deliberately absent: a trigram GIN on `ce_subject`

Unanchored `ILIKE '%…%'` over the fact table is **not supported**. A trigram GIN over 10⁸ rows is a multi-GB index that
also slows every insert. Substring discovery happens on the dimension tables and resolves into an exact equality
filter; whole-word search is served by (10).

### Dimension tables — where `pg_trgm` belongs

| Index | Table | Query shape |
|---|---|---|
| `device_label_trgm_ix` | `events.device` | `WHERE label ILIKE $1` — autocomplete |
| `device_id_trgm_ix` | `events.device` | `WHERE device_id ILIKE $1` — autocomplete |
| `dim_event_type_trgm_ix` | `events.dim_event_type` | `WHERE ce_type ILIKE $1` |
| `room_label_trgm_ix` | `events.room` | `WHERE label ILIKE $1` |
| `person_label_trgm_ix` | `events.person` | `WHERE label ILIKE $1` |

These tables hold thousands of rows, not hundreds of millions. `events.device` additionally carries `label`, `room_id`,
`first_seen`, `last_seen` and `event_count`; `room`, `person` and `dim_event_type` are `(id, label?, last_seen)`.

Every index in the migration carries a catalog `COMMENT` naming its query shape, so an index nobody can name gets
dropped.

---

## The hourly rollup

```sql
CREATE MATERIALIZED VIEW events.event_rollup_hourly AS
SELECT date_trunc('hour', occurred_at) AS bucket, ce_type, ce_source,
       coalesce(severity, 'none') AS severity,
       count(*)                                    AS event_count,
       count(*) FILTER (WHERE severity_rank >= 50) AS error_count,
       count(DISTINCT device_id)                   AS device_count,
       avg(metric_value) AS avg_value, min(metric_value) AS min_value,
       max(metric_value) AS max_value
FROM events.cloud_event
WHERE occurred_at >= now() - interval '90 days'
GROUP BY 1,2,3,4
WITH NO DATA;

CREATE UNIQUE INDEX event_rollup_hourly_uk
    ON events.event_rollup_hourly (bucket, ce_type, ce_source, severity);
CREATE INDEX event_rollup_hourly_bucket_ix
    ON events.event_rollup_hourly (bucket DESC);
REFRESH MATERIALIZED VIEW events.event_rollup_hourly;
```

**A materialized view rather than counters maintained by the consumer.** Counters incremented by an at-least-once
consumer drift on redelivery — the row that `ON CONFLICT DO NOTHING` correctly declines to write is the row a counter
would have double-counted. The MV is recomputed from the fact table, so it is idempotent and authoritative by
construction.

`event_rollup_hourly_uk` is not decoration: **a unique index is what makes `REFRESH MATERIALIZED VIEW CONCURRENTLY`
legal.** Without it the refresh takes `ACCESS EXCLUSIVE` and every dashboard read blocks for the duration.
`event_rollup_hourly_bucket_ix` serves the landing-page histogram (`WHERE bucket >= $1 ORDER BY bucket DESC`).

`coalesce(severity, 'none')` is required rather than cosmetic: `NULL` in a unique index column would let two rows with
otherwise identical keys coexist, and `CONCURRENTLY` needs the key to be genuinely unique.

`WITH NO DATA` followed by an explicit `REFRESH` keeps the migration fast on a populated database and makes the first
population an ordinary, interruptible statement rather than part of the DDL transaction.

The refresh cadence (every ~5 minutes, guarded by `pg_try_advisory_lock` so replicas do not race) is a scheduled job,
not part of the migration. The tripwire that reopens the "MV vs. counters" decision is a refresh taking longer than
60 seconds.

---

## Saved searches

```sql
CREATE TABLE events.saved_search (
    slug       text PRIMARY KEY,       -- base32(sha256(ast))[0,12]
    ast        jsonb NOT NULL,
    label      text,
    created_at timestamptz NOT NULL DEFAULT now());
```

Content-addressed, so the table is immutable by construction: the key *is* the hash of the value. Only filters too long
for a querystring land here (`?s=k3f9x2mq7z1a`); short ones stay in the URL, where they remain readable and
hand-editable. Cursors are a separate mechanism and are legitimately opaque — base64url of
`(occurred_at, event_uid, filterFingerprint)`, where the fingerprint invalidates a cursor whose filter changed.

---

## How the read path uses all of this

| Query | Shape | Index |
|---|---|---|
| Search page | `SELECT occurred_at, event_uid, ingested_at, ce_id, ce_source, ce_type, ce_subject, device_id, room_id, person_id, severity, severity_rank, metric_value FROM events.cloud_event WHERE … ORDER BY occurred_at DESC, event_uid DESC LIMIT n` | (2)–(5) + partition pruning |
| Detail | same columns `+ raw`, `WHERE occurred_at = ? AND event_uid = ?` | `cloud_event_pk` |
| Facets | `WITH cand AS MATERIALIZED (SELECT dims … LIMIT 50000)` then one `GROUP BY GROUPING SETS ((ce_type),(ce_source),(device_id),(room_id),(person_id),(severity),())` | whichever of (2)–(11) the filter selects |
| Tag facet | `cand CROSS JOIN LATERAL unnest(cand.tags)` | (9) via the candidate set |
| Histogram | `generate_series(…) LEFT JOIN (SELECT date_bin(…), count(*) … GROUP BY 1)` | (2)–(5) + pruning |
| Result total | `SELECT count(*) FROM (SELECT 1 FROM … LIMIT 10001) t` | as the filter selects |

**The list projection omits `data` and `raw`**, so the planner never de-TOASTs payloads for rows the user will not
open. Detail fetches them by primary key.

**`MATERIALIZED` on the candidate CTE is not optional.** Without it the planner may inline the CTE into each grouping
set and re-apply the filter, turning one capped scan into several uncapped ones and deleting the cap's entire purpose.
When the cap is reached (50 000 by default, 200 000 at
most), every facet count is a lower bound and the UI renders "50,000+" — the same honest
approximation Kibana and GitHub ship, and a signed-off product decision rather than a hidden implementation detail.
Totals are bounded the same way: `SELECT 1` *inside* the subquery, because the `LIMIT` can only stop a scan that is
producing rows.

**The histogram's `generate_series` skeleton** is what makes an empty hour render as a zero bar. Without it a quiet
period disappears from the chart and the shape of the data becomes a lie told by omission. The window is re-bounded in
the query itself rather than trusted to the caller's filter, so a broader filter cannot bin months of events into the
first bucket.

---

## Open items

These are provisioned by the migration but not yet read or maintained by any code, and are listed so nobody mistakes
DDL for behaviour:

- **The rolling partition job.** No `CREATE TABLE … PARTITION OF` runs anywhere outside the migration, so events after
  `2026-09-01` land in `cloud_event_default`.
- **The MV refresh job.** `events.event_rollup_hourly` is populated once, by the migration. Nothing refreshes it, and
  no query reads it — ferrite's histogram currently goes to the fact table.
- **Dimension-table population.** `events.device`, `room`, `person` and `dim_event_type` have no writer, so
  autocomplete has nothing to complete against yet.
- **`events.saved_search`** has no reader or writer; long filters are not yet persisted.

See [the event model](../event-model.md) for what is stored in `raw` and why it is the canonical form.
