-- ============================================================================
-- V1__events.sql — the event store (ADR-0000 §5).
--
-- The shape of this file is the whole read-performance argument of the system,
-- so it is transcribed from the ADR rather than re-derived:
--
--   * the CloudEvent is stored VERBATIM in `raw jsonb`; every queryable attribute
--     is `GENERATED ALWAYS AS ... STORED` off `raw`, so a projection cannot drift
--     from the payload it claims to describe — there is no second write to get wrong;
--   * the fact table is RANGE-partitioned by month, so retention is a DETACH and
--     never a DELETE, and a time-bounded search prunes whole partitions;
--   * every index below names the query shape it exists for, both as a comment
--     here and as a catalog COMMENT, so an index nobody can name gets dropped.
--
-- Run Flyway with -Duser.timezone=UTC. Partition bounds carry an explicit +00
-- offset because bounds are parsed in the SESSION timezone: a bare date silently
-- shifts every partition by the server offset.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS events;

-- Trigram search is for the small dimension tables ONLY (ADR §0, decision 6).
-- A trigram GIN over the fact table would be a multi-GB index that also slows ingest.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ---------------------------------------------------------------------------
-- Extraction helpers.
--
-- IMMUTABLE and non-throwing, both deliberately. A bare `(raw #>> path)::float8`
-- in a generated column aborts the ENTIRE insert — a whole 500-event batch from
-- cobalt — on one malformed payload from one device. Returning NULL instead makes
-- a bad payload a missing dimension rather than an outage.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION events.jsonb_num(j jsonb, path text[])
RETURNS double precision LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT AS $$
BEGIN RETURN (j #>> path)::double precision;
EXCEPTION WHEN others THEN RETURN NULL; END; $$;

CREATE OR REPLACE FUNCTION events.jsonb_text_array(j jsonb, path text[])
RETURNS text[] LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  IF jsonb_typeof(j #> path) <> 'array' THEN RETURN NULL; END IF;
  RETURN ARRAY(SELECT jsonb_array_elements_text(j #> path));
EXCEPTION WHEN others THEN RETURN NULL; END; $$;

-- Ordered severity. Text alone cannot be range-compared; the rank can, which is
-- what makes "at least warning" an index scan instead of an IN-list.
-- These numbers are the SAME numbers as io.kzonix.kernel.search.Severity. If the
-- two drift, the UI alert filter and partial index (11) disagree on what an alert is.
CREATE OR REPLACE FUNCTION events.severity_rank(s text)
RETURNS smallint LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE lower(s)
    WHEN 'debug' THEN 10 WHEN 'info' THEN 20 WHEN 'notice' THEN 30
    WHEN 'warn' THEN 40 WHEN 'warning' THEN 40 WHEN 'error' THEN 50
    WHEN 'critical' THEN 60 WHEN 'alert' THEN 70 WHEN 'fatal' THEN 80
    WHEN 'emergency' THEN 80 ELSE NULL END::smallint $$;

-- ---------------------------------------------------------------------------
-- Fact table.
-- ---------------------------------------------------------------------------

CREATE TABLE events.cloud_event (
    -- Partition key. NOT generated: text::timestamptz is only STABLE (it depends on
    -- TimeZone) and PostgreSQL forbids generated columns in a partition key anyway.
    -- Written by the ingestion layer from the validated CloudEvent `time`.
    occurred_at        timestamptz NOT NULL,
    event_uid          uuid        NOT NULL DEFAULT gen_random_uuid(),
    ingested_at        timestamptz NOT NULL DEFAULT now(),

    raw                jsonb       NOT NULL,   -- canonical CloudEvent, structure verbatim
    payload_sha256     bytea       NOT NULL,   -- integrity + cross-partition dup detection

    -- Extractions cannot drift from `raw`: they ARE `raw`.
    ce_specversion     text  GENERATED ALWAYS AS (raw ->> 'specversion')     STORED,
    ce_id              text  GENERATED ALWAYS AS (raw ->> 'id')              STORED,
    ce_source          text  GENERATED ALWAYS AS (raw ->> 'source')          STORED,
    ce_type            text  GENERATED ALWAYS AS (raw ->> 'type')            STORED,
    ce_subject         text  GENERATED ALWAYS AS (raw ->> 'subject')         STORED,
    ce_dataschema      text  GENERATED ALWAYS AS (raw ->> 'dataschema')      STORED,
    ce_datacontenttype text  GENERATED ALWAYS AS (raw ->> 'datacontenttype') STORED,
    data               jsonb GENERATED ALWAYS AS (raw -> 'data')             STORED,
    -- The reserved-attribute list is written on ONE line on purpose: unquoted array
    -- elements are whitespace-trimmed, but wrapping this literal is exactly the kind
    -- of edit that silently changes which keys count as extensions. It must stay
    -- identical to io.kzonix.kernel.event.Envelope.ReservedAttributes.
    extensions         jsonb GENERATED ALWAYS AS (raw - '{specversion,id,source,type,subject,time,dataschema,datacontenttype,data,data_base64}'::text[]) STORED,

    -- smart-home dimensions
    device_id     text     GENERATED ALWAYS AS (raw #>> '{data,deviceId}')            STORED,
    room_id       text     GENERATED ALWAYS AS (raw #>> '{data,roomId}')              STORED,
    person_id     text     GENERATED ALWAYS AS (raw #>> '{data,personId}')            STORED,
    site_id       text     GENERATED ALWAYS AS (raw #>> '{data,siteId}')              STORED,
    severity      text     GENERATED ALWAYS AS (lower(raw #>> '{data,severity}'))     STORED,
    severity_rank smallint GENERATED ALWAYS AS
                    (events.severity_rank(raw #>> '{data,severity}'))                 STORED,
    metric_value  double precision GENERATED ALWAYS AS
                    (events.jsonb_num(raw, '{data,value}'))                           STORED,
    tags          text[]   GENERATED ALWAYS AS
                    (events.jsonb_text_array(raw, '{data,tags}'))                     STORED,

    -- Free text. The 2-arg to_tsvector form is MANDATORY: the 1-arg form is only
    -- STABLE and PostgreSQL rejects it in a generated column.
    -- 'simple' for identifiers (device ids, MQTT topics, reverse-DNS types) —
    -- English stemming mangles them. 'english' only for prose.
    search_doc tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple',  coalesce(raw ->> 'type', '')),          'A') ||
        setweight(to_tsvector('simple',  coalesce(raw ->> 'source', '') || ' ' ||
                                         coalesce(raw ->> 'subject', '')),       'B') ||
        setweight(to_tsvector('english', coalesce(raw #>> '{data,message}', '')),'C') ||
        setweight(jsonb_to_tsvector('simple', coalesce(raw -> 'data', '{}'::jsonb),
                                    '["string"]'),                               'D')
    ) STORED,

    CONSTRAINT cloud_event_pk PRIMARY KEY (occurred_at, event_uid),
    CONSTRAINT cloud_event_specversion_ck CHECK (raw ->> 'specversion' = '1.0'),
    CONSTRAINT cloud_event_required_ck
        CHECK (raw ? 'id' AND raw ? 'source' AND raw ? 'type')
) PARTITION BY RANGE (occurred_at);

COMMENT ON TABLE events.cloud_event IS
  'CloudEvents 1.0 fact table. raw is the verbatim event; every other column is derived from it.';
COMMENT ON COLUMN events.cloud_event.occurred_at IS
  'CloudEvents time, validated at ingest. Partition key, so a wrong value files the row in the wrong month.';
COMMENT ON COLUMN events.cloud_event.raw IS
  'Canonical jsonb. NOT byte-verbatim: jsonb drops key order, whitespace and duplicate keys (ADR 12.4).';

-- Monthly partitions. Explicit +00 offsets: bounds are parsed in the SESSION
-- timezone, so a bare date silently shifts every partition by the server offset.
-- Partition maintenance does NOT live here — migrations are versioned and immutable,
-- partitions are a rolling concern. An advisory-locked job in ferrite creates N+3
-- months ahead (ADR §5).
CREATE TABLE events.cloud_event_2026_07 PARTITION OF events.cloud_event
    FOR VALUES FROM ('2026-07-01 00:00:00+00') TO ('2026-08-01 00:00:00+00');
CREATE TABLE events.cloud_event_2026_08 PARTITION OF events.cloud_event
    FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');

-- Safety net for clock-skewed producers. MUST stay empty; alarm on count(*) > 0.
-- Once it holds rows, creating an overlapping partition takes ACCESS EXCLUSIVE and
-- scans it, so the alarm is the cheap moment to act.
CREATE TABLE events.cloud_event_default PARTITION OF events.cloud_event DEFAULT;

COMMENT ON TABLE events.cloud_event_default IS
  'Clock-skew safety net. Must stay empty; partition.default.rows is gauged and alerted on.';

-- ---------------------------------------------------------------------------
-- Indexes — every one names the query it exists for.
-- ---------------------------------------------------------------------------

-- (1) IDEMPOTENT INGEST. Kafka is at-least-once; cobalt replays after a crash.
--     Backs: INSERT ... ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING.
--     Must contain the partition key to be enforceable on a partitioned table.
--     This index IS the dedup contract: CloudEvents guarantees (source, id) is
--     unique per producer, so at-least-once delivery becomes observationally
--     exactly-once at the database and nowhere else.
CREATE UNIQUE INDEX cloud_event_identity_uk
    ON events.cloud_event (occurred_at, ce_source, ce_id);
COMMENT ON INDEX events.cloud_event_identity_uk IS
  'Dedup: INSERT ... ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING on replay.';

-- (2) PRIMARY TIMELINE. Equality key first, sort key second, so ONE index supplies
--     filter + ORDER BY + the keyset range scan.
--     Backs: WHERE ce_type = $1 AND (occurred_at,event_uid) < ($2,$3)
--            ORDER BY occurred_at DESC, event_uid DESC LIMIT 50
CREATE INDEX cloud_event_type_time_ix
    ON events.cloud_event (ce_type, occurred_at DESC, event_uid DESC);
COMMENT ON INDEX events.cloud_event_type_time_ix IS
  'Type timeline: WHERE ce_type = ANY($1) ORDER BY occurred_at DESC, event_uid DESC, keyset seek.';

-- (3) PER-DEVICE DRILLDOWN. Partial: system/aggregate events carry no deviceId and
--     never appear in device views, so they are excluded rather than bloating.
CREATE INDEX cloud_event_device_time_ix
    ON events.cloud_event (device_id, occurred_at DESC, event_uid DESC)
    WHERE device_id IS NOT NULL;
COMMENT ON INDEX events.cloud_event_device_time_ix IS
  'Device drilldown: WHERE device_id = ANY($1) ORDER BY occurred_at DESC, event_uid DESC.';

-- (4) PER-INTEGRATION TIMELINE (one row per CE `source` URI).
CREATE INDEX cloud_event_source_time_ix
    ON events.cloud_event (ce_source, occurred_at DESC, event_uid DESC);
COMMENT ON INDEX events.cloud_event_source_time_ix IS
  'Integration timeline: WHERE ce_source = ANY($1) ORDER BY occurred_at DESC, event_uid DESC.';

-- (5) ROOM / PERSON FACET DRILLDOWN (the two remaining hot dimensions).
CREATE INDEX cloud_event_room_time_ix
    ON events.cloud_event (room_id, occurred_at DESC) WHERE room_id IS NOT NULL;
COMMENT ON INDEX events.cloud_event_room_time_ix IS
  'Room drilldown: WHERE room_id = ANY($1) ORDER BY occurred_at DESC.';

CREATE INDEX cloud_event_person_time_ix
    ON events.cloud_event (person_id, occurred_at DESC) WHERE person_id IS NOT NULL;
COMMENT ON INDEX events.cloud_event_person_time_ix IS
  'Person drilldown: WHERE person_id = ANY($1) ORDER BY occurred_at DESC.';

-- (6) BRIN ON INGEST ORDER. The table is append-only so ingested_at is almost
--     perfectly physically correlated: a few KB replaces a multi-GB btree.
CREATE INDEX cloud_event_ingested_brin
    ON events.cloud_event USING brin (ingested_at)
    WITH (pages_per_range = 32, autosummarize = on);
COMMENT ON INDEX events.cloud_event_ingested_brin IS
  'Ingest-lag dashboards and backfill windows: WHERE ingested_at >= now() - $1.';

-- (7) AD-HOC PAYLOAD SEARCH. jsonb_path_ops, not the default jsonb_ops: about half
--     the size and materially faster for @> and @?, at the cost of ? / ?| / ?&,
--     which the UI does not use on `data`.
CREATE INDEX cloud_event_data_gin
    ON events.cloud_event USING gin (data jsonb_path_ops);
COMMENT ON INDEX events.cloud_event_data_gin IS
  'Payload search: data @> $1::jsonb, and partially data @? $1::jsonpath.';

-- (8) CUSTOM CE EXTENSIONS. jsonb_ops HERE (not path_ops) because extension
--     filtering is key-existence based. `extensions` is tiny, so jsonb_ops costs nothing.
CREATE INDEX cloud_event_extensions_gin
    ON events.cloud_event USING gin (extensions);
COMMENT ON INDEX events.cloud_event_extensions_gin IS
  'Extension filter: jsonb_exists(extensions, $1) and extensions ->> $1 = $2.';

-- (9) TAG FILTER. array_ops for containment.
CREATE INDEX cloud_event_tags_gin
    ON events.cloud_event USING gin (tags array_ops) WHERE tags IS NOT NULL;
COMMENT ON INDEX events.cloud_event_tags_gin IS
  'Tag filter: tags @> $1::text[].';

-- (10) FREE TEXT. The only supported substring-free text path over the fact table;
--      unanchored ILIKE is deliberately not supported here (ADR §0, decision 6).
CREATE INDEX cloud_event_search_gin
    ON events.cloud_event USING gin (search_doc);
COMMENT ON INDEX events.cloud_event_search_gin IS
  'Free text: search_doc @@ websearch_to_tsquery($1::regconfig, $2).';

-- (11) ALERT FEED. Partial over <1% of rows, so the index is orders of magnitude
--      smaller than the table and the "recent alerts" panel is a trivial scan.
CREATE INDEX cloud_event_alerts_ix
    ON events.cloud_event (occurred_at DESC) WHERE severity_rank >= 50;
COMMENT ON INDEX events.cloud_event_alerts_ix IS
  'Alert feed: WHERE severity_rank >= 50 ORDER BY occurred_at DESC LIMIT 20.';

-- (12) TIME-SERIES CHARTING. Partial (rows carrying a metric) with INCLUDE, so the
--      plan is an index-only scan — no heap fetch per plotted point.
CREATE INDEX cloud_event_metric_ix
    ON events.cloud_event (device_id, occurred_at DESC)
    INCLUDE (metric_value) WHERE metric_value IS NOT NULL;
COMMENT ON INDEX events.cloud_event_metric_ix IS
  'Metric chart: WHERE device_id = $1 AND metric_value IS NOT NULL ORDER BY occurred_at DESC.';

-- ---------------------------------------------------------------------------
-- Planner and storage.
-- ---------------------------------------------------------------------------

ALTER TABLE events.cloud_event ALTER COLUMN ce_type   SET STATISTICS 1000;
ALTER TABLE events.cloud_event ALTER COLUMN device_id SET STATISTICS 1000;

-- Append-only tables are never touched by dead-tuple autovacuum; force insert-driven
-- vacuums so visibility maps stay fresh and index-only scans stay index-only.
ALTER TABLE events.cloud_event SET (
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold    = 50000);

-- ---------------------------------------------------------------------------
-- Dimension tables — thousands of rows, and THIS is where pg_trgm belongs.
-- They power filter-bar autocomplete, which then resolves into an EXACT equality
-- filter against the fact table; the fact table never sees a substring predicate.
-- ---------------------------------------------------------------------------

CREATE TABLE events.device (
    device_id   text PRIMARY KEY,
    label       text,
    room_id     text,
    first_seen  timestamptz NOT NULL DEFAULT now(),
    last_seen   timestamptz NOT NULL DEFAULT now(),
    event_count bigint      NOT NULL DEFAULT 0);
CREATE INDEX device_label_trgm_ix ON events.device USING gin (label gin_trgm_ops);
CREATE INDEX device_id_trgm_ix    ON events.device USING gin (device_id gin_trgm_ops);
COMMENT ON INDEX events.device_label_trgm_ix IS
  'Autocomplete: WHERE label ILIKE $1 over the dimension, never over the fact table.';
COMMENT ON INDEX events.device_id_trgm_ix IS
  'Autocomplete: WHERE device_id ILIKE $1 over the dimension, never over the fact table.';

CREATE TABLE events.dim_event_type (
    ce_type text PRIMARY KEY, last_seen timestamptz NOT NULL DEFAULT now());
CREATE INDEX dim_event_type_trgm_ix ON events.dim_event_type USING gin (ce_type gin_trgm_ops);
COMMENT ON INDEX events.dim_event_type_trgm_ix IS
  'Autocomplete: WHERE ce_type ILIKE $1.';

CREATE TABLE events.room (
    room_id text PRIMARY KEY, label text, last_seen timestamptz NOT NULL DEFAULT now());
CREATE INDEX room_label_trgm_ix ON events.room USING gin (label gin_trgm_ops);
COMMENT ON INDEX events.room_label_trgm_ix IS
  'Autocomplete: WHERE label ILIKE $1.';

CREATE TABLE events.person (
    person_id text PRIMARY KEY, label text, last_seen timestamptz NOT NULL DEFAULT now());
CREATE INDEX person_label_trgm_ix ON events.person USING gin (label gin_trgm_ops);
COMMENT ON INDEX events.person_label_trgm_ix IS
  'Autocomplete: WHERE label ILIKE $1.';

-- ---------------------------------------------------------------------------
-- Hourly rollup. Dashboards and the landing-page histogram never scan the fact
-- table. A materialized view rather than consumer-maintained counters (ADR §0,
-- decision 7): counters incremented by an at-least-once consumer drift on
-- redelivery, the MV is idempotent and authoritative.
--
-- The UNIQUE index is what makes REFRESH ... CONCURRENTLY legal — without it the
-- refresh takes an ACCESS EXCLUSIVE lock and the dashboard blocks.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW events.event_rollup_hourly AS
SELECT date_trunc('hour', occurred_at) AS bucket, ce_type, ce_source,
       coalesce(severity, 'none') AS severity,
       count(*)                                        AS event_count,
       count(*) FILTER (WHERE severity_rank >= 50)     AS error_count,
       count(DISTINCT device_id)                       AS device_count,
       avg(metric_value) AS avg_value, min(metric_value) AS min_value,
       max(metric_value) AS max_value
FROM events.cloud_event
WHERE occurred_at >= now() - interval '90 days'
GROUP BY 1,2,3,4
WITH NO DATA;

CREATE UNIQUE INDEX event_rollup_hourly_uk
    ON events.event_rollup_hourly (bucket, ce_type, ce_source, severity);
COMMENT ON INDEX events.event_rollup_hourly_uk IS
  'Makes REFRESH MATERIALIZED VIEW CONCURRENTLY legal; also the rollup lookup key.';

CREATE INDEX event_rollup_hourly_bucket_ix
    ON events.event_rollup_hourly (bucket DESC);
COMMENT ON INDEX events.event_rollup_hourly_bucket_ix IS
  'Landing-page histogram: WHERE bucket >= $1 ORDER BY bucket DESC.';

REFRESH MATERIALIZED VIEW events.event_rollup_hourly;

-- ---------------------------------------------------------------------------
-- Saved searches and long permalinks: the filter AST as jsonb, keyed by content
-- hash. Only filters too long for a querystring land here; short ones stay in the
-- URL where they remain readable and hand-editable (ADR §6.3).
-- ---------------------------------------------------------------------------

CREATE TABLE events.saved_search (
    slug       text PRIMARY KEY,                 -- base32(sha256(ast))[0,12]
    ast        jsonb       NOT NULL,
    label      text,
    created_at timestamptz NOT NULL DEFAULT now());

COMMENT ON TABLE events.saved_search IS
  'Content-addressed filter ASTs behind ?s=<slug>. Immutable by construction: the key is the hash.';
