-- Externalised consumer offsets.
--
-- WHY THIS EXISTS WHEN KAFKA ALREADY STORES OFFSETS.
--
-- Kafka's __consumer_offsets is authoritative for *fetching* and this table does not
-- replace it. It answers two questions Kafka's own storage cannot:
--
--   1. `offsets.retention.minutes` defaults to seven days. A consumer group that stops
--      committing for longer than that has its offsets DELETED, and the next start
--      resolves `auto.offset.reset=earliest` and replays the entire retained log. For a
--      pipeline whose whole correctness story is "at-least-once made idempotent", that
--      is survivable but expensive, and it is completely silent. A checkpoint here
--      outlives the broker's retention.
--
--   2. It is written in the SAME transaction as the batch insert. The offset and the
--      rows it accounts for therefore commit or roll back together, which is the only
--      arrangement in which "what was the last durably stored offset" has an exact
--      answer. Kafka's commit is a separate round trip after the write, and the window
--      between them is precisely the replay window.
--
-- WHY POSTGRESQL AND NOT REDIS OR CASSANDRA.
--
-- The requirement was an external store, and the honest answer for this system is the
-- database that is already here. Redis would be a second datastore to run, back up and
-- monitor, and — being non-transactional with respect to the event insert — it would
-- give up property 2 above, which is the only reason worth doing this at all. Cassandra
-- would add an operational surface larger than the entire rest of the stack for a table
-- that holds one row per partition. Postgres is already a hard dependency of the write
-- path: if it is down, cobalt is not consuming anyway, so this adds no new failure mode.

CREATE TABLE IF NOT EXISTS events.consumer_checkpoint (
    -- The natural key IS the Kafka coordinate. No surrogate: there is exactly one
    -- checkpoint per (group, topic, partition) and a surrogate id would permit two.
    group_id        text        NOT NULL,
    topic           text        NOT NULL,
    partition       integer     NOT NULL,

    -- The offset of the NEXT record to read, matching Kafka's own commit convention
    -- (committed offset = last processed + 1). Storing "last processed" instead would
    -- read identically and be off by one at every seek, in the direction that
    -- reprocesses a record — which is safe here and still wrong.
    next_offset     bigint      NOT NULL CHECK (next_offset >= 0),

    -- How many records this checkpoint accounts for, cumulative. Diagnostic, and the
    -- cheapest way to tell "this partition is idle" from "this partition is stuck":
    -- both have a static next_offset, only one has a static count.
    records         bigint      NOT NULL DEFAULT 0 CHECK (records >= 0),

    -- Which process wrote it. A rebalance moves a partition between replicas, and when
    -- a checkpoint stops advancing this is the first thing worth knowing.
    owner           text,

    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT consumer_checkpoint_pkey PRIMARY KEY (group_id, topic, partition)
);

COMMENT ON TABLE events.consumer_checkpoint IS
    'Externalised Kafka consumer offsets, written in the same transaction as the batch '
    'insert. Survives offsets.retention.minutes expiry and makes the last durably '
    'stored offset exactly answerable. Kafka remains authoritative for fetching.';

COMMENT ON COLUMN events.consumer_checkpoint.next_offset IS
    'Offset of the next record to read — Kafka''s commit convention, not the last '
    'processed offset.';

-- Serves the supervisor''s "show me every checkpoint for this group, newest activity
-- first" read, which is one query per admin request and never on the hot path. The
-- primary key already covers lookups by exact coordinate.
CREATE INDEX IF NOT EXISTS consumer_checkpoint_group_updated_idx
    ON events.consumer_checkpoint (group_id, updated_at DESC);

COMMENT ON INDEX events.consumer_checkpoint_group_updated_idx IS
    'GET /admin/consumer — checkpoints for one group, ordered by recency of activity.';
