# Flows

Six behaviours that are hard to read off the source because the interesting part is an *ordering* — which call happens
before which, and what breaks if they swap. Everything here is drawn from the code; where a diagram and the prose
beside it disagree, the code is at `applications/…` and the code wins.

Structure lives elsewhere: [the ADR](../adr/0000-architecture.md) has the component map, and the per-service pages have
the module layouts. This page is only about time.

---

## 1. Ingest, happy path

**What question this answers:** between the producer's `POST` and its `200`, what does wolfram decide, in what order,
and at what point does the trace context get attached to the record?

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant A as IngestApi<br/>Vert.x event loop
    participant V as JwtVerifier
    participant S as IngestionService
    participant K as KafkaEventPublisher
    participant T as sender thread<br/>1 thread, bounded queue
    participant B as Kafka<br/>events.cloudevents.v1

    P->>A: POST /v1/events<br/>Authorization Bearer, ce-* headers or JSON body
    A->>V: verify token, require scope events:write
    V-->>A: Principal
    Note over A,V: applied once on the shared `base` endpoint,<br/>so no route can exist without it

    A->>S: ingest(headers, body)
    Note over S: in this order, and the order matters:<br/>1. size vs max-event-bytes, refused before anything is decoded<br/>2. HttpBinding.modeOf — ce-specversion first, media type second<br/>3. Envelope.decoder — kernel's codec, identical for both modes<br/>4. TimeClamp.check — reject an implausible or absent time, never invent one

    S->>K: publish(envelope)
    Note over K: KafkaCodecs.producerRecord — binary mode, key = Envelope.partitionKey.<br/>Context.current() is read HERE, on the calling thread, and carried<br/>across the hand-off: OTel context is ThreadLocal, so reading it on<br/>the sender thread would return root and orphan the produce span.
    K->>T: sender.execute — enqueue

    alt queue full
        T--)K: RejectedExecutionException
        K-->>S: BrokerUnavailable
        S-->>P: 503 UNAVAILABLE, retryable, nothing published
    else accepted
        Note over T: start PRODUCER span, parented to the captured context
        T->>T: KafkaTrace.inject — traceparent into the record headers
        T->>B: producer.send, acks=all, enable.idempotence=true
        B-->>T: RecordMetadata, partition and offset
        Note over T: span ends in the broker callback, not when send returns —<br/>so the span and kafka.produce.latency measure acknowledgement
        T-->>S: PublishAck
        Note over S: metrics.accepted and metrics.observed fire AFTER the ack,<br/>so a refused event never reports a producer problem
        S-->>P: 200 with the created event, name = events/{event}
    end
```

`send` is the reason for the sender thread. `KafkaProducer.send` blocks while topic metadata is unknown or the record
accumulator is full — up to `max.block.ms` — and on a Vert.x event loop that is not a latency problem but an
availability one, because the loop serves every other connection too. One thread keeps per-key ordering; the bounded
queue is where backpressure becomes a 503 instead of a heap.

**200 and not 201.** AIP-133 wants a Create to return the resource, and 201 would oblige a `Location` pointing at a
`GET /v1/events/{event}` this service cannot serve — it owns no storage.

---

## 2. Consume and persist

**What question this answers:** when is an event durable, and when is its Kafka offset allowed to move past it?

The answer is the whole correctness story of the pipeline, so read the diagram bottom-up: the committer is the last
stage, downstream of the write, and nothing else in the graph can reach it first.

```mermaid
sequenceDiagram
    autonumber
    participant B as Kafka
    participant C as committableSource
    participant D as RecordDecoder
    participant P as BatchProcessor
    participant R as PostgresEventRepository
    participant G as PostgreSQL
    participant M as Committer.flow

    B->>C: ConsumerRecord, ByteArrayDeserializer
    Note over C: never CloudEventDeserializer — a throwing deserializer throws<br/>inside poll(), before the connector sees the record,<br/>so every restart replays the same poison pill
    C->>D: CommittableMessage
    Note over D: KafkaTrace.withConsumerSpan — extract the traceparent<br/>wolfram injected, open a CONSUMER span under it
    D->>D: KafkaCodecs.decode then NewEvent.from
    D-->>C: DecodedRecord = record + committableOffset + Either

    C->>C: groupedWithin(500, 250 millis)
    C->>P: mapAsync(1) process(batch)
    Note over P: mapAsync(1), not (n): two batches from one partition in flight<br/>would let the younger one's offset commit past the older one's failure

    P->>R: insertAllCheckpointed(events, commit)
    R->>G: BEGIN
    R->>G: INSERT INTO events.cloud_event ... ON CONFLICT<br/>(occurred_at, ce_source, ce_id) DO NOTHING
    R->>G: INSERT INTO events.consumer_checkpoint ... ON CONFLICT DO UPDATE<br/>guarded so a late write cannot rewind next_offset
    R->>G: COMMIT
    Note over R,G: ONE transaction. The rows and the offset that accounts for them<br/>commit together or not at all — that is the entire reason<br/>events.consumer_checkpoint exists rather than a Redis key.
    G-->>R: rows written
    Note over P: batch.size minus written is consume.records.duplicate —<br/>the direct evidence that redelivery is being absorbed

    P-->>C: Vector[Committable]
    C->>M: mapConcat, then the committer
    M->>B: commit offsets
    Note over M,B: strictly downstream of the write. An offset cannot physically<br/>reach here until the transaction above committed, so an offset<br/>is a receipt for a durable effect.
```

Swap the last two stages — commit first, or commit inside the write stage's `andThen` — and a crash in the window
between them loses every event in flight, silently, with the consumer group reporting zero lag.

The write is idempotent, so at-least-once redelivery plus CloudEvents' own `(source, id)` uniqueness is
observationally exactly-once *at the database and nowhere else*. See
[the dedup contract](../data/schema.md#the-dedup-contract).

The checkpoint statement is drawn unconditionally because production wires it unconditionally — `Main` builds the
processor with a `BatchProcessor.Checkpointing` carrying the group id and the container's hostname as `owner`. Without
one, `BatchProcessor` falls back to a plain `insertAll` and is *silent about offsets* rather than writing them in a
second transaction, which would reintroduce exactly the window `events.consumer_checkpoint` removes while looking like
it worked. That fallback exists for the suites that have no store.

---

## 3. A poison record

**What question this answers:** a record that cannot be decoded blocks nothing — so what actually happens to it, and
why does its offset move even though it was never persisted?

```mermaid
sequenceDiagram
    autonumber
    participant B as Kafka<br/>events.cloudevents.v1
    participant D as RecordDecoder
    participant P as BatchProcessor
    participant Q as Kafka<br/>events.cloudevents.v1.dlq
    participant G as PostgreSQL
    participant M as Committer.flow

    B->>D: one batch, one record undecodable
    Note over D: total by construction — unknown content mode, malformed JSON,<br/>a missing attribute, no time, or any NonFatal throw<br/>all become Left(DeadLetter)
    D-->>P: DecodedRecord with Left(DeadLetter) beside the good ones

    P->>P: partitionMap: poison, pending
    Note over P: dead letters go FIRST, one at a time.<br/>A fan-out of produces at a broker that may itself be the problem<br/>is how a poison batch becomes an outage.
    P->>Q: publish, structured mode,<br/>key = Topics.dlqKey(topic, partition, offset)
    Note over Q: the dead letter is itself a CloudEvent, carrying reason, detail,<br/>the origin coordinates and the original value bytes and headers verbatim
    Q-->>P: ack

    P->>G: the rest of the batch, ON CONFLICT DO NOTHING
    G-->>P: rows written
    P-->>M: committables for the WHOLE batch, poison record included
    M->>B: commit
    Note over M,B: the one case where an offset moves past an event that was<br/>never persisted. Defensible only because the DLQ record is durable<br/>and carries why, where and what.
```

A record the *database* rejects follows the same road by a longer route. `BatchProcessor` retries the whole batch
first — a database blip is not a data problem — then bisects, halving until the failure is attributed to one record,
and dead-letters that one. `log₂(500) ≈ 9` extra round trips buys never losing a good event and never stalling. The
bisection is only allowed to run on SQLSTATE class `22` or `23`; anything else is rethrown, because a database that is
merely *down* would otherwise bisect to singletons and shovel the whole batch into the DLQ.

### The replay

**What question this answers:** an operator has a DLQ full of records from an incident that is now fixed. What do the
two requests look like, and why is running the second one twice safe?

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator
    participant L as cobalt /admin<br/>DeadLetterAdmin
    participant S as DeadLetterStore
    participant Q as Kafka DLQ
    participant B as Kafka main topic
    participant C as cobalt consumer
    participant G as PostgreSQL

    O->>L: POST /admin/dlq:replay?limit=50<br/>Bearer with the write scope
    Note over L: dryRun defaults to TRUE at the route, so the shape that<br/>publishes takes one extra deliberate keystroke
    L->>S: recent(fetchLimit)
    S->>Q: poll newest-first
    Q-->>S: DlqRecords
    S-->>L: Vector[DlqRecord]
    Note over L: DeadLetterReplay.plan classifies every candidate:<br/>Undecodable, ForeignTopic, BudgetExhausted, or Replay(attempt = n+1)
    L-->>O: 200 with the full plan. Nothing was published.

    O->>L: POST /admin/dlq:replay?limit=50&dryRun=false
    L->>S: recent, plan again
    alt named refs, and one is missing or skipped
        L-->>O: 422. Nothing published — a stated set is all-or-nothing.
    else
        loop oldest-first, stopping at the first refusal
            Note over L: producerRecord copies the ORIGINAL bytes, headers and key.<br/>Nothing is re-encoded, so the CloudEvents id and source survive.<br/>Two plain transport headers are added: replay-attempt and replay-of.
            L->>S: publish onto the dead letter's own origin topic
            S->>B: produce
        end
        L-->>O: 200 with published count, skipped, and where it stopped
    end

    B->>C: the replayed record, indistinguishable from the original
    C->>G: INSERT ... ON CONFLICT (occurred_at, ce_source, ce_id) DO NOTHING
    Note over C,G: a record that DID land the first time is absorbed here.<br/>Replay is not idempotent at the broker — two copies on the log —<br/>and is idempotent at the database, which is what makes<br/>retrying a half-finished replay safe.
```

Three bounds are worth naming because they are what keep this endpoint from being a general-purpose producer:
`ForeignTopic` refuses a dead letter whose origin topic is not the one this consumer owns (the destination is read out
of the record's own payload); `max-records` caps one operation; and `ReplayHeaders.Attempt` caps the poison loop at
`max-attempts` generations, because a record that fails again lands back on the DLQ under a *new* key and compaction
does nothing to stop it accumulating.

---

## 4. A search request

**What question this answers:** how many database round trips does one search page cost, and are they serial?

Four queries, and they are **started before any of them is awaited**. A `for` comprehension over `Future`s would
sequence them and turn one 40 ms page into four serial round trips — the classic way to make a fast page slow without
anyone noticing. `SearchService` starts them as `val`s and only then comprehends.

```mermaid
sequenceDiagram
    autonumber
    participant U as Browser<br/>htmx
    participant E as EventsController
    participant S as SearchService
    participant R as PostgresEventRepository<br/>read pool, 2s statement_timeout
    participant V as Presenter and Twirl

    U->>E: GET /events?type=...&from=...<br/>HX-Request when it is a swap
    E->>E: SearchQuery.parse(rawQueryString)
    alt the query string does not parse
        E->>V: filter bar rebuilt from the rejected permalink, plus the errors
        V-->>U: 4xx fragment or page, every bad value still in its input
    else a fragment request carrying a cursor
        Note over E: paging. No facets, no histogram, no count —<br/>the filter has not changed, so neither have they.
        E->>S: page(query)
        S->>R: one keyset SELECT
        R-->>S: rows plus nextCursor
        V-->>U: rows fragment plus a fresh sentinel
    else
        E->>S: search(query)
        par all four issued, none awaited
            S->>R: search — keyset page, list projection, no data or raw
        and
            S->>R: facets — one statement, one MATERIALIZED candidate CTE
        and
            S->>R: histogram — generate_series skeleton, LEFT JOIN date_bin
        and
            S->>R: countAtMost — SELECT 1 inside a LIMIT 10001 subquery
        end
        R-->>S: four results
        Note over S: only NOW a for-comprehension joins them, plus<br/>andThen so the timer records the failed search too
        S-->>E: SearchOutcome
        E->>V: Presenter.filterBar and Presenter.results
        V-->>U: full page, or the results fragment plus HX-Push-Url
    end
    Note over E,U: every response carries Vary HX-Request, error responses included
```

The pool matters to reading this. The repository was constructed with a `SearchExecutionContext` whose fixed pool size
equals the read pool's `maximumPoolSize`, so "four concurrent" means four of eight connections for the duration of one
page — which is also why the live tail below has a hard concurrency cap.

---

## 5. The live tail

**What question this answers:** the browser opens one `EventSource` and rows appear. What is on the server for that
hour, and what stops a slow client from growing a buffer in the heap?

```mermaid
sequenceDiagram
    autonumber
    participant U as Browser EventSource
    participant T as TailController
    participant V as TailService
    participant R as PostgresEventRepository
    participant W as Twirl fragment

    U->>T: GET /live?filter...&after=...&afterUid=...
    Note over T: after and afterUid are peeled off BEFORE the filter is parsed.<br/>FilterQuery owns the grammar and reports anything else as unknown,<br/>so leaving them in would 400 every stream.
    alt no capacity
        T-->>U: 503 naming the reason. A capacity limit on this replica, not a quota.
    else admitted
        T-->>U: 200 chunked, Cache-Control no-store, X-Accel-Buffering no

        Note over T,V: everything below runs inside lazyFutureSource, at body materialisation —<br/>which is also the only thing that can terminate and release the slot
        T->>V: opened()
        opt no client cursor
            V->>R: newest row matching the filter, LIMIT 1
            R-->>V: seed cursor
            Note over V: starting at the wall clock instead would silently skip<br/>every event whose occurred_at lags ingest by a few seconds
        end

        loop Source.tick every 2 seconds, threaded by scanAsync
            V->>R: keyset seek: (occurred_at, event_uid) greater than the cursor,<br/>ORDER BY occurred_at ASC, LIMIT 50
            R-->>V: rows, oldest first
            Note over V: ASCENDING even though the page renders newest-first.<br/>A descending query capped at 50 would skip everything between<br/>the newest 50 and the cursor — silent loss during the burst<br/>somebody opened the tail to watch.
            V->>W: Presenter.row, the same template search uses
            W-->>T: one tr per row
            alt rows
                T-->>U: one SSE frame per row, event row, id = event_uid
            else nothing new
                T-->>U: heartbeat frame
            end
        end

        U--)T: tab closed
        T->>V: closed() from watchTermination
    end
```

Three bounds, and the third is the one people miss. `scanAsync` runs one future at a time, so ticks cannot overlap;
`Source.tick` *drops* a tick the downstream is not ready for rather than queueing it, so a slow client falls behind in
time and never in the server's heap; and `MaxConcurrent` refuses the seventeenth tail, because sixteen forgotten tabs
against a pool of eight connections make *search* slow for everyone, which is a failure whose cause is invisible from
the page that is failing.

The honest limitation: `occurred_at` is the producer's clock, so an event ingested now but stamped behind the cursor
sorts behind it and never appears. Ordering on `ingested_at` has only a BRIN index, which cannot serve an ordered seek.

---

## 6. A consumer restart with custom offsets

**What question this answers:** why does the supervisor insist on draining before it moves the group's offsets, when
draining costs a rebalance?

Because Kafka refuses `alterConsumerGroupOffsets` while the group has live members — and it is right to. An offset
moved under a running consumer would be overwritten by that consumer's next commit, so the call would appear to
succeed and change nothing.

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator
    participant A as SupervisorAdmin
    participant S as ConsumerSupervisor
    participant H as the running stream
    participant K as Kafka AdminClient
    participant P as PostgreSQL<br/>consumer_checkpoint

    O->>A: POST /admin/consumer:restart?target=stored
    Note over A: dryRun defaults to TRUE, matching the DLQ replay —<br/>one convention for both irreversible operations
    A->>S: status
    S->>P: load(groupId)
    P-->>S: stored next_offsets
    A-->>O: 200 wouldSeek. Nothing stopped, nothing moved.

    O->>A: POST /admin/consumer:restart?target=stored&dryRun=false
    A->>S: restart(target, explicit)
    Note over S: everything below is inside one synchronized transition.<br/>status never takes that lock, so an operator can still ask<br/>what is happening WHILE a slow drain happens.

    S->>P: resolve target=stored
    P-->>S: concrete offsets per partition
    Note over S: resolve runs BEFORE the drain, so a target that cannot be<br/>resolved refuses without having stopped anything

    S->>H: drain, bounded by DrainTimeout
    Note over H,K: drain commits everything in flight and LEAVES THE GROUP.<br/>Pause is implemented the same way — Consumer.Control has no pause,<br/>and holding partitions assigned while not committing<br/>only delays the rebalance unpredictably.
    H-->>S: done, state Stopped

    S->>K: alterConsumerGroupOffsets(groupId, offsets)
    alt the group still had members
        K--)S: refused
        Note over S: state stays Stopped and the response is 409.<br/>Starting against offsets nobody chose cannot be undone.
        S-->>O: 409 with the cause
    else moved
        K-->>S: ok
        S->>H: factory.start — a fresh stream, generation + 1
        Note over H: a drained stream is not restartable —<br/>resume always materialises a new one
        S-->>O: 200 with the offsets it actually set
    end
```

`target=stored` reads `events.consumer_checkpoint`, not Kafka. That table exists because
`offsets.retention.minutes` defaults to seven days: a group that stops committing for longer has its offsets *deleted*,
and the next start resolves `auto.offset.reset` and replays the retained log, silently. `:clearCheckpoints` forgets
that table and deliberately does **not** touch `__consumer_offsets` — conflating the two would make one word mean two
irreversible things.

---

See also [the event model](../event-model.md) for what travels on the wire, and
[the schema](../data/schema.md) for what the writes land in.
