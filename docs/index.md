# playground event observatory

An event observatory for smart-home and IoT telemetry. It ingests [CloudEvents](https://cloudevents.io/) over HTTP,
streams them through Kafka, stores them in PostgreSQL, and serves a fast server-rendered UI for exploring,
searching and monitoring them.

The design goal is closer to Grafana, Kibana or Home Assistant than to a CRUD application: the event log is the
product, and search over it is the primary feature.

## The three services

| Service | Stack | Responsibility |
| --- | --- | --- |
| **wolfram** | Tapir on Vert.x 5 | HTTP ingestion. Validates CloudEvents and publishes to Kafka. Owns no state. |
| **cobalt** | Pekko Streams Kafka | Consumes, decodes and persists events. Cask serves only its metrics and health. |
| **ferrite** | Play 3 + Twirl/HTMX | The web application: PostgreSQL, search, and the UI. Never sees Kafka. |

Named after metals rather than their frameworks, so a name does not have to change if a stack does.

## Shared libraries

`applications/` holds exactly those three deployable services. The shared contracts live in `modules/`, as
libraries with no `main` and no image:

- **kernel** — the domain. The CloudEvents envelope, the observation ADT and the search filter grammar. Depends on
  circe and the standard library and *nothing else*; a build-load assertion fails the build if that ever changes.
  This is what keeps the dependency arrows pointing inward.
- **eventing** — the only place that knows both the domain envelope and the Kafka wire format, so the producer and
  the consumer cannot disagree about the encoding.
- **persistence** — the schema, the connection pool, and the compiler from a search filter to parameterised SQL.
- **observability** — one metric vocabulary and one tracing setup, shared so dashboards written against one
  service work against the others.

## Design commitments

**CloudEvents are the source of truth.** Events are stored verbatim as `jsonb`; every queryable column is
`GENERATED ALWAYS AS … STORED` from that raw document, so a projection cannot drift from the payload it describes.
An event type this system has never seen is still persisted, still searchable, and still viewable.

**Search is pure PostgreSQL.** JSONB with GIN, BRIN on time, partial indexes and a rollup materialized view —
no second datastore. The operational cost of running a search cluster alongside the database is real, and the
query shapes here do not need one.

**At-least-once, made idempotent.** The consumer commits only after a durable write, and the write deduplicates on
the CloudEvents identity, so a redelivery is a no-op rather than a duplicate row.

**One trace, end to end.** A W3C trace context is injected into Kafka headers at ingestion and extracted by the
consumer, so a single trace spans HTTP → Kafka → database.

## Further reading

- [Architecture decision record](adr/0000-architecture.md) — the full contract: dependency table, schema DDL,
  index rationale, and the risks with their fallbacks.
- [Scaladoc](api/index.html) — generated API documentation for every module.
