# Design Decisions & Tradeoffs

This document explains *why* the system is built the way it is — the kind of reasoning you'd walk through in a system-design discussion. The guiding constraint throughout: **money must never be charged twice, and no accepted payment may be silently lost.**

---

## 1. Why event-driven instead of synchronous?

A naive payment endpoint does everything in the request path: validate → write to DB → send receipt → respond. That couples the client's latency to the slowest dependency and makes the write path fragile (if the email server is slow, the customer waits).

Instead, the API does the minimum synchronously and hands the rest off:

```
POST /charges  →  dedupe (Redis)  →  publish event (Kafka)  →  202 Accepted
                                              │
                                   (async, out of request path)
                                              ▼
                              Worker → persist (Postgres) → notify
```

**Tradeoff:** the client gets `202 Accepted` ("we'll process this"), *not* `201 Created`. The charge isn't durably written when the API responds — it's durably *queued*. This is the standard model for high-throughput payment systems (it's why Stripe charges are asynchronous and you poll/await webhooks). The price is eventual consistency on the read side; the benefit is a fast, resilient write path that scales independently of the database.

---

## 2. Idempotency: why Redis *and* a DB unique constraint?

Duplicate submissions are the central correctness problem. We defend in **two layers**:

| Layer | Mechanism | Catches |
|-------|-----------|---------|
| **Edge** | Redis `SET key` with 24h TTL | The common case — client retries, double-clicks. Rejected in microseconds with `409`, before any work. |
| **Storage** | `UNIQUE` constraint on `charges.idempotency_key` | The race/failure case — two requests slip past Redis concurrently, or Redis is flushed. The database is the final arbiter. |

**Why not just Redis?** Redis is a cache — it can be evicted or lost. Using it alone would allow a double-charge after a Redis restart.

**Why not just the DB constraint?** That works, but every duplicate would travel all the way to the worker and the database before being rejected — wasteful, and it doesn't give the client a fast `409`.

The layering gives **speed (Redis) + correctness (Postgres)**. Redis is an optimization; Postgres is the source of truth.

---

## 3. Delivery semantics: at-least-once

Kafka with consumer groups gives **at-least-once** delivery: if the worker crashes after processing but before committing its offset, the message is redelivered. That means **the worker must be idempotent**, which it is — a redelivered message hits the `UNIQUE` constraint and is rejected rather than creating a second charge.

We deliberately do *not* attempt exactly-once (which would require transactional Kafka + DB writes via the outbox/2PC pattern). At-least-once + an idempotent consumer is simpler and achieves the same business guarantee.

---

## 4. Failure handling

### Notification is best-effort
Once a charge is **persisted**, it is the source of truth. The email receipt is a side effect. So `process_payment` commits the charge first, then attempts the webhook with **exponential backoff** (`1s → 2s → 4s`). If all retries fail, we **log and move on** — we do *not* fail the message. Re-processing it would re-attempt the DB insert and violate the unique key. A failed receipt is a degraded experience, not a correctness bug.

### Processing failure → Dead Letter Queue
If the charge itself can't be processed — malformed message (missing field), or the DB is down — the message is **not dropped**. It's published to a dead-letter topic (`payments.DLQ`) with the original payload and the error. This means:
- A poison message can't block the partition forever.
- Failed payments are inspectable and replayable, not silently lost.

### API boot resilience
If Kafka is unreachable at startup, the API **boots anyway** rather than crash-looping. `/readiness` then reports `503` (so the load balancer holds traffic) and `create_charge` returns `503`, until Kafka recovers. Crashing wouldn't fix Kafka; it would just produce a restart loop.

---

## 5. Why these technologies?

| Choice | Alternative considered | Why this one |
|--------|------------------------|--------------|
| **Kafka** | RabbitMQ, SQS | Durable, replayable log; consumer groups for horizontal scaling; partitions preserve per-key ordering. RabbitMQ is great for task queues but Kafka's retention/replay fits an event log better. |
| **Redis** | DB lookup for every request | Sub-millisecond dedup without touching Postgres on the hot path. |
| **PostgreSQL** | NoSQL | Financial data wants ACID transactions and constraints (the unique key *is* our safety net). |
| **FastAPI / async** | Flask, Django | Non-blocking I/O matches an I/O-bound workload (network + DB) and gives free OpenAPI docs + Pydantic validation. |

---

## 6. How this scales

- **API**: stateless — scale horizontally behind a load balancer.
- **Worker**: scale by adding consumers to the `payment-processors` group; Kafka rebalances partitions across them. Throughput is bounded by partition count, so the `payments` topic would be partitioned by `user_id` in production (also preserving per-user ordering).
- **Postgres**: the eventual bottleneck — addressed with read replicas for the `GET /charges` path and connection pooling.
- **Redis**: cluster mode; keys are independent so it shards cleanly.

---

## 7. Known limitations / next steps

- **DLQ replay tooling** — messages land in the DLQ but there's no automated replay/inspection consumer yet. Replay must be made idempotency-aware (upsert or pre-check) to avoid unique-key collisions.
- **Outbox pattern** — for true exactly-once between the DB commit and the notification, a transactional outbox would be the next step.
- **AuthN/AuthZ** — a single static API key is a stand-in for real per-merchant key management / OAuth.
- **Schema migrations** — tables are created from models at startup; a production system would use Alembic.
- **Distributed tracing** — metrics and structured logs exist; OpenTelemetry trace propagation across the API → Kafka → worker → notifier hops would close the observability loop.
