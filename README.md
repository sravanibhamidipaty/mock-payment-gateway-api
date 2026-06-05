# 🚀 Mock Payment Gateway API

An **event-driven, asynchronous payment processing system** built with Python and FastAPI — designed to mirror the architecture real payment processors (Stripe, Adyen) use to handle money reliably at scale.

This isn't a CRUD app. It demonstrates the patterns that matter when correctness is non-negotiable: **idempotency**, **decoupled async processing via a message broker**, **at-least-once delivery with a background worker**, and **service-to-service webhooks** — all containerized and covered by CI.

[![API Tests](https://github.com/sravanibhamidipaty/mock-payment-gateway-api/actions/workflows/tests.yml/badge.svg)](https://github.com/sravanibhamidipaty/mock-payment-gateway-api/actions/workflows/tests.yml)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688)
![Kafka](https://img.shields.io/badge/Kafka-event--driven-231F20)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## 🧠 Why This Project Is Interesting

Payment APIs have a hard requirement: **a single click must never charge a customer twice**, even if the network retries, the client double-submits, or a service crashes mid-request. Naively writing to a database doesn't solve this.

This system solves it with a layered design:

1. **Accept fast, process later.** The API validates the request, deduplicates it, drops it onto a Kafka topic, and instantly returns `202 Accepted`. The user never waits on the database.
2. **Deduplicate at the edge.** Redis holds every `Idempotency-Key` for 24h, so duplicate requests are rejected in microseconds with `409 Conflict` — before any work happens.
3. **Process durably.** A dedicated background worker consumes the Kafka topic, writes the charge to PostgreSQL in a transaction, and only then fires a webhook to the notification service.
4. **Notify out-of-band.** A separate microservice receives the webhook and sends a real email receipt over SMTP.

This is the same **CQRS-flavored, write-behind** pattern used in high-throughput financial systems: the write path is cheap and fast, the heavy lifting is async and horizontally scalable.

---

## 🏗️ System Architecture

```mermaid
flowchart LR
    Client([Client]) -->|POST /charges| API[FastAPI<br/>API Gateway]
    API -->|dedupe check| Redis[(Redis<br/>Idempotency)]
    API -->|publish event| Kafka{{Kafka<br/>'payments' topic}}
    API -.->|202 Accepted| Client
    Kafka -->|consume| Worker[Background<br/>Worker]
    Worker -->|persist charge| DB[(PostgreSQL)]
    Worker -->|webhook + retry/backoff| Notifier[Notifier<br/>Microservice]
    Worker -.->|unprocessable| DLQ{{Dead Letter<br/>Queue}}
    Notifier -->|SMTP| Email([📧 Email Receipt])
```

**Request lifecycle:**

```
1. Client POSTs a charge with an Idempotency-Key
2. API checks Redis      → key seen before? → 409 Conflict
3. API publishes event   → Kafka 'payments' topic
4. API stores key in Redis (TTL 24h) and returns 202 Accepted
5. Worker consumes event → writes Charge row to PostgreSQL
6. Worker fires webhook  → Notifier service
7. Notifier sends email  → SMTP receipt
```

---

## 📐 UML Diagrams

### Component Diagram

```mermaid
flowchart TB
    subgraph API_Service["API Service (main.py)"]
        AppA[FastAPI app]
        Auth[verify_api_key dependency]
        Prod[AIOKafkaProducer]
    end
    subgraph Worker_Service["Worker Service (worker.py)"]
        Cons[AIOKafkaConsumer]
        Persist[Charge persistence]
    end
    subgraph Notifier_Service["Notifier Service (notifier.py)"]
        Hook["/webhook endpoint"]
        SMTP[SMTP client]
    end
    Data[(SQLAlchemy / models.py)]
    R[(Redis)]
    K{{Kafka}}

    AppA --> Auth
    AppA --> R
    AppA --> Prod --> K
    K --> Cons --> Persist --> Data
    Persist -->|HTTP webhook| Hook --> SMTP
```

### Class Diagram (Domain Model)

```mermaid
classDiagram
    class Charge {
        +int id  «PK»
        +str idempotency_key  «unique»
        +int amount
        +str currency
        +int user_id
        +datetime created_at
    }
    class ChargeRequest {
        +int amount  «gt 0»
        +str currency  «len 3»
        +Optional~str~ description
    }
    class ChargeResponse {
        +int id
        +int user_id
        +int amount
        +str currency
        +str idempotency_key
        +datetime created_at
    }
    ChargeRequest ..> Charge : validated & persisted as
    Charge ..> ChargeResponse : serialized to
```

### Sequence Diagram (Charge Flow)

```mermaid
sequenceDiagram
    actor Client
    participant API as FastAPI API
    participant Redis
    participant Kafka
    participant Worker
    participant DB as PostgreSQL
    participant Notifier

    Client->>API: POST /users/{id}/charges (x-api-key, idempotency-key)
    API->>API: verify_api_key()
    API->>Redis: GET idempotency_key
    alt key already exists
        Redis-->>API: hit
        API-->>Client: 409 Conflict
    else new key
        Redis-->>API: miss
        API->>Kafka: publish "payments" event
        API->>Redis: SET key (TTL 24h)
        API-->>Client: 202 Accepted
        Kafka->>Worker: consume event
        Worker->>DB: INSERT charge (transaction)
        Worker->>Notifier: POST /webhook
        Notifier->>Notifier: send SMTP email receipt
    end
```

---

## 🛠️ Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **API Framework** | FastAPI (Python 3.12) | Async, type-safe, auto-generated OpenAPI docs |
| **Message Broker** | Apache Kafka (aiokafka) | Decouples ingestion from processing; durable, replayable event log |
| **Cache / Dedup** | Redis | Sub-millisecond idempotency lookups with TTL |
| **Database** | PostgreSQL + asyncpg | Durable transactional storage of charges |
| **ORM** | SQLAlchemy 2.0 (async) | Modern async ORM patterns |
| **Validation** | Pydantic v2 | Strict request/response schemas |
| **Containerization** | Docker & Docker Compose | One-command, reproducible multi-service stack |
| **CI** | GitHub Actions | Automated integration tests on every push/PR |
| **Notifications** | SMTP microservice | Out-of-band, service-to-service webhooks |

---

## ✨ Core Features

- **⚡ Async, non-blocking I/O** end-to-end — high throughput under load.
- **🔁 Idempotency protection** via Redis — duplicate `Idempotency-Key`s return `409 Conflict` instantly, never double-charging.
- **📨 Event-driven processing** — Kafka decouples the API from the database so the write path stays fast and the worker scales independently.
- **👷 Durable background worker** — consumes events, persists charges transactionally with rollback on failure.
- **🛟 Resilience** — exponential-backoff retries on the notifier webhook and a **Dead Letter Queue** for messages that can't be processed, so no accepted payment is silently lost. See [`docs/DESIGN.md`](docs/DESIGN.md).
- **🔔 Service-to-service webhooks** — a standalone notifier microservice sends email receipts.
- **🔐 API-key authentication** enforced via FastAPI dependency injection.
- **✅ Strict validation** — all payloads validated against Pydantic schemas.
- **📊 Observability** — liveness/readiness probes, dependency health checks, Prometheus metrics, and structured JSON logging.
- **🧪 Automated CI** — async integration tests (incl. real-Postgres Testcontainers) plus lint/format/type-check gates on every push and pull request.
- **🔑 12-factor config** — all secrets and endpoints externalized to environment variables, never hardcoded.

---

## 📦 Quickstart (One Command)

No local Python or Postgres needed — just Docker.

### 1. Clone and configure

```bash
git clone https://github.com/sravanibhamidipaty/mock-payment-gateway-api.git
cd mock-payment-gateway-api
cp .env.example .env   # then fill in your values
```

### 2. Launch the full stack

```bash
docker compose up --build
```

This spins up the full stack: API, PostgreSQL, Redis, Kafka, the background worker, the notifier, plus **Prometheus + Grafana** for monitoring.

| Service | URL / Port |
|---------|-----------|
| API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| Notifier | http://localhost:8001 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| PostgreSQL | `localhost:5433` |
| Redis | `localhost:6379` |
| Kafka | `localhost:9092` |

> If port 3000 is already in use, start Grafana on another port: `GRAFANA_PORT=3001 docker compose up`.

---

## 💻 Usage Example

```bash
curl -X POST http://localhost:8000/users/999/charges \
  -H "x-api-key: $API_KEY_SECRET" \
  -H "idempotency-key: order-abc-123" \
  -H "Content-Type: application/json" \
  -d '{"amount": 50, "currency": "USD", "description": "Test Charge"}'
```

**First request → `202 Accepted`:**

```json
{
  "status": "processing",
  "message": "Payment dropped on the conveyor belt!",
  "idempotency_key": "order-abc-123"
}
```

**Duplicate request (same key) → `409 Conflict`:**

```json
{
  "detail": "Idempotency Key 'order-abc-123' has already been used. (Caught by Redis!)"
}
```

**Retrieve a user's charges → `200 OK`:**

```bash
curl http://localhost:8000/users/999/charges -H "x-api-key: $API_KEY_SECRET"
```

---

## 📡 API Reference

| Method | Endpoint | Auth | Success | Description |
|--------|----------|------|---------|-------------|
| `POST` | `/users/{user_id}/charges` | `x-api-key` | `202 Accepted` | Submit a charge for async processing |
| `GET`  | `/users/{user_id}/charges` | `x-api-key` | `200 OK` | List all charges for a user |
| `GET`  | `/health` | — | `200 OK` | Liveness probe |
| `GET`  | `/readiness` | — | `200` / `503` | Readiness probe (checks Redis, Kafka, DB) |
| `GET`  | `/metrics` | — | `200 OK` | Prometheus metrics |

### Required Headers

| Header | Required on | Description |
|--------|-------------|-------------|
| `x-api-key` | all endpoints | API authentication key |
| `idempotency-key` | `POST` | Unique key preventing duplicate charges |
| `Content-Type` | `POST` | Must be `application/json` |

### Request Body

```json
{
  "amount": 50,
  "currency": "USD",
  "description": "Test Charge"
}
```

---

## 📊 Observability

The API ships with the probes and metrics you'd expect to run it under Kubernetes or behind a load balancer.

| Endpoint | Purpose | Behavior |
|----------|---------|----------|
| `GET /health` | **Liveness** — "is the process up?" | Always `200` if the app is running. Orchestrators use this to decide whether to **restart** the container. Intentionally does *not* check dependencies. |
| `GET /readiness` | **Readiness** — "can I serve traffic right now?" | Actively pings Redis, Kafka, and PostgreSQL. Returns `200` when all are healthy, `503` when any dependency is down — so traffic is **held**, not the container killed. |
| `GET /metrics` | **Metrics** — request counts, latency, status codes | Prometheus exposition format, scraped by a monitoring system. |

**Why two probes?** A transient Kafka outage shouldn't trigger a restart loop (restarting won't bring Kafka back). `/health` stays "alive" so the container isn't killed, while `/readiness` flips to `503` so the load balancer stops sending requests until the dependency recovers.

Example `/readiness` response when a dependency is down:

```json
{
  "ready": false,
  "checks": { "redis": "ok", "kafka": "down", "database": "ok" }
}
```

Logs are emitted as structured JSON via [`structlog`](https://www.structlog.org/), so they're ready to ship to any log aggregator (Datadog, Loki, CloudWatch) without parsing.

### Prometheus + Grafana

`docker compose up` also starts a **Prometheus** instance that scrapes `/metrics` and a **Grafana** instance pre-provisioned with a dashboard (request rate, latency p50/p95/p99, status codes, per-endpoint throughput). Open Grafana at **http://localhost:3000** → *Dashboards → "Payment Gateway API"* — no login required.

![Grafana dashboard](docs/images/grafana-dashboard.png)

---

## ⚙️ Configuration

All configuration lives in environment variables (see [`.env.example`](.env.example)). Nothing sensitive is committed to source.

| Variable | Description |
|----------|-------------|
| `API_KEY_SECRET` | API authentication key |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Database credentials |
| `DATABASE_URL_LOCAL` | Connection string for host runs (the Docker URL is built from the `POSTGRES_*` vars) |
| `REDIS_URL` | Redis connection URL |
| `KAFKA_URL` | Kafka broker address |
| `NOTIFIER_URL` | Worker → notifier webhook URL |
| `MAX_WEBHOOK_RETRIES` | Webhook retry attempts before giving up (default `3`) |
| `WEBHOOK_BACKOFF_BASE` | Initial backoff in seconds, doubled each retry (default `1.0`) |
| `DLQ_TOPIC` | Kafka dead-letter topic for unprocessable messages (default `payments.DLQ`) |
| `SENDER_EMAIL` / `RECEIVER_EMAIL` / `APP_PASSWORD` | SMTP credentials for email receipts |
| `AWS_*` | LocalStack/Floci endpoint + dummy credentials |

> **CI note:** GitHub Actions reads `API_KEY_SECRET`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` from repository secrets.

---

## 🧪 Running Tests

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
PYTHONPATH=src pytest
```

The suite combines fast hermetic unit tests (`pytest-asyncio` + `httpx` ASGI transport, with Redis/Kafka mocked) and an integration test that spins up a **real PostgreSQL via [Testcontainers](https://testcontainers.com/)** to verify the worker persists charges and fires webhooks. The Testcontainers test requires a running Docker daemon and skips gracefully without one. The same suite runs automatically in CI via GitHub Actions.

---

## 📁 Project Structure

```text
.
├── src/
│   ├── main.py            # FastAPI app: auth, idempotency, Kafka producer, health/metrics
│   ├── worker.py          # Kafka consumer: persist, retry-with-backoff webhook, DLQ
│   ├── notifier.py        # Notification microservice: SMTP email receipts
│   ├── database.py        # Async SQLAlchemy engine & session management
│   ├── models.py          # SQLAlchemy ORM models
│   └── schemas.py         # Pydantic request/response schemas
├── tests/
│   ├── test_api.py        # Async API integration tests
│   ├── test_worker.py     # Testcontainers (real Postgres) worker test
│   └── test_worker_unit.py # Retry/backoff + DLQ unit tests
├── docs/
│   └── DESIGN.md          # Architecture decisions & tradeoffs
├── monitoring/
│   ├── prometheus.yml     # Prometheus scrape config
│   └── grafana/           # Provisioned datasource + dashboard
├── .github/workflows/
│   └── tests.yml          # CI: tests + coverage + lint/format/type-check
├── docker-compose.yml     # full stack + monitoring orchestration
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## 🎯 What This Demonstrates

- Event-driven architecture with a real message broker (Kafka)
- Idempotent, at-least-once financial request handling with layered dedup (Redis + DB constraint)
- Resilience patterns: retry-with-backoff and a Dead Letter Queue
- Async Python end-to-end (FastAPI, SQLAlchemy 2.0, asyncpg, aiokafka)
- Microservice decomposition and service-to-service communication
- Observability: health/readiness probes, Prometheus metrics, structured logging
- Multi-container orchestration with Docker Compose
- CI/CD with quality gates (ruff, black, mypy) and Testcontainers integration tests
- 12-factor configuration and secrets management
- Documented system-design tradeoffs ([`docs/DESIGN.md`](docs/DESIGN.md))

---

## 📄 License

Provided for educational and portfolio purposes. See [LICENSE](LICENSE).
