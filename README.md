# Exchange

A single-stock order matching exchange in Go, built around a Kafka event pipeline with exactly-once settlement semantics. Orders are matched with price-time priority against an in-memory order book, and every trade is durably settled into a PostgreSQL ledger exactly once, even under message redelivery.

I created this project to explore real distributed systems depth: consumer groups, idempotent producers, exactly-once settlement, and the concrete tradeoffs between different exactly-once patterns depending on what a consumer is actually doing with what it reads.

### This is a simulation, not a real trading system

Exchange settles trades against an in-memory order book and a PostgreSQL ledger for the purpose of demonstrating distributed systems patterns. It never moves real money and never touches a real brokerage account.

## Architecture

```mermaid
flowchart LR
    Client[REST client] -->|POST /orders| Producer[Producer service]
    Producer -->|OrderSubmitted| OrdersTopic[(Kafka: orders)]
    OrdersTopic --> Matcher[Matcher consumer]
    Matcher -->|Filled, one per trade| FillsTopic[(Kafka: fills)]
    Matcher -.->|planned| DLQ[(Kafka: DLQ)]
    FillsTopic --> Settler[Settler consumer]
    Settler -->|SettleFill| Postgres[(PostgreSQL ledger)]
    Settler -.->|planned| DLQ
```

The producer accepts orders over REST, validates them, and publishes an `OrderSubmitted` event to Kafka with an idempotent producer configuration. The matcher consumes those events, runs them through an in-memory order book, and publishes one `Filled` event per trade for every match produced. The settler consumes `Filled` events and writes them into PostgreSQL, one trade per row, with the row's primary key doubling as the idempotency guard against redelivery.

## Tech stack

| Layer               | Technology                                                                           |
| ------------------- | ------------------------------------------------------------------------------------ |
| Language            | Go                                                                                   |
| Messaging           | Kafka, KRaft mode, no Zookeeper                                                      |
| Serialization       | Protobuf, via buf                                                                    |
| Schema management   | Confluent Schema Registry                                                            |
| Kafka client        | Sarama (pure Go, no CGO)                                                             |
| Database            | PostgreSQL                                                                           |
| Postgres client     | pgx v5, used directly, not through database/sql                                      |
| Metrics             | Prometheus (in progress)                                                             |
| Logging             | zerolog                                                                              |
| Local orchestration | Docker Compose                                                                       |
| Testing             | testcontainers-go for real Postgres in tests, table-driven tests, `-race` throughout |

## Quick start

```bash
# 1. Clone the repo and enter exchange
git clone https://github.com/BennerG/exchange.git
cd exchange

# 2. Spin up containers [Kafka cluster, Confluent schema registry, Postgres, Prometheus]
docker-compose up -d

# 3. Run migration
psql "postgresql://exchange:exchange@localhost:5432/exchange" -f migrations/001_initial_schema.sql

# 4. Start all three services, each in its own terminal
go run ./cmd/producer
go run ./cmd/matcher
go run ./cmd/settler
```

With all three running, submit a resting order and a crossing order:

```bash
# Buy order
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "a1b2c3d4-1111-4a1a-9c1a-000000000001",
    "quantity": 100,
    "price_per_share": {"amount_cents": 47500, "currency": "USD"},
    "side": "BUY"
  }'

# Sell order
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "a1b2c3d4-1111-4a1a-9c1a-000000000002",
    "quantity": 100,
    "price_per_share": {"amount_cents": 47500, "currency": "USD"},
    "side": "SELL"
  }'
```

The second request crosses the spread, and the trade settles into Postgres within a second or two. Verify it landed:

```bash
psql "postgresql://postgres:postgres@localhost:5432/exchange" -c "SELECT * FROM transactions;"
psql "postgresql://postgres:postgres@localhost:5432/exchange" -c "SELECT * FROM accounts;"
```

**Current status:** the full pipeline runs end to end, REST submission, idempotent Kafka publish, order matching, exactly-once settlement into PostgreSQL with correct double-entry balances. The matcher's produce step is idempotent but not yet wrapped in a Kafka transaction, see [Known limitation](#known-limitation-the-matchers-order-book-is-not-yet-idempotent-against-redelivery) below. DLQ routing for malformed or unrecoverable messages is not yet built.

### Running tests

```bash
go test ./... -race
```

`internal/store`'s Postgres tests spin up a real, disposable Postgres container via testcontainers-go, so this requires a working Docker connection.

## Repository structure

```
exchange/
├── cmd/
│   ├── matcher               # matcher service entrypoint
│   ├── producer              # producer service entrypoint
│   └── settler               # settler service entrypoint
├── internal/
│   ├── orderbook/            # price-time priority matching engine, pure in-memory
│   ├── producer/             # REST handler, Kafka publisher
│   ├── consumer/             # matcher and settler
│   └── store/                # Store interface, in-memory and Postgres implementations
├── proto/trading/events/   # protobuf schema
├── migrations/             # PostgreSQL schema
├── docker-compose.yml      # Kafka (KRaft), Schema Registry, PostgreSQL, Prometheus
└── scripts/prometheus.yml
```

## Design decisions

### Single stock, single partition, by design

Exchange trades one symbol, QQQ, on a single Kafka partition. This was a deliberate scoping decision, not a limitation discovered later. For this project, I wanted the focus to be completely on learning and implementing the defining characteristics of a durable, high-throughput system by utilizing Kafka: exactly-once settlement, consumer group semantics, and idempotent production. To add more stocks or partitions, I'll need to extend horizontally: partition by `stock_id`, run one order book instance per partition, and scale out consumer instances.

### Price-time priority, not greedy matching

Resting orders at the same price fill in the order they arrived, not arbitrarily. To mirror match orders in real-world exchanges, I sort my heaps (buyHeap and sellHeap) to both have the best bid and ask price available followed by a tie-breaker comparison of arrival time.

### Partial fills and cancellation

An order that doesn't fully match on submission rests in the book with its remaining quantity, and can be cancelled at any point while pending, including after a partial fill has already occurred. `Book.Cancel` uses an index tracked on each `Order` plus a `map[string]*Order` lookup, giving O(1) lookup and O(log n) heap removal via `heap.Remove`.

### Money as a structured Protobuf type

Price is represented as `amount_cents int64` plus `currency string`, not a bare integer, even though this project only ever uses USD. This keeps the schema honest about what a price actually is at the wire level and leaves room to extend without a breaking schema change later.

### Filled events represent one trade, both sides, atomically

Originally, `Filled` was shaped from a single order's perspective, one order ID, one counterparty. I changed the design to represent a complete trade instead: both order IDs, both user IDs, one `trade_id`. This was a deliberate tradeoff. The alternative, one event per order per trade, is a more natural shape for a future "notify me when my order fills" feature, but it means settlement can no longer be atomic across both sides of a trade, since two independent messages describing one economic event can arrive, fail, or redeliver independently of each other. Currently, I am prioritizing atomic settlement for this project's scope. The per-order notification shape is deferred; see [What's Next](#whats-next).

I removed `PartialFillRejected` before the matcher was written. Nothing in the current matching logic produces a partial fill that then gets rejected, a partial fill simply rests until it either fills further or is cancelled, so the message type had no trigger. I removed two `remaining_quantity` fields on `Filled` for the same reason: `orderbook.FillResult` doesn't currently supply them. Both are easy to reintroduce once a concrete consumer actually needs them.

### Two different exactly-once patterns, not one

The settler and the matcher solve exactly-once in genuinely different ways, because they're doing genuinely different kinds of work.

The settler reads a `Filled` event and writes to an external system, PostgreSQL, that isn't Kafka. Its guarantee comes from committing the Kafka offset only after the database transaction commits, combined with `trade_id` as a database primary key. A redelivered message hits the primary key constraint, returns `ErrAlreadySettled`, and the settler treats that as a safe no-op rather than a failure.

The matcher reads an `OrderSubmitted` event and also produces new events, `Filled`, as a result. This is the shape Kafka's own transactional producer API exists for, consume, transform, produce, and commit the offset as one atomic unit, with downstream consumers reading with `isolation.level=read_committed` so they never see output from an aborted transaction. This is not yet implemented; see the known limitation below.

### Known limitation: the matcher's order book is not yet idempotent against redelivery

The order book is in-memory, mutable state scoped to a single matcher process. Every call to `book.Add` permanently changes which orders are resting, so matching is not a pure function the way settlement is.

If a Kafka transaction wrapping consume, match, produce, and commit were to abort partway through, for example because publishing the second of three resulting fills failed, the offset never commits and Kafka redelivers the same `OrderSubmitted` event. The order book has already been mutated by the failed attempt, so re-running `Add` on the redelivered order matches against a book that has already partially advanced, and can produce different fills the second time through.

This is a known limitation of the current scope, not a solved problem. Two candidate fixes are listed under [What's Next](#whats-next).

### Known limitation: the order book has no persistence

A crashed matcher process loses every resting order along with it, since the book exists only in memory. There is currently no mechanism to reconstruct book state on restart.

### pgx direct, not database/sql

`internal/store/postgres.go` uses pgx v5 directly rather than through Go's `database/sql` interface. This project has no need to swap database engines, and pgx's direct API gives cleaner transaction handling for `SettleFill`'s atomic multi-statement write than the more generic `database/sql` interface would.

### Real Postgres in tests, not a mock

`internal/store`'s Postgres tests run against a genuine, disposable Postgres container via testcontainers-go rather than a mocked database layer. This tests real constraint enforcement, the unique constraint on `trade_id` is what actually proves the idempotency guarantee, something no mock could verify.

### A poison pill, found by actually running the pipeline

Testing the full pipeline end to end for the first time surfaced a real failure mode that isolated component tests couldn't catch. A test order used a readable placeholder like `user-abc` instead of a real UUID. It passed through the producer and the matcher cleanly, since neither validated the shape of `user_id`, and only failed once the settler tried to insert it into a `uuid` typed Postgres column.

Because the settler only commits a Kafka offset after a message settles successfully, and this message could never succeed no matter how many times it retried, it entered a permanent retry loop, once a second, forever. On a single partition, every trade behind it in the topic was silently blocked too. Nothing crashed and nothing alerted; the pipeline just quietly stopped making progress, arguably a more dangerous failure mode than a crash, since nothing prompts anyone to notice.

The fix has two parts. UUID validation was added at the REST boundary, so malformed input now fails fast with a 400 instead of propagating three services deep. And the incident exposed a real gap in the consumer's error handling: it currently only distinguishes malformed bytes (skip and log) from every other failure (retry), but this was a third case, structurally valid data guaranteed to fail deterministically, which needs its own path rather than being retried alongside genuinely transient failures. That distinction is part of the DLQ design work listed under [What's Next](#whats-next).

## What's Next

- **Full Kafka transactional produce for the matcher**, `BeginTxn`, `AddOffsetsToTxn`, `CommitTxn`, so consume, match, produce, and offset commit become one atomic unit and the redelivery gap below is fully closed. The matcher currently uses a simpler pattern, idempotent produce plus manual offset commit after publish succeeds, which runs correctly end to end but does not fully close the gap.
- **Order book persistence**, so a crashed matcher can reconstruct resting orders on restart rather than losing them.
- **DLQ routing** for messages that fail processing in either consumer, including a dedicated path for input that is structurally valid but will deterministically fail every retry, distinct from genuinely transient failures.
- **Prometheus metrics and a Grafana dashboard**: order latency, fill rate, consumer lag, DLQ count.
- **Multi-stock support**, partitioning by `stock_id`, one order book per partition.
- **Per-order notification events** (`Filled` from a single order's perspective, alongside the current trade-based event), for a future live fill notification feature.
- **An automated trading client**, as an ordinary producer client submitting orders through the existing REST API based on a learned strategy, consuming historical or live price data. This would live entirely outside Exchange's own code.
- **Table-driven refactor** of the producer's rejection-case tests into one table-driven test.
- A possible reimplementation of the matching engine in Rust or C++ down the line, once the Go version is complete, to compare latency characteristics directly against a garbage collected runtime.
