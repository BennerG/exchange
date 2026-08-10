# Exchange

A single-stock order matching exchange in Go, built around a Kafka event pipeline with exactly-once settlement semantics. Orders are matched with price-time priority against an in-memory order book, and every trade is durably settled into a PostgreSQL ledger exactly once, even under message redelivery.

I created this project to explore real distributed systems depth: consumer groups, idempotent producers, exactly-once settlement, and the concrete tradeoffs between different exactly-once patterns depending on what a consumer is actually doing with what it reads.

### This is a simulation

Exchange settles trades against an in-memory order book and a PostgreSQL ledger for the purpose of demonstrating distributed systems patterns. It never moves real money and never touches a real brokerage account.

## Architecture

```mermaid
flowchart LR
    Client[REST client] -->|POST /orders| Producer[Producer service]
    Producer -->|OrderSubmitted| OrdersTopic[(Kafka: orders)]
    OrdersTopic --> Matcher[Matcher consumer]
    Matcher -->|Filled, one per trade, transactional| FillsTopic[(Kafka: fills)]
    Matcher -->|unmarshal or permanent failures| DLQ[(Kafka: dlq)]
    FillsTopic --> Settler[Settler consumer]
    Settler -->|SettleFill| Postgres[(PostgreSQL ledger)]
    Settler -->|unmarshal or permanent failures| DLQ
```

The producer accepts orders over REST, validates them, and publishes an `OrderSubmitted` event to Kafka with an idempotent producer configuration. The matcher consumes those events, runs them through an in-memory order book, and produces one `Filled` event per trade for every match, with the match, the produce, and the offset commit all wrapped in a single Kafka transaction. The settler consumes `Filled` events and writes them into PostgreSQL, one trade per row, with the row's primary key doubling as the idempotency guard against redelivery. Messages that fail unrecoverably at either stage route to a shared dead letter queue instead of retrying forever.

## Tech stack

| Layer               | Technology                                                                           |
| ------------------- | ------------------------------------------------------------------------------------ |
| Language            | Go                                                                                   |
| Messaging           | Kafka, KRaft mode, no Zookeeper                                                      |
| Serialization       | Protobuf, via buf                                                                    |
| Schema management   | Confluent Schema Registry                                                            |
| Kafka client        | Sarama (pure Go, no CGO)                                                             |
| Database            | PostgreSQL                                                                           |
| Postgres client     | pgx v5                                                                               |
| Metrics             | Prometheus and Grafana, dashboard provisioned as code                                |
| Logging             | zerolog                                                                              |
| Local orchestration | Docker Compose                                                                       |
| Testing             | testcontainers-go for real Postgres in tests, table-driven tests, `-race` throughout |

## Quick start

```bash
# 1. Clone the repo and enter exchange
git clone https://github.com/BennerG/exchange.git
cd exchange

# 2. Spin up containers [Kafka cluster, Confluent schema registry, Postgres, Prometheus, Grafana]
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
psql "postgresql://exchange:exchange@localhost:5432/exchange" -c "SELECT * FROM transactions;"
psql "postgresql://exchange:exchange@localhost:5432/exchange" -c "SELECT * FROM accounts;"
```

To test the unmarshal failure path to the dead letter queue, send invalid data to the `matcher`:

```bash
echo "this is not valid protobuf" | docker exec -i exchange-kafka kafka-console-producer --broker-list localhost:9092 --topic orders
```

That error shows up in the logs, followed by an entry on the `dlq` topic.

Grafana is available at `http://localhost:3000` (default login `admin`/`admin`, Grafana prompts a password change on first login), with the Exchange dashboard already provisioned under the Exchange folder. Prometheus's own UI is at `http://localhost:9090/targets`, useful for confirming all three services are being scraped successfully.

**Current status:** the full pipeline runs end to end, REST submission, idempotent Kafka publish, order matching, and exactly-once settlement into PostgreSQL with correct double-entry balances. The matcher's match, produce, and offset commit are fully transactional, and resting order book state survives a process restart via a Postgres-backed pending orders table, kept honest by a periodic reconciliation loop. DLQ routing is live on both consumers. Prometheus and Grafana are fully wired: order latency, fill rate, transaction outcomes, consumer lag, settlement outcomes, and DLQ rate are all live in a provisioned dashboard.

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
│   ├── producer/             # REST handler, Kafka publisher, metrics
│   ├── consumer/             # matcher, settler, group handlers, dead letter routing, metrics
│   ├── kafka/                # shared idempotent and transactional producer configuration
│   ├── metrics/              # shared Prometheus HTTP server
│   └── store/                # Store interface, in-memory and Postgres implementations
├── proto/trading/events/   # protobuf schema
├── migrations/             # PostgreSQL schema
├── grafana/provisioning/   # Grafana datasource and dashboard, provisioned as code
├── docker-compose.yml      # Kafka (KRaft), Schema Registry, PostgreSQL, Prometheus, Grafana
└── scripts/prometheus.yml
```

## Design decisions

### Single stock, single partition, by design

Exchange trades one symbol, QQQ, on a single Kafka partition. This was a deliberate scoping decision made from the start. For this project, I wanted the focus to be completely on learning and implementing the defining characteristics of a durable, high-throughput system by utilizing Kafka: exactly-once settlement, consumer group semantics, and idempotent production. To add more stocks or partitions, I'll need to extend horizontally: partition by `stock_id`, run one order book instance per partition, and scale out consumer instances.

### Price-time priority

Resting orders at the same price fill in the order they arrived. To mirror real-world exchanges, I sort my heaps (buyHeap and sellHeap) to surface the best bid and ask price first, followed by a tie-breaker comparison of arrival time.

### Partial fills and cancellation

An order that doesn't fully match on submission rests in the book with its remaining quantity, and can be cancelled at any point while pending, including after a partial fill has already occurred. `Book.Cancel` uses an index tracked on each `Order` plus a `map[string]*Order` lookup, giving O(1) lookup and O(log n) heap removal via `heap.Remove`.

### Money as a structured Protobuf type

Price is represented as `amount_cents int64` plus `currency string`, even though this project only ever uses USD. This keeps the schema honest about what a price actually is at the wire level and leaves room to extend without a breaking schema change later.

### Filled events represent one trade, both sides, atomically

Originally, `Filled` was shaped from a single order's perspective, one order ID, one counterparty. I changed the design to represent a complete trade instead: both order IDs, both user IDs, one `trade_id`. This was a deliberate tradeoff. The alternative, one event per order per trade, is a more natural shape for a future "notify me when my order fills" feature, but it means settlement can no longer be atomic across both sides of a trade, since two independent messages describing one economic event can arrive, fail, or redeliver independently of each other. Currently, I am prioritizing atomic settlement for this project's scope. The per-order notification shape is deferred; see [What's Next](#whats-next).

I removed `PartialFillRejected` before the matcher was written. Nothing in the current matching logic produces a partial fill that then gets rejected, a partial fill simply rests until it either fills further or is cancelled, so the message type had no trigger. I removed two `remaining_quantity` fields on `Filled` for the same reason: `orderbook.FillResult` doesn't currently supply them. Both are easy to reintroduce once a concrete consumer actually needs them.

### Two different exactly-once patterns

The settler and the matcher solve exactly-once in genuinely different ways, because they're doing genuinely different kinds of work.

The settler reads a `Filled` event and writes to an external system, PostgreSQL, that isn't Kafka. Its guarantee comes from committing the Kafka offset only after the database transaction commits, combined with `trade_id` as a database primary key. A redelivered message hits the primary key constraint, returns `ErrAlreadySettled`, and the settler treats that as a safe no-op rather than a failure.

The matcher reads an `OrderSubmitted` event and also produces new events, `Filled`, as a result. Kafka's transactional producer API, `BeginTxn`, `AddMessageToTxn`, `CommitTxn`, makes consume, produce, and offset commit one atomic unit, with the settler reading `fills` at `isolation.level=read_committed` so it never sees output from an aborted transaction. Getting this fully correct required one more layer than the Kafka transaction alone provides; see [Resolved: the matcher's match and produce are fully atomic](#resolved-the-matchers-match-and-produce-are-fully-atomic) below.

### Resolved: the matcher's match and produce are fully atomic

This gap went through two passes before it was actually closed. I'll explain a bit of my first fix to provide context for the fix that exists with this latest update.

The order book is in-memory, mutable state, so a redelivered `OrderSubmitted` after a partial failure could match against a book already altered by the failed attempt. The first fix wrapped Kafka produce and offset commit in a transaction, `BeginTxn`, `AddMessageToTxn`, `CommitTxn`. That closed the Kafka-side half of the problem, a `read_committed` consumer would never see a partial, inconsistent batch of fills. It did not close the in-memory half: `book.Add` still mutated the real book before the transaction's outcome was known, so a failure in `CommitTxn` itself, after every fill had already published successfully, could still leave the book reflecting a match Kafka never actually finalized.

The real fix splits what used to be one operation, `Add`, into three. `Book.Match` computes a match and mutates counterparty orders for real, immediately, but deliberately never rests the incoming order itself. `Book.Apply` rests it, the one remaining step. `Book.Discard` fully reverses everything `Match` did: every counterparty it popped is pushed back, every partial fill it applied is subtracted back off, and the incoming order's own `Filled` resets to zero. `TransactionalGroupHandler` calls `Apply` only once `CommitTxn` has actually returned success, and `Discard` on every other exit path, including a failed `CommitTxn`. A redelivered order after any failure, at any point in that sequence, matches against a book in exactly the state it was in before the failed attempt ever ran.

### Resolved: the order book is resilient against crashes

The order book only ever existed in memory, so a crashed or restarted matcher used to lose every resting order. This now persists into the `pending_orders` table that has been sitting in the schema since the very first migration.

Every committed match writes to Postgres in the same step that finalizes it in memory: whichever counterparty orders were fully consumed get deleted, whichever one was left partially filled gets its `quantity_filled` updated, and the incoming order gets inserted if it still has quantity remaining after the match. This write happens strictly after the Kafka transaction has committed, since that's the only point where the outcome is actually final, and it retries with exponential backoff if it fails, since a database blip at that exact moment is the realistic failure mode worth absorbing automatically.

On startup, the matcher loads every row from `pending_orders`, ordered by when it was originally submitted, and restores each one directly onto the book with `Book.Restore`. This bypasses the matching algorithm entirely. Replaying orders through it instead could turn a subtle bug elsewhere into orders quietly matching against each other during startup, which could turn into a debugging nightmare.

A periodic reconciliation loop compares the book's live state against what's persisted and corrects any drift it finds, treating memory as the source of truth, since it already reflects every transaction that has actually committed. The book itself stays single-threaded; the matcher publishes an immutable snapshot after every commit and discard using `atomic.Pointer`, so the reconciler reads a recent view with no lock ever touching `orderbook.Book`.

### Using pgx directly for Postgres access

`internal/store/postgres.go` uses pgx v5 directly. This project has no need to swap database engines, and pgx's direct API gives cleaner transaction handling for `SettleFill`'s atomic multi-statement write.

### Testing against a real Postgres instance

`internal/store`'s Postgres tests run against a genuine, disposable Postgres container via testcontainers-go. This exercises real constraint enforcement: the unique constraint on `trade_id` is what actually proves the idempotency guarantee.

### A poison pill, found by actually running the pipeline

Testing the full pipeline end to end for the first time surfaced a real failure mode that isolated component tests couldn't catch. A test order used a readable placeholder like `user-abc` instead of a real UUID. It passed through the producer and the matcher cleanly, since neither validated the shape of `user_id`, and only failed once the settler tried to insert it into a `uuid` typed Postgres column.

Because the settler only commits a Kafka offset after a message settles successfully, and this message could never succeed no matter how many times it retried, it entered a permanent retry loop, once a second, forever. On a single partition, every trade behind it in the topic was silently blocked too. Nothing crashed and nothing alerted; the pipeline just quietly stopped making progress, arguably a more dangerous failure mode than a crash, since nothing prompts anyone to notice.

The fix has two parts. UUID validation was added at the REST boundary, so malformed input now fails fast with a 400 instead of propagating three services deep. And the incident exposed a real gap in the consumer's error handling: it only distinguished malformed bytes (skip and log) from every other failure (retry), but this was a third case, structurally valid data guaranteed to fail deterministically, which needed its own path. That distinction led directly to the DLQ design below.

### One shared dead letter queue with a JSON envelope

Both consumers route unrecoverable messages to one shared `dlq` topic. The originating topic, partition, and offset are carried in the envelope, so nothing is lost by consolidating. The envelope itself is plain JSON: a dead letter entry is an operational artifact meant for a human to inspect while debugging, so it doesn't need schema registry involvement.

Two distinct failure classes route here. A message that fails to unmarshal at all is dead-lettered immediately, since corrupted bytes fail identically on every retry. A settlement failure wrapping `store.ErrPermanent`, currently certain Postgres data and constraint errors, routes here too, since those are also guaranteed to fail on every retry. Genuinely transient failures, a database or broker briefly unreachable, are left uncommitted for ordinary Kafka redelivery instead. The classification logic lives in exactly one place, `postgres.go`, the only file that actually knows what a given Postgres error code means; the consumer group handlers only ever check `errors.Is(err, store.ErrPermanent)`, with zero database-specific knowledge of their own.

The matcher does not yet have its own permanent-failure classification, since its only realistic failure today is a Kafka publish itself failing, which is transient by nature; every match attempt currently aborts and retries instead. See [What's Next](#whats-next).

### One idempotent producer constructor shared across every publisher

`internal/kafka.NewIdempotentProducer` and `NewTransactionalProducer` hold the sarama configuration every publisher in this project needs: `MaxOpenRequests`, `RequiredAcks`, `Idempotent`, `Retry.Max`. This was deliberately duplicated across the order producer and the fills publisher at first, on the rule that duplication earns being extracted on its third occurrence. The dead letter publisher was that third occurrence, and the shared constructor came out of it.

### Instrumentation follows the same package boundaries as everything else

Each service defines its own metrics next to the code that updates them, `internal/producer/metrics.go`, `internal/consumer/metrics.go`, so a metric always lives beside whatever it measures. The one exception is `internal/metrics.Serve`, the HTTP handler that exposes them: every service needs the exact same handful of lines to start that server, the same third-occurrence reasoning that earned the shared Kafka producer constructor its own package.

Matching duration uses its own histogram buckets instead of Prometheus's defaults. `prometheus.DefBuckets` starts at five milliseconds, tuned for network-scale latency; in-memory matching finishes in microseconds, so every observation would land in the very first bucket and the resulting percentiles would be meaningless. The matcher's buckets start at fifty microseconds.

The dead letter counter is instrumented once, inside `deadLetterAndCommit`, the function both `GroupHandler` and `TransactionalGroupHandler` already share. Both consumers get DLQ metrics from that one line.

Grafana's datasource and dashboard are both provisioned as files under `grafana/provisioning`. Cloning the repo and running `docker-compose up` produces a dashboard that already exists, in keeping with the same setup philosophy as the rest of this project.

## What's Next

- **Matcher failure classification.** The matcher currently treats every failure as transient and retries. Its only real failure mode today is a Kafka publish itself failing, but a dedicated classification, mirroring `store.ErrPermanent`, would be worth adding if a genuinely unrecoverable matcher failure mode ever appears.
- **Multi-stock support**, partitioning by `stock_id`, one order book per partition.
- **Per-order notification events** (`Filled` from a single order's perspective, alongside the current trade-based event), for a future live fill notification feature.
- **An automated trading client**, as an ordinary producer client submitting orders through the existing REST API based on a learned strategy, consuming historical or live price data. This would live entirely outside Exchange's own code.
- A possible reimplementation of the matching engine in Rust or C++ down the line, once the Go version is complete, to compare latency characteristics directly against a garbage collected runtime.
