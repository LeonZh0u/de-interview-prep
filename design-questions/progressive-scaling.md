# Design: Progressive System Scaling (1 req/min to 1M req/min)

**Difficulty:** Medium-Hard
**Time:** ~60 minutes
**Topics:** Database design, Kafka, sharding, horizontal scaling, circuit breaker, latency SLAs

---

## Overview

This question is structured as a progressive design problem. You start with the simplest possible system and evolve it as new requirements are introduced. The goal is not to immediately propose the most complex solution — it is to reason clearly about **when** complexity is warranted and **what specifically** drives each architectural change.

For each phase, think through: what breaks at this scale? what is the minimum change needed to fix it?

---

## Phase 1: Low Scale — Simple REST to PostgreSQL

### Requirements

- ~1 request per minute.
- Each request contains a structured payload (assume: an event from an IoT device or a data pipeline step).
- Results must be queryable within 1 hour of ingestion (1 hour SLA).
- Up to 5% of requests can be dropped without consequence.
- No existing infrastructure — start from scratch.

### Architecture

```
[Client]
   │  HTTP POST /events
   ▼
[REST API Service]
   │  SQL INSERT
   ▼
[PostgreSQL]
```

A single stateless REST service receives the request, validates the payload, and writes directly to PostgreSQL. No queue, no cache, no replicas.

### Database Schema Design

Design a schema that separates mutable facts from dimension data. This makes queries efficient and schema evolution easier.

```sql
-- Dimension: the source (device, service, or user) that generated the event
CREATE TABLE sources (
    source_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name VARCHAR(256) NOT NULL,
    source_type VARCHAR(64)  NOT NULL,   -- e.g. 'iot_device', 'pipeline_step'
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fact: the event itself
CREATE TABLE events (
    event_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id   UUID NOT NULL REFERENCES sources(source_id),
    event_type  VARCHAR(128) NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_time  TIMESTAMPTZ NOT NULL    -- time the event occurred (from payload)
);

-- Composite index for the most common query pattern:
-- "give me all events of type X from source Y in the last hour"
CREATE INDEX idx_events_source_type_time
    ON events (source_id, event_type, event_time DESC);

-- Index for time-range scans without source filter
CREATE INDEX idx_events_event_time
    ON events (event_time DESC);
```

**Why JSONB for payload?** The event payload schema may vary by event type or source. Using JSONB avoids schema migrations when new fields are added, while still allowing indexed queries on specific payload fields via a GIN index if needed.

**Why separate sources from events?** Source metadata changes rarely (source name, type). Keeping it in a separate table avoids duplicating it in every event row. Queries joining on `source_id` can filter and aggregate across sources efficiently.

### What is Acceptable at This Scale

- Single PostgreSQL instance (no read replicas needed at 1 req/min).
- No connection pooling — a single connection from the service is fine.
- No retries — with a 5% drop budget, failed requests can be discarded.
- Simple synchronous write: the HTTP response is sent only after the DB write completes.

---

## Phase 2: Reliability — Zero Drops at 100 req/min

### New Requirements

- Volume increased to ~100 requests per minute.
- **Zero drops allowed.** Every request must be durably stored, even if the database is temporarily unavailable or overloaded.
- SLA still 1 hour — eventual write to DB within 1 hour is acceptable.

### What Breaks at Phase 1

At 100 req/min (~1.7 req/sec), a single PostgreSQL instance can still handle the write throughput easily. The reliability problem is the **coupling** between the REST service and the database. If the database is slow, restarting, or running a long migration, requests are lost.

### Architecture Change: Durable Message Queue

```
[Client]
   │  HTTP POST /events
   ▼
[REST API Service]
   │  publish message (durable)
   ▼
[Message Queue]         ←── durable: messages survive restarts
(Kafka or RabbitMQ)
   │  consume (worker)
   ▼
[Worker Service]
   │  SQL INSERT
   ▼
[PostgreSQL]
```

The REST service now acknowledges the request as soon as the message is durably written to the queue. The database write happens asynchronously in a separate worker process.

**Why this guarantees zero drops:** Even if the database is down, messages accumulate in the queue. When the database recovers, the worker drains the backlog. The queue provides durability (messages are persisted to disk) and decoupling (the REST service does not need to wait for the DB).

### Worker Pattern

```python
def run_worker():
    consumer = KafkaConsumer("events.raw", group_id="db-writer")
    for message in consumer:
        event = parse(message.value)
        try:
            db.execute("INSERT INTO events (...) VALUES (...) ON CONFLICT DO NOTHING", event)
            consumer.commit()   # only commit after successful DB write
        except DatabaseError as e:
            log_error(e)
            # do NOT commit — message will be re-delivered on worker restart
            sleep(backoff)
```

Key point: commit the Kafka offset only after the DB write succeeds. If the worker crashes after the DB write but before the commit, the message will be re-delivered and the `ON CONFLICT DO NOTHING` clause ensures the duplicate insert is a no-op.

### Kafka vs. RabbitMQ for This Use Case

| | Kafka | RabbitMQ |
|---|---|---|
| Durability | Excellent — log-based, configurable retention | Good — messages can be persisted to disk |
| Replay | Yes — any consumer can re-read from any offset | No — consumed messages are deleted |
| Throughput | Very high (millions/sec) | Moderate (tens of thousands/sec) |
| Operational complexity | Higher | Lower |

At 100 req/min, either works. Kafka is the better choice if you anticipate needing replay (e.g., for Phase 4 re-processing) or if you expect volume to grow significantly.

---

## Phase 3: High Scale — 1 Million req/min

### New Requirements

- Volume: ~1,000,000 requests per minute (~16,700 req/sec).
- SLA: still 1 hour for full ingestion.
- Zero drops.

### What Breaks at Phase 2

1. **Single PostgreSQL instance cannot sustain ~16,700 inserts/sec** with indexes maintained.
2. **Single Kafka topic partition** means messages are processed sequentially — one consumer cannot keep up.
3. **Single worker service** is a single point of failure and a throughput bottleneck.

### Architecture Changes

#### Horizontal Scaling of Consumers

Add partitions to the Kafka topic and run one worker per partition. Kafka guarantees that each partition is consumed by at most one consumer in a consumer group at any time.

```
Kafka Topic: events.raw (64 partitions)
                │
    ┌───────────┼───────────┐
    │           │           │
[Worker 1]  [Worker 2] ... [Worker 64]
(partition 0) (partition 1)  (partition 63)
    │           │           │
    └───────────┼───────────┘
                │
           [Database layer]
```

Scale consumer instances horizontally. On Kubernetes, this is a horizontal pod autoscaler (HPA) that scales based on Kafka consumer group lag.

#### NoSQL for Write Throughput

PostgreSQL with a single primary can sustain roughly 5,000–15,000 simple inserts/sec depending on hardware and index configuration. At 16,700 inserts/sec, you need either:

**Option A: PostgreSQL with sharding**
- Partition the `events` table by `source_id` hash or by `event_time` range (time-series partitioning).
- Route writes to different PostgreSQL instances based on the shard key.
- Tools: Citus (PostgreSQL extension), Vitess (MySQL), or manual application-level sharding.

**Option B: Switch to a NoSQL write-optimized store**
- **Apache Cassandra:** LSM-tree based storage engine; optimized for high write throughput. Write path is append-only (fast). Reads require compaction overhead.
- **Amazon DynamoDB:** Managed; scales writes horizontally by partition key. Simple to operate; vendor lock-in tradeoff.

For a time-series event store at this scale, Cassandra with a partition key of `(source_id, date_bucket)` and clustering key of `event_time` is a strong choice.

```
Cassandra table:
CREATE TABLE events_by_source (
    source_id    UUID,
    date_bucket  DATE,          -- partition by day to avoid wide partitions
    event_time   TIMESTAMP,
    event_id     UUID,
    event_type   TEXT,
    payload      TEXT,          -- serialized JSON
    PRIMARY KEY ((source_id, date_bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

#### Read Replicas for Query Traffic

Separate write traffic from read traffic. Writes go to the primary (or Cassandra write coordinators). Analytical queries and dashboard reads go to read replicas or a separate read-optimized store (e.g., ClickHouse for OLAP queries).

---

## Phase 4: External Dependency — Circuit Breaker

### New Requirements

The system now enriches events by calling an external third-party API (e.g., geocoding an IP address, looking up a user's profile). The external API is sometimes slow or unavailable.

### What Breaks

Without protection, a slow or unavailable external dependency will:
1. Cause write workers to block waiting for the API response.
2. Back up the Kafka consumer lag.
3. Potentially cascade and bring down the entire ingestion pipeline.

### Circuit Breaker Pattern

```
[Worker]
   │
   ▼
[Circuit Breaker]  ←── tracks success/failure rate of external calls
   │
   ▼
[External API]
```

States:
- **Closed:** Normal operation. All requests pass through to the external API.
- **Open:** The external API has failed too many times recently. All requests are immediately rejected (fail fast) without calling the API. The circuit is open for a configured timeout (e.g., 30 seconds).
- **Half-Open:** After the timeout, one test request is allowed through. If it succeeds, the circuit closes. If it fails, the circuit stays open.

```
Failure threshold: 50% of requests fail in last 10 seconds → OPEN circuit
Recovery timeout: 30 seconds → try HALF-OPEN
Success threshold: 5 consecutive successes in HALF-OPEN → CLOSE circuit
```

### Caching Enrichment Results

Most enrichment lookups are repeated (the same IP address appears in many events). Add an LRU cache in front of the external API:

```
[Worker]
   │
   ▼
[LRU Cache]  → cache hit: return enrichment instantly (microseconds)
   │ cache miss
   ▼
[Circuit Breaker]
   │
   ▼
[External API]
```

Cache TTL should reflect how often the external data changes. For IP geolocation, 24 hours is reasonable. For user profile data, 5–15 minutes.

### Retry with Exponential Backoff

For transient failures (network blip, momentary overload), retry with exponential backoff and jitter:

```python
def call_external_api_with_retry(payload, max_retries=3):
    for attempt in range(max_retries):
        try:
            return external_api.call(payload)
        except TransientError as e:
            if attempt == max_retries - 1:
                raise
            sleep_time = (2 ** attempt) + random.uniform(0, 1)  # jitter
            time.sleep(sleep_time)
```

### Async Enrichment (Decouple Enrichment from Ingestion)

If enrichment latency is not acceptable in the critical path, decouple it:
1. Write the raw unenriched event to the database immediately.
2. Publish the event to an `events.to-enrich` Kafka topic.
3. A separate enrichment worker reads from `events.to-enrich`, calls the external API, and updates the event record.

This means events are available in near-real-time (without enrichment) and enriched within a few seconds. The ingestion pipeline is no longer coupled to the external dependency.

---

## Phase 5: Strict Latency SLA — 500ms End-to-End

### New Requirements

A premium tier of customers requires that events are queryable within **500ms of receipt**. The queue-based architecture (Phase 2 onward) adds seconds to minutes of latency before DB write.

### What Breaks

The Kafka-based worker pattern has inherent latency:
- Producer → broker write: ~5–50ms
- Consumer poll interval: 100ms–1s
- Worker processing time: variable

Total: easily 200ms–5s. Cannot meet 500ms reliably.

### Architecture Change: Dual Path

Run two ingestion paths in parallel:

```
[Client]
   │
   ├──────────────────────────┐
   │                          │
   ▼                          ▼
[Fast Path]               [Reliable Path]
Direct DB write           Kafka → Worker
(synchronous)             (async, all customers)
   │
   ▼
[Redis Cache]   ←── serve queries from cache
(write-through)     until DB write propagates
                    to read replicas
```

**Fast path details:**
- REST service writes directly to the database (bypasses Kafka).
- Also writes to Redis with a 10-minute TTL (write-through cache).
- Query service reads from Redis first; falls back to DB on cache miss.
- Achieves sub-100ms write-to-query latency for premium events.

**Premium flag routing:** The REST service checks whether the request is from a premium customer (from the auth token or a header) and routes it through the fast path. All other requests go through the Kafka path.

### Geographic Deployment

For globally distributed customers, network latency from client to API can dominate:
- Client in Tokyo → US-East API: ~150ms round trip just for network.

Deploy regional API endpoints (US-East, EU-West, AP-Southeast) and route clients to the nearest region via DNS or anycast. Each region writes to a regional database. A background replication process syncs data cross-region for global queries.

---

## Phase 6: Mixed Payloads — Real-Time + Batch

### New Requirements

In addition to small real-time events (~1KB each), the system now receives daily batch files from some sources (up to 1MB per file, containing thousands of records).

### What Breaks

Sending a 1MB message through Kafka is technically possible (Kafka's default max message size is 1MB) but problematic:
- Large messages slow down consumer poll loops.
- They take more broker memory and disk bandwidth.
- They mix latency characteristics — a 1MB message takes longer to write and replicate than a 1KB message.

### Separate Pipelines

```
Small real-time events (<10KB):
[Client] → [REST API] → [Kafka] → [Worker] → [DB]

Large batch files (>10KB):
[Client] → [S3 upload]
                │
                │ S3 event notification (ObjectCreated)
                ▼
           [SQS Queue] → [Batch Processor Lambda or service]
                               │
                               │ parse file, split into records
                               ▼
                          [DB: bulk insert]
```

**Why S3 for batch files:**
- S3 handles large objects efficiently (multipart upload, parallel download).
- S3 event notifications provide a reliable trigger when a file is uploaded.
- The batch processor can parallelize processing by splitting the file into chunks.

**Bulk insert for batch files:**
```python
def process_batch_file(s3_path):
    records = download_and_parse(s3_path)  # returns list of events
    # Insert in chunks to avoid locking the table for too long
    for chunk in chunks(records, size=1000):
        db.execute_many(
            "INSERT INTO events (...) VALUES %s ON CONFLICT DO NOTHING",
            chunk
        )
```

---

## Operations

### Deployment Strategies

| Strategy | Mechanism | Best For |
|---|---|---|
| Blue-Green | Two identical environments; switch traffic from blue to green | Zero-downtime deploys; easy rollback |
| Canary | Roll out to 5% of traffic first; monitor; expand or roll back | Catching production-specific bugs; validating performance |
| Feature Flags | Code deployed to all instances; feature enabled per tenant/user | Testing features with specific customers; gradual rollouts without redeployment |

For a data ingestion pipeline, canary deployments are particularly valuable: you can route 5% of real traffic to the new version and compare output (DB writes, error rates, latency) before committing to full rollout.

### Testing Strategy

| Level | What It Tests | Example |
|---|---|---|
| Unit | Individual functions, parsers, transformers | Parse a malformed event payload; assert correct error returned |
| Integration | Service + real DB / queue (in Docker) | Worker writes to a test Kafka topic; assert DB row inserted correctly |
| End-to-End | Full pipeline from API to DB | POST request to staging API; assert record queryable within 1 second |
| Load | Throughput and latency under target load | `locust` or `k6` at 16,700 req/sec; assert P99 < 200ms |
| Chaos Engineering | System behavior during failures | Kill a Kafka broker; assert no message loss; assert consumer reconnects |

**Chaos engineering is especially important for this system.** The whole point of the queue-based architecture is resilience to failures. If you cannot demonstrate that the system recovers from Kafka broker failures, consumer crashes, or database restarts, you do not know that it actually will.

---

## Summary: Scaling Decision Points

| Phase | Scale | Key Change | Reason |
|---|---|---|---|
| 1 | 1 req/min | REST → PostgreSQL | Simplest thing that works |
| 2 | 100 req/min | Add Kafka | Zero drops; decouple from DB availability |
| 3 | 1M req/min | Multiple Kafka partitions + NoSQL | Single writer can't keep up |
| 4 | External dep | Circuit breaker + cache | Prevent cascade failures |
| 5 | 500ms SLA | Fast path + Redis cache | Queue latency too high for premium tier |
| 6 | Mixed payloads | S3 + batch processor | Large files don't belong in Kafka |
