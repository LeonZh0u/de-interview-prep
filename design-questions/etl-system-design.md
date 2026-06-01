# Design: ETL System (Kafka → Multi-Store)

**Difficulty:** Senior / Hard
**Time:** ~60 minutes
**Focus areas:** Distributed systems, Kafka, data stores, idempotency, checkpointing, schema evolution, scalability

---

## Problem Statement

Design an ETL system that ingests data from **Kafka** and writes it into **multiple data stores**. The system must be:

- Highly configurable (different transformation rules per topic)
- Support different data transformations and filtering
- Support routing to different target stores based on data attributes
- All driven by user-defined configuration — no code changes to add new pipelines

### Motivating Use Cases

| Use Case | Data Volume | Transformation | Target Store |
|---|---|---|---|
| New equity tickers | Low | Enrich with company name | Redis (fast inference) |
| News for sentiment | Medium | NLP preprocessing | Cassandra |
| Chat data for training | High | Tokenization, dedup | HDFS / S3 (parquet) |

### Example Workflow
- Raw trade prices arrive on a Kafka topic: `[ticker, price, timestamp]`
- Enrich with full ticker name (AAPL → "Apple Inc.")
- Prepare for ML training (normalize, type-cast)
- Store in S3 parquet for future training runs
- Allow point-in-time extraction by date range

---

## Step 1: Clarify Requirements

Before designing, ask:

- What are the SLA requirements for each use case? (low-latency inference vs. batch training)
- What transformation complexity is needed? (simple field mapping vs. complex joins/aggregations)
- What are the expected data volumes?
- How do we handle schema changes in Kafka topics?
- Do we need to support data replay from a specific offset/time?
- How is configuration managed — static files or a live API?
- Multi-tenancy requirements?

---

## Step 2: High-Level Architecture

```
Kafka Topics
    |
    v
[Consumer Layer]         <- reads from Kafka, manages offsets
    |
    v
[Config Store]           <- Zookeeper / Confluent Schema Registry
    |
    v
[Transformation Layer]   <- Flink / Spark Structured Streaming
    |  filter | map | join | aggregate | schema validate
    v
[Router / Sink Layer]    <- Kafka Connect sinks / custom connectors
    |
    +---> PostgreSQL (structured transactional)
    +---> Cassandra (high-throughput NoSQL)
    +---> S3 / HDFS (data lake, training data)
    +---> Redis (feature cache)
    +---> Delta / Iceberg (lakehouse, ACID)
```

---

## Step 3: Deep Dive — Key Design Decisions

### Kafka Offset Management
- **Manual commit offsets** only after successful write to target store.
- Use consumer groups so each ETL job is an independent consumer.
- For replay: reset offsets with `kafka-consumer-groups --reset-offsets --to-datetime`.
- Note: Kafka TTL limits how far back you can replay. Archive to S3/HDFS for longer retention.

### Idempotent Operations
- Every write to a target must be idempotent.
- RDBMS: Use UPSERT (`INSERT ... ON CONFLICT DO UPDATE`).
- S3: Use unique file names (include partition key + timestamp + UUID).
- Cassandra: Writes are naturally idempotent (last-write-wins).
- Redis: SET operations are idempotent by key.

### Checkpointing & Stateful Processing
- Use Flink's periodic checkpoints to HDFS/S3.
- On failure, restart from last checkpoint — only replay messages since the checkpoint.
- Checkpoint interval trade-off: shorter → more overhead but faster recovery.
- For stateful transformations (windowed aggregations), Flink's state backend (RocksDB) persists state between restarts.

### Schema Evolution
- Use **Confluent Schema Registry** with Avro/Protobuf.
- Enforce **backward compatibility** for all schema changes (new fields must have defaults).
- Consumers fetch the schema by schema ID embedded in each message.
- ETL configuration includes the schema version it was written for.

### Data Transformation Configuration
Example configuration for a single ETL job:
```json
{
  "jobId": "news-sentiment-etl",
  "source": { "kafkaTopic": "news-raw", "groupId": "sentiment-etl-group" },
  "transformations": [
    { "type": "filter", "condition": "language == 'EN'" },
    { "type": "map", "field": "body", "fn": "strip_html" },
    { "type": "enrich", "join_table": "company_tickers", "key": "ticker" }
  ],
  "targets": [
    { "type": "cassandra", "keyspace": "news", "table": "articles" },
    { "type": "s3", "bucket": "ml-training", "prefix": "news/2024/", "format": "parquet" }
  ]
}
```

### Error Handling
- Failed messages → **Dead Letter Queue (DLQ)** Kafka topic.
- DLQ consumer alerts on-call and stores to S3 for inspection.
- Retry with exponential backoff for transient failures (e.g., Cassandra timeout).
- Do not retry for schema validation errors — send to DLQ immediately.

### Scalability
- Scale consumers horizontally: add more consumer instances to a group.
- Kafka partitioning determines max parallelism: N consumers per N partitions.
- Stateless transformations scale linearly.
- Stateful aggregations: partition by the aggregation key so each partition is handled by one worker (no cross-node coordination).

---

## Step 4: Data Replay

**Trigger:** A bug corrupted 3 days of data in Cassandra. You need to replay from Kafka.

1. Identify the time range of bad data.
2. Reset consumer group offsets to the start of that range.
3. Restart the ETL job — it replays all messages in the range.
4. Because writes are idempotent (UPSERT), re-running overwrites the bad data.
5. Monitor consumer lag until caught up.

**Limitation:** If Kafka TTL has expired for that range, replay from S3 archive instead (using a batch Spark job).

---

## Step 5: API Design (Optional)

**Configuration Management API:**
```
POST   /etl/jobs          — create a new job config
PUT    /etl/jobs/{id}     — update config (hot reload, no downtime)
GET    /etl/jobs/{id}     — retrieve config
DELETE /etl/jobs/{id}     — delete job
```

**Control API:**
```
POST /etl/jobs/{id}/start
POST /etl/jobs/{id}/stop
POST /etl/jobs/{id}/replay?from=2024-01-01&to=2024-01-03
```

**Monitoring API:**
```
GET /etl/jobs/{id}/status     — lag, throughput, error rate
GET /etl/jobs/{id}/dlq        — dead letter queue contents
```

---

## What to Cover in Your Answer

- [ ] Kafka consumer group setup and offset management strategy
- [ ] Idempotency: how each target handles replay without duplicates
- [ ] Checkpointing: where state is saved and how recovery works
- [ ] Schema evolution: how new fields are added without breaking consumers
- [ ] Transformation DSL or configuration format
- [ ] Error handling: DLQ, retry, alerting
- [ ] Horizontal scaling: how you add throughput
- [ ] Data replay: the exact steps to reprocess a date range
- [ ] Monitoring: what metrics and alerts you would instrument

---

## Follow-Up Twists

- "What if a configuration change (new transformation) must be applied mid-stream without data loss?"
- "What if the same message must be written to both Cassandra AND S3 atomically?"
- "How do you handle a Cassandra cluster that is temporarily unavailable while Kafka keeps filling up?"
- "One Kafka partition receives 100x more messages than others. How do you detect and fix this?"
