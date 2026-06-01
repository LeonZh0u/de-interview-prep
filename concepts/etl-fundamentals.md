# ETL Fundamentals

ETL (Extract, Transform, Load) is the backbone of data engineering. Every data pipeline, feature store, analytics system, and ML training pipeline is fundamentally an ETL.

---

## The Three Phases

### Extract
Pull data from one or more source systems.

| Source Type | Examples | Challenges |
|---|---|---|
| Message queues | Kafka, RabbitMQ | Offset management, TTL, ordering |
| Relational DB | PostgreSQL, MySQL | Change data capture (CDC), lock contention |
| File storage | S3, HDFS, GCS | Large files, schema diversity |
| APIs | REST, GraphQL | Rate limits, pagination, auth |
| Streaming | Kafka, Kinesis, Pub/Sub | Exactly-once semantics |

**Interview angle:** "What is your source of truth and what are the failure modes of reading from it?"

---

### Transform
Apply business logic, cleansing, enrichment, and aggregation to the data.

Common transformations:
- **Filtering:** Remove invalid/irrelevant records (e.g., internal users, null fields).
- **Normalization / Standardization:** Clean trailing spaces, normalize encodings, coerce types.
- **Enrichment:** Join with reference data (e.g., resolve ticker symbol → company name).
- **Aggregation:** Group by key and compute counts, sums, averages.
- **Schema evolution:** Handle new/deprecated fields gracefully.
- **Deduplication:** Detect and drop duplicate records.

**Interview angle:** "How do you make your transformations idempotent — i.e., running them twice produces the same result?" (Critical for replay scenarios.)

---

### Load
Write the transformed data to one or more target stores.

| Target Type | Best For | Examples |
|---|---|---|
| Data Lake | Bulk storage, ML training data | S3, HDFS (Parquet, ORC, Avro) |
| Data Warehouse | Analytics and BI queries | Hive, BigQuery, Snowflake, Redshift |
| NoSQL DB | High-throughput online reads | Cassandra, HBase, DynamoDB |
| Cache / KV Store | Low-latency feature serving | Redis, Memcached |
| RDBMS | Transactional data | PostgreSQL, MySQL |
| Lakehouse | Unified batch + streaming | Delta Lake, Apache Iceberg, Apache Hudi |

---

## Batch vs. Streaming ETL

| | Batch | Streaming |
|---|---|---|
| **Trigger** | Scheduled (e.g., daily at 2am) | Continuous / event-driven |
| **Latency** | Minutes to hours | Milliseconds to seconds |
| **Throughput** | Very high (optimized for bulk) | Lower per-event, but sustained |
| **Complexity** | Simpler state management | Windowing, watermarks, exactly-once |
| **Use case** | Historical aggregations, training data | Real-time dashboards, fraud, alerts |
| **Tools** | Spark, Hive, Hadoop MR | Flink, Kafka Streams, Spark Structured Streaming |

**Key interview point:** "Can you implement batch with streaming tech?" Yes — treat the bounded dataset as a stream with a start and end watermark. Trade-off: added complexity of state management, loss of some bulk optimizations.

---

## Kafka Offset Management

Kafka's offset is the position of a consumer within a partition. Correct offset management is what separates reliable ETLs from broken ones.

- **Auto-commit offsets:** Kafka auto-commits offsets at a regular interval. Risk: if the process crashes between commit and processing, data may be lost or duplicated.
- **Manual offset commit:** Commit only after the data has been successfully written to the target. Guarantees **at-least-once** delivery.
- **Exactly-once semantics:** Requires transactional APIs in Kafka + idempotent writes to the target (e.g., Flink's two-phase commit with Kafka transactions).

**Delivery guarantees:**
- **At-most-once:** Offsets committed before processing. Risk of data loss.
- **At-least-once:** Offsets committed after processing. Risk of duplicates.
- **Exactly-once:** Transactional. Hardest to achieve; requires coordination.

---

## Idempotency

An operation is **idempotent** if applying it multiple times produces the same result as applying it once.

Why it matters: In any distributed ETL, you will replay messages (network failure, process crash). Without idempotency, replay creates duplicate data.

**Strategies:**
- **UPSERT (insert-or-update):** Use a unique key; if the record exists, update it instead of inserting.
- **Deduplication table:** Track processed message IDs; skip if already seen.
- **Idempotent writes by design:** Writing a pre-aggregated count to a KV store is idempotent if you compute from raw data each time (overwrite, not increment).

---

## Checkpointing and Stateful Processing

**Checkpointing** saves the ETL's state (offsets + in-progress computation) to durable storage at regular intervals. On restart, the job resumes from the last checkpoint instead of the beginning.

**Flink checkpoints:**
- Snapshots of all operator state to HDFS/S3.
- Checkpoint interval trades off: shorter = more overhead, faster recovery; longer = less overhead, more replay on failure.

**Spark checkpointing:**
- Truncates the lineage DAG to a saved state on HDFS.
- Write-Ahead Log (WAL) for streaming sources.

**Interview angle:** "If your ETL crashes mid-run, what exactly would be replayed, and how do you ensure replaying doesn't corrupt the target?"

---

## Schema Evolution

Schemas change over time (new fields, deprecated fields, type changes). Your ETL must handle this gracefully.

### Common Patterns (Avro / Protobuf / JSON Schema)

| Pattern | Definition | Example |
|---|---|---|
| **Backward compatible** | New schema can read old data | Add an optional field with a default |
| **Forward compatible** | Old schema can read new data | Consumers ignore unknown fields |
| **Full compatible** | Both directions | Conservative changes only |

**Schema Registry (e.g., Confluent):** A central service that enforces compatibility rules. Producers register new versions; consumers can fetch the schema for any version by ID.

**Avro specifics:** Field defaults are required for backward compatibility. A new field must have a default value so old readers can still parse messages that lack it.

---

## Data Replay

When you need to reprocess historical data from Kafka:

1. **Reset consumer group offsets** to the desired point in time using `kafka-consumer-groups --reset-offsets`.
2. **Respect TTL:** Kafka retains messages for a configurable period (e.g., 7 days). If you need to replay older data, it must be archived elsewhere (e.g., S3 mirror via Kafka Connect).
3. **Idempotent target writes** — replay will re-deliver messages; the target must handle duplicates.
4. **Restore from checkpoint** — restart Flink/Spark job from a checkpoint rather than offset 0.

---

## Error Handling

### Dead Letter Queue (DLQ)
Messages that fail processing (bad schema, downstream timeout) are routed to a DLQ topic instead of blocking the pipeline.

### Retry Strategies
- **Exponential backoff:** Wait 1s, 2s, 4s, 8s... between retries.
- **Max retries + DLQ:** After N retries, move to DLQ and alert.

### Alerting and Monitoring
- Track consumer lag (how far behind is your consumer?).
- Track error rates per transform step.
- Use Prometheus + Grafana or similar.

---

## Orchestration

ETL jobs don't run in isolation — they have dependencies. Orchestrators manage scheduling, dependency tracking, and retries.

| Tool | Style | Notes |
|---|---|---|
| **Apache Airflow** | DAG-based, Python | Most common; rich UI; cron-like scheduling |
| **Argo Workflows** | Kubernetes-native, YAML | Good for containerized jobs |
| **Prefect / Dagster** | Modern Python-first | Better observability than Airflow |

**Interview angle:** "If step 2 of your ETL fails, how does your orchestrator handle it? Does step 3 run? How do you retry only the failed step without restarting the whole pipeline?"

---

## ETL Design Checklist

Use this when designing any ETL pipeline in an interview:

- [ ] **Source:** What's the source? How does data arrive (push vs. pull)?
- [ ] **Schema:** What's the schema? How does it evolve?
- [ ] **Extraction:** How do you handle source unavailability / backpressure?
- [ ] **Transform:** What are the steps? Are they idempotent?
- [ ] **Filtering:** What records are excluded and why?
- [ ] **Enrichment:** What external data is joined? Is it a broadcast join?
- [ ] **Target(s):** Where does data go? Why that store?
- [ ] **Latency:** Batch or streaming? What SLA?
- [ ] **Failure handling:** DLQ? Retry? Checkpointing?
- [ ] **Replay:** Can you reprocess? What's the retention?
- [ ] **Monitoring:** How do you know if it's broken?
- [ ] **Scalability:** What happens if volume doubles?

---

## Key Interview Questions to Practice

1. **Idempotency:** "Your ETL crashes and replays 1 million messages into your PostgreSQL target. How do you ensure no duplicates?"
2. **Offset management:** "Explain at-least-once vs. exactly-once delivery in Kafka. When would you choose each?"
3. **Schema evolution:** "A new field `precipitation_rate` is being added to a Kafka topic, but rollout takes 3 months. How do you update the schema without losing messages?"
4. **Checkpointing:** "Your Flink job crashes 4 hours into an 8-hour aggregation. What's replayed? How do you minimize replay?"
5. **Batch vs. streaming:** "You need daily aggregated counts AND real-time alerts on the same data. Design the pipeline."
6. **Orchestration:** "Your ETL has 5 steps with step 4 depending on steps 2 and 3. Step 3 fails. What does Airflow do?"
