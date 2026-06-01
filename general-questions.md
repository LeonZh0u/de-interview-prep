# General / Warm-Up Questions for Data Engineering Interviews

Use these to self-assess, or as conversation starters in early rounds. Strong answers show breadth of experience and practical wisdom, not just textbook knowledge.

---

## Background & Experience

- Describe the data system you're most proud of building. What was the source? The consumers? The scale?
- What type of data do you manage in your current role?
  - Who else relies on it? How do they use it?
  - What systems did you choose, and why those specifically?
  - What challenges did you face, and how did you handle them?
  - How do you manage outages?
- Describe your interactions with data scientists. What do they typically need from you?
- Do you follow any major data engineering OSS projects or trends? (e.g., Apache Iceberg, DuckDB, OpenLineage)

---

## Architectural

**Batch vs. Streaming:**
- Describe an application that requires streaming and one that requires batch.
- Can you implement batch using stream processing technology? What trade-offs do you need to consider?
- When would you choose Hive over Spark? *(Answer: Hive for very large, stable, SQL-heavy workloads where Spark's startup cost isn't worth it; Spark for complex transformations, ML pipelines, or iterative processing.)*

**Data Lake vs. Data Warehouse:**
- What is the difference between a Data Lake and a Data Warehouse?
- When would you use each?
- What trends are you following? *(Look for: Lakehouse / Delta / Iceberg, HTAP systems)*

**ETL Design:**
- Walk me through how you'd design an ETL from an S3 data lake to a feature store.
- How do you handle schema evolution in a Kafka-based pipeline?

---

## Compute & Distributed Processing

**Cardinality:**
- How is cardinality important in distributed processing?
  - What happens if cardinality is too low? *(Buckets overflow → hotspot)*
  - Too high? *(Too many tiny buckets → overhead)*
  - What's the "Goldilocks zone" you aim for?

**Spark Internals (key interview topic):**
- What is the difference between a Spark **task**, **stage**, and **job**?
- What is a **UDF**? What kinds exist in Spark?
  - Python UDF (slow), Pandas UDF / vectorized UDF (faster), built-in functions (fastest)
- What is a **broadcast join**? When do you use it?
- What is **Apache Arrow** and how does it help Spark?
- What is your approach to scaling a slow Spark job?
  - (Diagnose first: what's slow? Shuffle? Join? Skew? Serialization?)
- What is `repartition` vs. `coalesce`? Which causes a full shuffle?

**MapReduce concepts:**
- Describe the Map, Shuffle, and Reduce phases of a MapReduce computation.
- What physical operations happen during a shuffle?
- What is data skew and how do you detect and fix it?

---

## Databases & Storage

**Distributed Databases:**
- What distributed databases have you worked with? What did you like and dislike?
- When would you choose Cassandra vs. Redis vs. HBase?
- What's the difference between an OLTP and OLAP database?
- What is a **star schema**? When would you use it?

**Data Partitioning:**
- What are the different ways to partition data in a distributed database?
- What is consistent hashing? Why is it better than standard hashing for adding/removing nodes?
- How does Kafka use partitioning? What's the relationship between partition key and consumer routing?

**Table Statistics:**
- Why are table statistics (e.g., in Hive) useful?
  - The query optimizer uses them to choose between broadcast join vs. shuffle join, partition pruning, etc.

---

## Systems

**Object Storage vs. Block Storage:**
- What are the differences between S3 and HDFS?
- What are some challenges with storing big data on S3?
  - Small files problem (overhead per object), lack of atomic renames, eventual consistency for listings, cost of LIST operations.

**Authentication:**
- What is a JWT (JSON Web Token)?
  - What are the three sections? *(Header, Payload, Signature)*
  - If a token is stolen, can the thief read its contents? *(Yes — payload is Base64-encoded, not encrypted. Encryption = JWE.)*

**Production Debugging:**
- Describe your approach to debugging a production data service.
  - If it helps: "HTTP service X is not responding to 38% of requests — client just hangs."
  - Strong answers mention: distributed tracing, heap dumps, CPU profiling, load testing, checking for thread pool exhaustion, checking downstream dependencies, log correlation, metrics dashboards.
  - Red flags: only mentions adding print statements / log lines.

---

## Data Quality & Reliability

- How do you test an ETL pipeline?
- How do you detect data drift (when the distribution of incoming data shifts unexpectedly)?
- What is a dead letter queue and when would you use one?
- How do you ensure an ETL is idempotent?

---

## Strong Answers to Aim For

For each question above, the best answers:
1. **Reference a specific technology** (not just vague concepts).
2. **State the trade-off** — what does this approach sacrifice?
3. **Draw from real experience** — "In my last role, we had to..."
4. **Acknowledge uncertainty honestly** — "I haven't used X directly, but conceptually it works like..."

---

## Quick-Fire Concept Checks

These should take 30–60 seconds each:

| Question | Key Point |
|---|---|
| What does "at-least-once" delivery mean? | May replay/duplicate; consumer must be idempotent |
| What is a consumer group in Kafka? | Multiple consumers sharing partitions of a topic |
| What is WAL (Write-Ahead Log)? | Durability: write intent to log before applying change |
| What is backpressure? | Upstream slows down when downstream is overwhelmed |
| What is a compacted Kafka topic? | Retains only the latest value per key — like a KV store |
| What is Delta Lake? | Lakehouse: ACID transactions + versioning on Parquet files in S3 |
| What does ACID stand for? | Atomicity, Consistency, Isolation, Durability |
| What is the difference between `map` and `flatMap` in Spark? | `map` → 1-to-1; `flatMap` → 1-to-many (like exploding arrays) |
| What is a shuffle in Spark? | Data movement across partitions/nodes, triggered by groupBy/join |
| What is a tombstone in Kafka? | A message with a null value — signals deletion in a compacted topic |
