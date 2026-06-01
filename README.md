# Data Engineering Interview Prep

Focused interview prep for senior data engineering roles, with emphasis on:
- **Data sharding & partitioning**
- **ETL pipeline design**
- **MapReduce concepts (and Spark)**

Content is sourced from real interview questions. All material is candidate-facing (no answer keys leaked from closed-book interviews).

---

## Structure

```
de-interview-prep/
├── README.md                              ← You are here
├── general-questions.md                   ← Warm-up / background / quick-fire concepts
│
├── concepts/
│   ├── distributed-systems.md             ← CAP theorem, replication, quorum, Kafka basics
│   ├── data-sharding.md                   ← Partitioning strategies, consistent hashing, hotspots
│   ├── mapreduce.md                       ← MapReduce model, Spark internals, skew, UDFs
│   └── etl-fundamentals.md               ← ETL patterns, idempotency, offsets, schema evolution
│
├── design-questions/
│   ├── etl-system-design.md              ← Full ETL: Kafka → multi-store (Kafka, Flink, Cassandra)
│   ├── data-aggregation.md               ← Time-series company mention counts (MapReduce + Cassandra)
│   ├── distributed-top-k.md              ← Daily top-100 ticker report (MapReduce + Spark)
│   ├── feature-store.md                  ← Online/offline feature store (point-in-time correctness)
│   ├── streaming-schema.md               ← Kafka schema design + evolution (Avro, Schema Registry)
│   ├── analytics-schema.md               ← Star schema / data warehouse DDL
│   ├── data-cleansing.md                 ← Immutable Parquet cleansing pipeline
│   └── sensitive-data.md                 ← Privacy-preserving data engineering
│
└── coding-questions/
    ├── distributed-enrichments.md        ← Stream aggregation + timeouts + distributed scaling
    ├── story-index.md                    ← Inverted index + binary search + distributed extension
    └── top-k-mapreduce.md               ← Top-K algorithms (heap/sort/quickselect) + Spark
```

---

## Recommended Study Order

### Week 1: Concepts

Start here. These are the building blocks every other question assumes.

1. **[Distributed Systems](concepts/distributed-systems.md)**
   - CAP theorem, quorum, replication, leader election, Kafka basics.
   - Know: Which side of CAP do you pick for a financial system? A social feed?

2. **[Data Sharding](concepts/data-sharding.md)**
   - Range, hash, list, composite partitioning.
   - Consistent hashing — why it matters when adding/removing nodes.
   - Hotspots and how to fix them.
   - How Kafka partitioning and Spark partitioning work.

3. **[MapReduce](concepts/mapreduce.md)**
   - Map / Shuffle / Reduce phases.
   - Spark: job / stage / task breakdown.
   - UDF types, broadcast joins, Arrow.
   - Data skew: identification and salting fix.

4. **[ETL Fundamentals](concepts/etl-fundamentals.md)**
   - Batch vs. streaming trade-offs.
   - Kafka offset management, idempotency, checkpointing.
   - Schema evolution (backward/forward/full compatibility).
   - Data replay, DLQ, error handling patterns.

---

### Week 2: Design Questions

Practice designing end-to-end systems. For each question:
1. Read the problem.
2. Close the doc and whiteboard your design.
3. Re-read the "What to Cover" checklist — see what you missed.
4. Review follow-up questions and practice answering them.

**Recommended order (easy → hard):**

| Question | Core Skill | Time |
|---|---|---|
| [Streaming Schema](design-questions/streaming-schema.md) | Schema + evolution | 30 min |
| [Analytics Schema](design-questions/analytics-schema.md) | Star schema / DDL | 20 min |
| [Distributed Top-K](design-questions/distributed-top-k.md) | MapReduce + Spark | 35 min |
| [Data Cleansing](design-questions/data-cleansing.md) | ETL pipeline design | 30 min |
| [Data Aggregation](design-questions/data-aggregation.md) | ETL + MapReduce + storage | 45 min |
| [Sensitive Data](design-questions/sensitive-data.md) | Privacy in data pipelines | 25 min |
| [Feature Store](design-questions/feature-store.md) | Online/offline architecture | 45 min |
| [ETL System Design](design-questions/etl-system-design.md) | Full system (Kafka → multi-store) | 60 min |

---

### Week 3: Coding Questions

Practice these until you can write the code + complexity analysis + follow-up discussion fluently.

| Question | Core Skill | Difficulty |
|---|---|---|
| [Top-K MapReduce](coding-questions/top-k-mapreduce.md) | Heap, sort, MapReduce | Easy → Medium |
| [Story Index](coding-questions/story-index.md) | Inverted index, binary search | Easy → Medium |
| [Distributed Enrichments](coding-questions/distributed-enrichments.md) | Stream aggregation, threading, sharding | Medium → Hard |

---

### Ongoing: General Questions

Use [general-questions.md](general-questions.md) to:
- Practice quick-fire concept checks (30–60 sec answers)
- Prepare behavioral answers grounded in real systems you've built
- Run through Spark internals cold (job/stage/task, UDF types, broadcast, Arrow)

---

## Key Concepts Cheat Sheet

### Data Sharding
- **Range partitioning:** fast range queries, hotspot risk on temporal data
- **Hash partitioning:** even distribution, bad for range queries, consistent hashing fixes rebalancing
- **Consistent hashing:** O(K/N) keys move when a node is added/removed (not O(K))
- **Hotspot fix:** salt the key, increase cardinality, pre-split hot ranges

### MapReduce Stages
```
Input → [Map] → (key, value) pairs → [Shuffle] → grouped by key → [Reduce] → output
```
- Shuffle is the most expensive phase (network transfer)
- Combiner reduces shuffle size (local pre-aggregation)
- Skew: one reducer overloaded → fix with salting or Spark skew join hint

### Spark Physical Operations
| Operation | Shuffle? |
|---|---|
| `filter`, `map`, `withColumn` | No |
| `groupBy`, `reduceByKey` | Yes |
| `join` (large + large) | Yes (both sides) |
| `join` (large + small, broadcast) | No |
| `repartition(n)` | Yes (full shuffle) |
| `coalesce(n)` | No (merge only) |
| `orderBy` | Yes (sort) |

### ETL Guarantees
| Delivery | When offset committed | Risk |
|---|---|---|
| At-most-once | Before processing | Data loss on crash |
| At-least-once | After successful write | Duplicate writes possible |
| Exactly-once | Transactional commit | Most complex, highest overhead |

### CAP Trade-offs
| System | CAP | Why |
|---|---|---|
| Cassandra | AP | Availability > consistency; tunable per query |
| HBase | CP | Consistency required; may reject writes during partition |
| Zookeeper | CP | Leader election requires strong consistency |
| Kafka | AP (for producers) | Availability preferred; consumers control consistency via acks |

---

## Technologies to Know

**Required:**
- Apache Kafka — topics, partitions, offsets, consumer groups, Schema Registry
- Apache Spark — RDDs, DataFrames, jobs/stages/tasks, shuffle, broadcast
- Parquet / Avro — columnar vs row storage, schema evolution
- S3 / HDFS — object vs. block storage trade-offs

**Recommended:**
- Cassandra — wide-column store, partitioning, replication factor, quorum
- Redis — sorted sets, pub/sub, use as feature cache
- Airflow / Argo — DAG orchestration, task dependencies, retry policies
- Flink — stateful stream processing, checkpoints, watermarks, exactly-once
- Delta Lake / Apache Iceberg — ACID on data lakes, time travel, schema enforcement
# de-interview-prep
