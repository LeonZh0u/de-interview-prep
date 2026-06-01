# Design: Feature Store

**Difficulty:** Medium–Hard
**Time:** ~45 minutes
**Focus areas:** Online vs offline storage, ETL pipelines, point-in-time correctness, data consistency, latency SLAs

---

## Problem Statement

Design a **Feature Store** — a system that allows ML engineers to:
1. Access features for **training** (historical point-in-time correct values).
2. Access features at **inference time** (latest values, low latency).

### Key Definitions

- **Feature:** A measurable property of an entity used as model input (e.g., "story count for company X in the last 10 minutes").
- **Feature Engineering:** Deriving features from raw data.
- **Time Series:** A feature with its value tracked at multiple timestamps.

### Example Use Case

A news importance model needs two features for a given company:
- **(a) Rolling story count:** Number of stories mentioning the company in the last 10 minutes — computed every few minutes (streaming).
- **(b) Daily read count:** Total reads on stories about the company for today — computed end-of-day (batch).

---

## Step 1: Clarify Requirements

- What's the read latency SLA for inference? → A few milliseconds for ~10s of features per request.
- What's acceptable latency for training data retrieval? → Several minutes for millions of feature rows — batch is fine.
- Do all features have timestamps? → Yes. If not provided, use `now()` at insertion time.
- What types of values? → Numeric only (simplify schema).
- How large can the store get? → Millions of rows/day, hundreds of features.
- Point-in-time correctness for training? → Critical. Training must use only features available at the time of the training label (no data leakage).
- How are features referenced? → By URI (e.g., `company_data:story_count_10min`).

---

## Step 2: Core Insight — Two Distinct Access Patterns

| | Training (Offline) | Inference (Online) |
|---|---|---|
| **Query type** | Point-in-time: "What was the feature value at timestamp T?" | Latest: "What is the feature value right now?" |
| **Latency** | Minutes (batch) | Milliseconds |
| **Volume** | Millions of rows | Tens of features per request |
| **Staleness tolerance** | None (must be exact) | Small window OK |
| **Storage** | Big table / Parquet / HDFS | Redis / Cassandra |

The system must have **two separate stores** optimized for each use case, kept in sync.

---

## Step 3: Architecture

```
[Feature Computation]
    |
    +-- Streaming (Flink/Kafka) → computes 10-min rolling counts
    +-- Batch (Spark/Airflow)   → computes daily read counts
    |
    v
[Ingestion API]  write(feature_name, entity_id, timestamp, value)
    |
    +---> [Offline Store]  (BigTable / HBase / Parquet on S3)
    |       — time-series, append-only
    |       — queryable by (entity_id, timestamp range)
    |
    +---> [Online Store]  (Redis / Cassandra)
    |       — latest value per (entity_id, feature_name)
    |       — optimized for low-latency key lookups
    |
[Sync Job]  periodically pushes latest offline values → online store
            (ensures online store doesn't diverge from offline ground truth)
```

---

## Step 4: Data Model

### Offline Store (Time-Series)

```
Row: (entity_id, feature_name, timestamp) → value

Example:
  (company_id=1001, "story_count_10min", 2021-04-12 10:59:42) → 15.0
  (company_id=1001, "story_count_10min", 2021-04-12 11:09:42) → 23.0
  (company_id=1001, "daily_reads",       2021-04-12 00:00:00) → 45201.0
```

**Implementation:** Wide table in Cassandra or BigTable with clustering key on timestamp — efficient range scans.

### Online Store (Latest Value)

```
Key: (entity_id, feature_name)
Value: latest value

Redis: HSET company:1001 story_count_10min 23.0
```

---

## Step 5: API Design

Based on the `feast` open-source Feature Store pattern:

```python
store = FeatureStore()

# --- OFFLINE (Training) ---
# Retrieve historical feature values at specific timestamps (point-in-time)
entity_df = pd.DataFrame({
    "company_id": [1001, 1002, 1003],
    "event_timestamp": [
        datetime(2021, 4, 12, 10, 59),
        datetime(2021, 4, 12, 8,  12),
        datetime(2021, 4, 12, 16, 40),
    ]
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["company_data:story_count_10min", "company_data:daily_reads"],
).to_df()
# Returns: entity_df + feature columns, each value = the most recent value
# BEFORE (not after) the entity's event_timestamp — prevents data leakage

# --- ONLINE (Inference) ---
# Retrieve the LATEST feature value for an entity
feature_vector = store.get_online_features(
    features=["company_data:story_count_10min", "company_data:daily_reads"],
    entity_rows=[{"company_id": 1001}]
).to_dict()
# Returns: {"story_count_10min": 23.0, "daily_reads": 45201.0}
```

**Point-in-time correctness:** For `get_historical_features`, look up the feature value with the largest timestamp `<= event_timestamp`. This ensures you never use future data during training.

---

## Step 6: Ingestion Pipelines

### Streaming (for 10-minute rolling counts)
```
Kafka (news events)
    → Flink (rolling window aggregation: COUNT per company per 10-min window)
    → Feature Store Ingestion API
    → writes to both Offline and Online stores
```

### Batch (for daily read counts)
```
Airflow trigger (end of day)
    → Spark job: compute daily_reads per company
    → Feature Store Ingestion API
    → writes to Offline store
    → Sync job pushes latest to Online store
```

---

## Step 7: Key Challenges

### Consistency Between Online and Offline
- Online store must always reflect the latest offline values.
- Approach: a sync job periodically reads the latest offline values and upserts into Redis/Cassandra.
- Risk: sync lag → online may be slightly stale. For most features this is acceptable.

### Availability
- Online store (Redis): replicate across multiple nodes; use Redis Sentinel or Cluster mode.
- Offline store: replicate across regions for DR.

### Backfills
- When a new feature is added, historical values must be computed back in time.
- Run a batch Spark job over the offline event archive to populate historical values.
- Backfill must be point-in-time correct (compute using only data available at each timestamp).

---

## Advanced Topics (Stretch Goals)

### Versioning
- "company_data:story_count_10min:v2" vs. "v1"
- Trade-off: rename = backward-incompatible change; version tag = more complex API.

### Provenance and Lineage
- Track which code version produced which feature values and from which source data.
- Enables debugging of model degradation: "the model started performing worse on 2024-03-01 — what changed?"

---

## What to Cover in Your Answer

- [ ] Two-store architecture: offline (historical, batch) + online (latest, low-latency)
- [ ] Point-in-time correctness for training (critical — prevents data leakage)
- [ ] Write API: what a feature row looks like
- [ ] Read APIs: `get_historical_features` vs. `get_online_features`
- [ ] Ingestion pipelines: streaming (Flink) vs. batch (Spark)
- [ ] Consistency: how offline and online stores stay in sync
- [ ] Backfill strategy for new features
- [ ] Availability: replication for online store

---

## Follow-Up Questions

1. "How does point-in-time correctness prevent data leakage? Give a concrete example."
2. "Your 10-minute rolling count feature updates every minute. How do you handle the sync between offline and online without the online store being too stale?"
3. "A data scientist wants to add a new feature computed from 2 years of historical data. How do you backfill it?"
4. "How would you implement feature versioning?"
5. "What happens to model quality if the online store is unavailable during inference?"
