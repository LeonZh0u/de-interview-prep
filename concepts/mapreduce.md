# MapReduce (Concept)

MapReduce is a programming model for processing large datasets in parallel across a distributed cluster. While the original Hadoop MapReduce framework is largely superseded by Spark, the **conceptual model** underlies nearly all modern big-data processing — Spark, Flink, Hive, and even streaming systems.

---

## The Core Idea

Break a large computation into two phases that can run in parallel:

1. **Map:** Apply a function to each input record independently → produces intermediate key-value pairs.
2. **Reduce:** Group all values by key, then aggregate each group.

Between Map and Reduce there is an implicit **Shuffle & Sort** phase that moves data so all values for a given key land on the same reducer.

```
Input Data (sharded)
   |
   v
[Map Workers] — each processes its own shard independently
   |
   v
[Shuffle & Sort] — network transfer; groups by key
   |
   v
[Reduce Workers] — each processes one group of keys
   |
   v
Output
```

---

## Step-by-Step Example: Word Count

**Input:** A large text file split into chunks across 100 machines.

**Map phase** (runs in parallel on each chunk):
```
"the quick fox" → [("the", 1), ("quick", 1), ("fox", 1)]
"the lazy dog"  → [("the", 1), ("lazy", 1),  ("dog", 1)]
```

**Shuffle & Sort** (framework handles this):
```
"the"   → [1, 1, 1, ...]
"quick" → [1]
"fox"   → [1]
...
```

**Reduce phase** (runs in parallel per key):
```
("the",   [1, 1, 1, ...]) → ("the",   157)
("quick", [1])            → ("quick", 1)
```

---

## Example: Daily Top-100 Ticker Report

This is a real interview question. The dataset is 25GB of stock quote events per day:
```
[event_ts: timestamp, equity_id: int, user_id: int, firm_id: int]
```

**MapReduce approach:**

**Map phase:**
- Input: raw event records.
- Filter: exclude internal users.
- Emit: `(equity_id, 1)` for each qualifying event.

**Shuffle:** Framework moves all `(equity_id, count)` pairs so all events for the same ticker land on the same reducer.

**Reduce phase:**
- Input: `(equity_id, [1, 1, 1, ...])`
- Sum: `(equity_id, total_count)`

**Post-reduce (partial sort):** Take the top 100 by count. In Spark, this is `takeOrdered(100, key=lambda x: -x[1])`.

**Spark equivalent:**
```python
events.filter(~col("user_id").isin(internal_users)) \
      .groupBy("equity_id") \
      .count() \
      .join(tickers, on="equity_id") \  # broadcast join — tickers table is small
      .orderBy(desc("count")) \
      .limit(100)
```

**Key question from interviewers:** "Where do you join the ticker name to the equity_id, and why?" — Answer: Join *after* aggregation (not before). Joining a small lookup table after reduces shuffle cost. Use a **broadcast join** in Spark.

---

## Physical Operations in MapReduce / Spark

Understanding what happens physically is a differentiator in interviews.

| Logical Op | Physical Operation | Cost |
|---|---|---|
| `groupBy` / `reduceByKey` | **Shuffle** — data moves across network | High |
| `join` (large + large) | **Shuffle join** — both sides repartition by join key | Very high |
| `join` (large + small) | **Broadcast join** — small table sent to all workers | Low |
| `filter` | **Local** — no network | Very low |
| `sort` within reducer | **Sort** — O(N log N) per partition | Medium |
| `repartition(n)` | Full shuffle into n partitions | High |
| `coalesce(n)` | Merge partitions without full shuffle | Low |

---

## Spark Internals (Frequently Asked)

### Job / Stage / Task
- **Job:** A complete Spark action (e.g., `count()`, `collect()`). One job per action.
- **Stage:** A set of operations that can run without a shuffle. A job is split into stages at each shuffle boundary.
- **Task:** The unit of execution — one task per partition per stage. Tasks run in parallel on executors.

**Example:** `filter → groupBy → count` → 2 stages:
- Stage 1: `filter` (no shuffle)
- Stage 2: `groupBy + count` (after shuffle)

### UDFs (User-Defined Functions)
- **SQL UDF:** Defined in SQL/HQL — limited expressiveness.
- **Python UDF:** Runs row-by-row in Python; serialization overhead (slow).
- **Pandas UDF (Vectorized UDF):** Operates on batches via Arrow; much faster than Python UDFs.
- **Scala/Java UDF:** Native JVM; fastest.

**Interview angle:** "Avoid Python UDFs on hot paths; use built-in Spark functions or Pandas UDFs instead."

### Broadcast Join
A small table is serialized and sent to every executor so the join is done locally without shuffling the large table.

```python
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), on="key")
```

### Apache Arrow
- Columnar in-memory format shared between JVM and Python.
- Enables zero-copy data transfer between Spark and Pandas UDFs.
- Eliminates row-by-row serialization overhead.

### Broadcast Variables
Immutable shared data sent once to all executors. Avoids re-sending the same data with each task.

---

## Data Skew in MapReduce/Spark

**Skew:** Some keys have vastly more records than others → one reducer becomes a bottleneck while others finish early.

**Example:** In a `groupBy("ticker")`, if `AAPL` has 10x more events than any other ticker, the AAPL reducer is the bottleneck.

**Solutions:**
1. **Salting:** Append a random number to the key (`AAPL_1`, `AAPL_2`, ...), process in sub-groups, then re-aggregate.
2. **Repartitioning:** Use a higher-cardinality composite key.
3. **Skew join hint:** Spark 3.x has a `skew join` optimization that auto-splits skewed partitions.

---

## Streaming vs. Batch MapReduce

| | Batch (Spark/Hadoop) | Streaming (Kafka Streams / Flink / Spark Streaming) |
|---|---|---|
| Input | Bounded dataset (e.g., a day's events) | Unbounded stream (continuous events) |
| Latency | Minutes to hours | Milliseconds to seconds |
| When to use | Historical aggregations, model training data | Real-time dashboards, fraud detection, alerts |
| State management | Stateless between jobs | Stateful (windows, watermarks, exactly-once) |

**Key insight:** "Can I implement batch using a stream?" Yes — treat the batch as a finite stream with a start and end watermark. Tradeoff: more complex state management, and you lose some batch optimizations (e.g., sort-based shuffle).

---

## Key Interview Questions to Practice

1. **Explain MapReduce for a non-trivial aggregation:** "Describe the Map, Shuffle, and Reduce phases for computing a daily top-100 ticker report from 25GB of events."
2. **Spark stages:** "I run `df.filter(...).groupBy('ticker').count().orderBy(desc('count')).limit(100)`. How many Spark stages are created, and where are the shuffle boundaries?"
3. **Broadcast join:** "Your ticker dimension table has 30,000 rows and your events table has 4 billion rows. How do you join them efficiently?"
4. **Data skew:** "One ticker (AAPL) accounts for 30% of all events. How does this affect your MapReduce job and how do you fix it?"
5. **UDF choice:** "You need to apply a custom normalization formula to every row of a 10B-row DataFrame. What's the fastest way in Spark?"
6. **Cardinality:** "Why does low cardinality in your groupBy key cause performance problems in a distributed system?"
