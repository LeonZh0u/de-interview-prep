# Design: Distributed Top-K Report

**Difficulty:** Medium
**Time:** ~30–45 minutes
**Focus areas:** MapReduce, data sharding, distributed aggregation, Spark internals, data skew

---

## Problem Statement

A `StockQuote` service records every ticker that users look up. The data is stored in a high-performance distributed database.

**Schema:**
```
quote_events: [event_ts: timestamp@utc,  equity_id: int,  user_id: int,  firm_id: int]
tickers:      [equity_id: int,  ticker: str]
```

**Scale:**
- ~10M concurrent users
- ~25GB of events per day

**Task:** Create a **daily top-100 ticker report** that excludes internal users.

---

## Step 1: Clarify Requirements

- What defines "internal users"? → Users with `firm_id` in a known internal set, or users in an internal user table.
- Is this a batch job (end-of-day) or real-time? → Start with daily batch; consider intra-day windows as a stretch goal.
- Where should the output go? → A report table, dashboard, or exported file.
- How is "top" defined? → By total lookup count for the day.
- Do we need to handle data from multiple days at once? → No, daily is sufficient.

---

## Step 2: MapReduce Design

### Conceptual Phases

**Map:**
- Input: raw `quote_events` records
- Filter: exclude internal users
- Emit: `(equity_id, 1)` per qualifying event

**Shuffle:**
- Framework groups all `(equity_id, count)` pairs — all events for the same ticker land on the same reducer

**Reduce:**
- Input: `(equity_id, [1, 1, 1, ...])`
- Sum: `(equity_id, total_count)`

**Post-reduce:**
- Sort by count descending, take top 100
- Join `equity_id` with `tickers` table to get ticker symbol

### Physical Operations Happening

1. **Filter** — local, no network cost
2. **Shuffle** — all `equity_id` values are moved across the network to group by key (most expensive step)
3. **Sort** within each reducer — `O(N log N)` per partition
4. **Final sort + limit** — `ORDER BY count DESC LIMIT 100` across all reducers

---

## Step 3: Spark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, desc

spark = SparkSession.builder.appName("daily-top-100").getOrCreate()

# Load data
events = spark.read.parquet("s3://events/date=2024-01-15/")
tickers = spark.read.parquet("s3://tickers/")  # small: ~30K rows
internal_users = spark.read.parquet("s3://internal-users/")  # small

# Filter out internal users
events_filtered = events.join(
    broadcast(internal_users),
    on="user_id",
    how="left_anti"  # keep events NOT in internal_users
)

# Aggregate
top_tickers = (
    events_filtered
    .groupBy("equity_id")
    .count()
    .orderBy(desc("count"))
    .limit(100)
    .join(broadcast(tickers), on="equity_id")
    .select("ticker", "count")
)

top_tickers.write.mode("overwrite").csv("s3://reports/top-100/date=2024-01-15/")
```

### Key Question: Where Do You Join the Ticker Names?

**Join AFTER aggregation, not before.**

Reason: Joining 25GB of events with the 30K-row tickers table *before* grouping would process 25GB × join overhead. Joining 100 rows *after* taking `limit(100)` is essentially free.

Also use a **broadcast join** for the tickers table — it's small (30K rows), so it can be sent to every executor, avoiding a shuffle.

---

## Step 4: Exception Handling

```python
try:
    events = spark.read.parquet("s3://events/date=2024-01-15/")
except Exception as e:
    # Alert on-call, write to error log, exit
    logger.error(f"Failed to read events: {e}")
    raise

# Handle bad records
events_cleaned = events.filter(
    col("equity_id").isNotNull() & col("user_id").isNotNull()
)

# Log count of dropped records
dropped = events.count() - events_cleaned.count()
logger.info(f"Dropped {dropped} malformed records")
```

---

## Step 5: Scale Challenges

### Data Skew
- Some tickers (e.g., AAPL, MSFT) may have dramatically more events than others.
- This means one reducer handles far more data than others → that reducer is the bottleneck.
- **Fix:** Salting — append a random suffix to the key for the first aggregation pass, then re-aggregate to remove the suffix.

### Partition Count
- With 25GB of data, Spark default 128MB partitions → ~200 partitions.
- Too few partitions → large tasks, memory pressure.
- Too many → overhead from task scheduling.
- Tune with `spark.sql.shuffle.partitions`.

### Join Skew
- `left_anti` join with internal_users: if the internal_users table is large, this shuffle can be expensive.
- If it's small (thousands of users), use `broadcast(internal_users)`.

---

## Step 6: Intra-Day Windows (Stretch Goal)

For real-time / intra-day leaderboards:
- Use **Spark Structured Streaming** or **Flink** reading from Kafka.
- Define tumbling windows: `window("event_ts", "1 hour")`.
- Emit top-100 per window to a Redis sorted set for fast retrieval.
- Challenge: late arrivals — use **watermarks** to define how long to wait for late events.

---

## Step 7: Trend Detection (Stretch Goal)

To show trending tickers (spike vs. baseline):
- Compare today's top-100 counts to a 7-day moving average.
- Store daily aggregation results in a time-series table.
- Compute: `trend_score = today_count / avg(last_7_days_count) - 1`
- A positive trend_score indicates a spike.

---

## What to Cover in Your Answer

- [ ] Map/Shuffle/Reduce phases clearly described
- [ ] Filter step placed correctly (before groupBy)
- [ ] Join placement: after aggregation, using broadcast for small tables
- [ ] Exception handling for bad records and source unavailability
- [ ] Data skew: identification and mitigation
- [ ] Spark-specific: stages, tasks, broadcast join, `left_anti` join
- [ ] Intra-day extension: streaming approach

---

## Key Questions to Expect

1. "Walk me through the physical operations your Spark job performs."
2. "Where did you join the ticker names and why?"
3. "AAPL gets 30% of all lookups. What does this do to your Spark job?"
4. "How do you offer hourly leaderboards instead of just daily?"
5. "If the events table is written as a Hive table, how does your query differ?"
6. "What Spark config would you tune first if the job is running slowly?"
