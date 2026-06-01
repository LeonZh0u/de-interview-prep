# Coding: Top-K (MapReduce & Distributed Approaches)

**Difficulty:** Easy → Medium (with distributed extension)
**Time:** ~30–45 minutes
**Languages:** Python
**Focus areas:** Heap-based algorithms, sorting, MapReduce pattern, distributed top-K, data skew

---

## Problem Statement

Given a stream or large dataset of stock ticker lookups, find the **top-K most frequently looked-up tickers** in a single day.

Variant A (coding): Implement in Python using in-memory data.
Variant B (design): Describe the distributed approach using MapReduce / Spark.

---

## Variant A: In-Memory Top-K

### Approach 1: Sort (Simple but slow for large N)

```python
from collections import Counter

def top_k_sort(events: list[str], k: int) -> list[tuple[str, int]]:
    """
    O(N log N) time, O(N) space
    N = number of events
    """
    counts = Counter(events)
    return counts.most_common(k)

# Test
events = ["AAPL", "MSFT", "AAPL", "GOOG", "AAPL", "MSFT", "TSLA"]
print(top_k_sort(events, 2))
# [('AAPL', 3), ('MSFT', 2)]
```

**Limitation:** O(N log N) — sorting all N unique tickers when you only need K.

---

### Approach 2: Min-Heap (Optimal for large N, small K)

```python
import heapq
from collections import Counter

def top_k_heap(events: list[str], k: int) -> list[tuple[str, int]]:
    """
    O(N + U log K) time, O(U + K) space
    N = events, U = unique tickers
    Maintains a min-heap of size K — only K tickers in memory at once.
    """
    counts = Counter(events)

    # Min-heap of (count, ticker) — min-heap pops the smallest count
    heap = []
    for ticker, count in counts.items():
        heapq.heappush(heap, (count, ticker))
        if len(heap) > k:
            heapq.heappop(heap)  # remove the smallest

    # Result is in ascending order; reverse for descending
    return sorted(heap, reverse=True)

# Test
events = ["AAPL", "MSFT", "AAPL", "GOOG", "AAPL", "MSFT", "TSLA"]
print(top_k_heap(events, 2))
# [(3, 'AAPL'), (2, 'MSFT')]
```

**Why min-heap?** A min-heap of size K keeps the K largest seen so far. When a new count exceeds the heap's minimum, it replaces the minimum.

**Complexity comparison:**

| Approach | Time | Space | When to use |
|---|---|---|---|
| Sort | O(N log N) | O(N) | Simple, N is manageable |
| Min-heap | O(N + U log K) | O(U + K) | Large N, small K |
| QuickSelect | O(N) avg | O(N) | Exact K-th element without sorting |

---

### Approach 3: QuickSelect (O(N) average, no sort)

```python
import random
from collections import Counter

def top_k_quickselect(events: list[str], k: int) -> list[tuple[str, int]]:
    """
    O(N) average time for finding top-K without fully sorting.
    Uses the QuickSelect algorithm (partition around a pivot).
    """
    counts = list(Counter(events).items())  # [(ticker, count), ...]

    def partition(arr, lo, hi):
        pivot = arr[hi][1]  # pivot on count
        i = lo
        for j in range(lo, hi):
            if arr[j][1] >= pivot:  # descending order
                arr[i], arr[j] = arr[j], arr[i]
                i += 1
        arr[i], arr[hi] = arr[hi], arr[i]
        return i

    def quickselect(arr, lo, hi, k):
        if lo >= hi:
            return
        pivot_idx = random.randint(lo, hi)
        arr[pivot_idx], arr[hi] = arr[hi], arr[pivot_idx]
        p = partition(arr, lo, hi)
        if p == k - 1:
            return
        elif p < k - 1:
            quickselect(arr, p + 1, hi, k)
        else:
            quickselect(arr, lo, p - 1, k)

    quickselect(counts, 0, len(counts) - 1, k)
    return sorted(counts[:k], key=lambda x: -x[1])

# Test
events = ["AAPL", "MSFT", "AAPL", "GOOG", "AAPL", "MSFT", "TSLA"]
print(top_k_quickselect(events, 2))
# [('AAPL', 3), ('MSFT', 2)]
```

**Trade-off:** O(N) average but O(N²) worst case (mitigated by random pivot selection). The top-K result from QuickSelect is not fully sorted — add a sort of the K elements if order matters (O(K log K) — small).

---

## Variant B: Distributed Top-K (MapReduce / Spark)

### Problem at Scale

The dataset is 25GB/day, across millions of events. Doesn't fit on one machine. Need MapReduce.

### MapReduce Design

**Map phase** (runs on each data shard in parallel):
```
Input:  [AAPL, MSFT, AAPL, GOOG, TSLA, AAPL, ...]
Output: [(AAPL, 1), (MSFT, 1), (AAPL, 1), (GOOG, 1), ...]
```

**Combiner** (optional local pre-aggregation before shuffle — reduces network traffic):
```
[(AAPL, 1), (AAPL, 1), (MSFT, 1)] → [(AAPL, 2), (MSFT, 1)]
```

**Shuffle:** Framework routes all (ticker, count) pairs so same ticker lands on same reducer.

**Reduce phase:**
```
Input:  (AAPL, [1, 2, 3, 2, ...])
Output: (AAPL, 157432)
```

**Final step:** All reducers output their (ticker, total_count) pairs. A final sort + limit picks the global top-K.

### Spark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, broadcast, desc

spark = SparkSession.builder.appName("top-k-tickers").getOrCreate()

events_df = spark.read.parquet("s3://events/date=2024-01-15/")
tickers_df = spark.read.parquet("s3://reference/tickers/")   # small: ~30K rows
internal_users = spark.read.parquet("s3://reference/internal-users/")  # small

# Step 1: Filter internal users (broadcast small table)
external_events = events_df.join(
    broadcast(internal_users),
    on="user_id",
    how="left_anti"
)

# Step 2: Count per ticker (groupBy = shuffle + aggregate)
ticker_counts = external_events.groupBy("equity_id").count()

# Step 3: Top-K (sort + limit)
top_k = ticker_counts.orderBy(desc("count")).limit(100)

# Step 4: Join ticker names AFTER aggregation (small join — cheap)
result = top_k.join(broadcast(tickers_df), on="equity_id") \
              .select("ticker", "count")

result.write.mode("overwrite").csv("s3://reports/top-100/")
```

### Physical Operations (What Spark Does)

1. `filter` / `left_anti` join with broadcast — **local, no shuffle**
2. `groupBy("equity_id")` → **shuffle** (most expensive step — all events move across network by ticker hash)
3. `orderBy` → **sort** (in each partition, then merge sort)
4. `limit(100)` → **reduce** to driver (only 100 rows)
5. `join(broadcast(tickers_df))` → **local** (no shuffle — tickers broadcast to all nodes)

### Important Design Decision: Where to Join Ticker Names?

**Join AFTER groupBy + limit, not before.**

Before: `events (25GB) JOIN tickers (small)` → then groupBy
- The join operates on 25GB of data.

After: `top_100_result (100 rows) JOIN tickers`
- The join operates on 100 rows → nearly free.

Use `broadcast(tickers_df)` regardless — tickers table is small enough to broadcast.

---

## Data Skew Considerations

If AAPL has 30% of all events:
- The reduce task handling AAPL gets 30% of the shuffle data.
- All other reducers finish; AAPL's reducer is still running → bottleneck.

**Fix — Salting:**
```python
# Add random salt to key for first aggregation pass
import pyspark.sql.functions as F

salted = events_df.withColumn(
    "salted_key",
    F.concat(col("equity_id"), F.lit("_"), (F.rand() * 10).cast("int").cast("string"))
)

# First pass: partial aggregation per salted key
partial = salted.groupBy("salted_key").count()

# Second pass: strip salt, re-aggregate to get true totals
result = partial.withColumn("equity_id", F.split(col("salted_key"), "_")[0]) \
                .groupBy("equity_id") \
                .agg(F.sum("count").alias("count"))
```

---

## What to Cover in Your Answer

**Coding:**
- [ ] Count frequencies (Counter)
- [ ] Min-heap of size K to maintain top-K efficiently
- [ ] Complexity: O(N + U log K) for heap approach
- [ ] When to use QuickSelect vs. heap vs. sort
- [ ] Edge cases: K > number of unique tickers; empty input

**Distributed:**
- [ ] Map/Combiner/Shuffle/Reduce phases
- [ ] Broadcast join for small tables (ticker lookup)
- [ ] Join placement: after aggregation, not before
- [ ] Data skew: salting technique
- [ ] Spark physical operations: which steps cause shuffles

---

## Follow-Up Questions

1. "You have 30,000 unique tickers and K=100. Is the heap approach O(N log 30000) or O(N log 100)?" *(Answer: O(N + 30000 log 100) — heap is size K.)*
2. "In your Spark job, how many stages are created?"
3. "The `orderBy` in Spark — is it a full sort of all partitions or just a top-K?" *(Answer: Spark's limit+orderBy is optimized — it doesn't sort everything.)*
4. "What happens to your MapReduce job if a reducer node crashes?"
5. "How would you compute the top-K for each hour of the day simultaneously in Spark?"
