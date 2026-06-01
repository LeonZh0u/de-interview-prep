# Design: News Article Time-Series Aggregation

**Difficulty:** Medium
**Time:** ~45 minutes
**Focus areas:** ETL design, MapReduce-style aggregation, storage selection, API design, batch vs. streaming

---

## Problem Statement

Design a system for a news platform that lets clients retrieve a **time series of company mention counts** between a start and end date.

News articles are tagged with all companies referenced in them. You must:
1. Aggregate daily mention counts per company across a 10-year history.
2. Serve queries like: `get_timeseries(start_date, end_date, [companies])`
3. Return a time series where ANY of the listed companies was mentioned.

### Example Article Record
```json
{
    "id": "BBTYYIAT142",
    "ts": 1614438904,
    "tags": ["MSFT", "TWT", "IBM"]
}
```

### Example Query
```
get_timeseries("2021-01-01", "2021-06-30", ["MSFT", "GOOG"])
→ {
    "2021-01-01": 143,
    "2021-01-02": 97,
    ...
  }
```

### Scale
- **10 years** of history, ~4 billion articles
- ~30,000 unique companies
- Historical data (up to yesterday); real-time queue retains 48 hours

---

## Step 1: Clarify Requirements

Questions to ask:
- Historical or real-time? → Historical (up to end of previous day). Optional: real-time extension.
- Granularity of time buckets? → Daily.
- Query semantics: ANY company or ALL companies? → Start with ANY; ALL is a stretch goal.
- Read vs. write ratio? → Many more reads than writes.
- Latency SLA for queries? → Should be fast (sub-second ideally).

---

## Step 2: API Design

```python
def get_timeseries(
    start_date: str,      # "YYYY-MM-DD"
    end_date: str,        # "YYYY-MM-DD"
    companies: list[str]  # ["MSFT", "GOOG"]
) -> dict[str, int]:
    """
    Returns daily mention counts where ANY listed company appears.
    Key = date string, Value = total article count.
    """
```

**ANY semantics implementation:** Retrieve the time series for each company individually, then union-merge by date (sum the counts). Note: this overcounts if an article mentions multiple queried companies on the same day → handle with OR logic at storage level if needed.

---

## Step 3: Aggregation Approach (MapReduce / Spark)

The 10-year history cannot be processed on a single machine. Use a distributed batch job.

### Spark / MapReduce Pipeline

```
Source: S3/HDFS — 10 years of articles (JSON/Parquet)
    |
    v
[Map phase]
  For each article:
    For each tag in article.tags:
      emit (tag, date(article.ts)) → 1
    |
    v
[Shuffle] — group by (tag, date)
    |
    v
[Reduce phase]
  (tag, date, [1, 1, 1, ...]) → (tag, date, count)
    |
    v
[Load] — write (tag, date, count) to time-series store
```

In Spark:
```python
articles_df \
    .select(explode("tags").alias("ticker"), date_trunc("day", "ts").alias("date")) \
    .groupBy("ticker", "date") \
    .count() \
    .write.mode("overwrite") \
    .partitionBy("ticker") \
    .parquet("s3://aggregations/daily-company-counts/")
```

---

## Step 4: Storage Design

### Target Store Selection

The aggregated result is essentially a **key-value time series**:
- Key: `(ticker, date)` or `ticker` → sorted list of `(date, count)`
- Access pattern: range query by date for one or more tickers

**Options:**

| Store | Pros | Cons |
|---|---|---|
| Redis (sorted set) | Sub-ms range queries; sorted by timestamp score | Memory-limited; not great for 30K × 3650 days |
| Cassandra | Horizontal scale; time-series-friendly clustering keys | Slightly higher latency than Redis |
| PostgreSQL + index | Simple; good for historical queries | Single-node bottleneck at scale |
| Parquet on S3 | Cheap at any scale | High latency for ad-hoc queries |

**Recommended:** Cassandra with schema:
```
PRIMARY KEY (ticker, date)
CLUSTERING ORDER BY (date ASC)
```
This allows efficient range scans: `WHERE ticker = 'MSFT' AND date >= '2021-01-01' AND date <= '2021-06-30'`

---

## Step 5: Full ETL Pipeline

```
[Source 1: Historical Store (S3/HDFS)]
    |
    v
[Spark Batch Job] — runs daily for incremental update
    — extracts new articles from previous day
    — computes (ticker, date, count) aggregations
    — upserts into Cassandra
    |
[Source 2: Live Kafka Queue (48hr retention)]
    |
    v
[Optional: Streaming Job (Flink/Spark Streaming)]
    — computes running counts for today
    — writes to a "today" staging table in Cassandra
    |
    v
[Query Layer]
    — for historical dates: read from Cassandra aggregation table
    — for today: read from staging + merge with historical
```

---

## Step 6: Performance Considerations

- **Partitioning by ticker:** Ensures each company's data is co-located on a few Cassandra nodes — range scans are efficient.
- **Multi-company queries (ANY):** Fetch each company's series independently, merge in application layer (union by date, sum counts). Cache popular combinations.
- **Cache:** Use Redis for frequently queried tickers (e.g., MSFT, AAPL).
- **Precompute popular date ranges:** Top 100 tickers × common date ranges can be precomputed and cached at startup.

---

## Step 7: Advanced — ALL Companies (Intersection)

For `ALL` semantics (article must mention ALL listed companies):
- The current approach (sum per ticker) doesn't work — you'd overcount.
- Instead: at aggregation time, generate counts for compound keys, e.g., `(MSFT,GOOG)` per day.
- Problem: combinatorial explosion (30K companies → billions of pairs).
- Practical solution: require clients to query at the article level and apply intersection in the query layer, or limit compound queries to small sets.

---

## What to Cover in Your Answer

- [ ] API design: function signature, return type, semantics (ANY vs ALL)
- [ ] MapReduce/Spark approach: map phase (explode tags), shuffle, reduce (aggregate), output
- [ ] Storage choice: why Cassandra (or alternative), schema design
- [ ] ETL pipeline: historical bulk load vs. incremental daily job
- [ ] Streaming extension: how to handle real-time (today's) data
- [ ] Query-time merging for multi-company ANY queries
- [ ] Caching strategy for hot tickers
- [ ] Fault tolerance: what if the daily Spark job fails halfway?

---

## Follow-Up Questions

1. "How does your system handle a correction to an article's tags retroactively?"
2. "If article volume doubles, what part of your system breaks first?"
3. "Design the ALL semantics — articles mentioning ALL companies in the list."
4. "How would you expose intra-day (hourly) buckets in addition to daily?"
5. "How do you show trends (week-over-week change) efficiently?"
