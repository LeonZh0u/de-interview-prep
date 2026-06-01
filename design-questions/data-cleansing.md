# Design: Data Cleansing Pipeline

**Difficulty:** Medium
**Time:** ~30 minutes
**Focus areas:** ETL design, immutable storage, pipeline extensibility, error handling, scalability

---

## Problem Statement

You are ingesting a large Kafka topic to **Parquet files on S3**. The raw messages contain many data quality issues:

- Trailing whitespace in string fields
- `""` (empty string) used instead of NULL
- Incorrect text encoding (e.g., garbled UTF-8)
- Potential future issues: control characters, HTML entities, inconsistent date formats

**Critical constraint:** Parquet files are **immutable** — you cannot edit records in place. You must read and rewrite.

Design a system to clean the data for downstream usage.

---

## Step 1: Clarify Requirements

Questions to ask:
- How is "clean" defined — is there a schema contract that defines valid values?
- How often does data need to be cleaned? → Continuously as it arrives, or periodic batch?
- Do all downstream teams need the same cleaning, or team-specific transformations?
- How do we expose the cleaned data — same S3 path? A new path? A catalog entry?
- What's the expected data volume? Single file? TB/day?
- What happens to records that cannot be cleaned (e.g., unrecoverable encoding)?

---

## Step 2: High-Level Architecture

```
[Kafka Topic]  (raw, messy data)
      |
      v
[Raw Parquet on S3]  (ingested as-is — source of truth, never modified)
   s3://data/raw/year=2024/month=01/day=15/

      |
      v
[Cleansing Pipeline]  (Spark batch job, triggered daily or near-real-time)
      |
    Steps:
      1. Read raw parquet
      2. Apply cleaning transformations
      3. Write cleaned parquet to new path

      |
      v
[Clean Parquet on S3]
   s3://data/clean/year=2024/month=01/day=15/

      |
      v
[Data Catalog]  (Glue / Hive Metastore — registers the clean path)
      |
      v
[Downstream Consumers]
   (ML training jobs, Spark queries, analytics tools)
```

---

## Step 3: Cleaning Transformations

Each cleaning rule is an idempotent transformation:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def apply_cleaning(df):
    # 1. Trim trailing/leading whitespace from all string columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    # 2. Replace empty string "" with null
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col) == "", None).otherwise(F.col(col)))

    # 3. Fix encoding issues — flag or drop undecodable records
    df = df.withColumn(
        "_encoding_valid",
        F.col("body").rlike("^[\\x00-\\x7F\\x80-\\xFF]*$")  # simplified check
    )
    bad_encoding = df.filter(~F.col("_encoding_valid"))
    df = df.filter(F.col("_encoding_valid")).drop("_encoding_valid")

    # Log bad records count
    print(f"Dropped {bad_encoding.count()} records with encoding issues")

    return df
```

---

## Step 4: Frequency and Scheduling

### Option A: Daily Batch
- Triggered by Airflow after raw ingestion completes.
- Reads entire day's raw partition, cleans it, writes clean partition.
- Simple, predictable. Latency: up to 24 hours for cleaned data.

### Option B: Near-Real-Time (Spark Structured Streaming)
- Reads Kafka topic directly (bypassing raw S3 layer).
- Applies cleaning as data arrives.
- Writes clean micro-batches to S3.
- Latency: minutes. Complexity: higher (state management, checkpointing).

### Option C: Triggered Batch (Recommended for most cases)
- Raw ingestion completes each hour.
- Airflow detects new raw partition → triggers cleaning job for that hour.
- Latency: minutes to an hour. Simpler than streaming.

---

## Step 5: Adding New Cleaning Routines

The cleaning pipeline must be extensible without touching core infrastructure.

### Plugin Pattern

```python
class CleaningRule:
    """Base class for all cleaning rules."""
    def apply(self, df):
        raise NotImplementedError

class TrimWhitespaceRule(CleaningRule):
    def apply(self, df):
        # ... trim all string columns
        return df

class EmptyStringToNullRule(CleaningRule):
    def apply(self, df):
        # ... replace "" with null
        return df

class StripControlCharactersRule(CleaningRule):
    """New rule: remove ASCII control characters from strings."""
    def apply(self, df):
        for col in get_string_cols(df):
            df = df.withColumn(col, regexp_replace(col(col), r"[\x00-\x1F\x7F]", ""))
        return df

# Pipeline definition — add new rules here without changing anything else
CLEANING_RULES = [
    TrimWhitespaceRule(),
    EmptyStringToNullRule(),
    StripControlCharactersRule(),  # <- new rule added here
]

def run_pipeline(df):
    for rule in CLEANING_RULES:
        df = rule.apply(df)
    return df
```

---

## Step 6: Downstream Access

### Data Catalog Registration

After cleaning, register the clean partition in a catalog (e.g., AWS Glue or Hive Metastore):

```
Table: data_clean
Location: s3://data/clean/
Partitions: year, month, day
```

Downstream teams query:
```sql
SELECT * FROM data_clean WHERE year=2024 AND month=1 AND day=15;
-- Reads only the clean partition, not raw
```

**Key benefit:** Consumers never touch raw data. The catalog abstracts which path they should read.

---

## Step 7: Error Handling Strategy

| Error Type | Handling |
|---|---|
| Unrecoverable encoding | Drop record, write to quarantine path |
| Suspicious but parseable | Clean and tag with `_dirty_flag = true` |
| Entire partition unreadable | Alert, do not overwrite clean partition |
| Cleaning job fails mid-run | Write to temp path; only swap if job succeeds |

**Quarantine path:** `s3://data/quarantine/year=2024/month=01/day=15/` — inspected by data engineers.

---

## Step 8: Scaling to Double Input Rate

If input volume doubles:

- **Bottleneck 1: Spark cluster size** → Increase executor count; Spark scales horizontally.
- **Bottleneck 2: S3 write throughput** → Use multiple output partitions; avoid very small files (file consolidation step).
- **Bottleneck 3: Kafka consumer lag** → Add more Kafka partitions + more consumer instances.
- **Bottleneck 4: Cleaning job runtime** → Parallelize by partition; run partition-level jobs concurrently.

No architectural changes needed — the pipeline scales horizontally by design.

---

## What to Cover in Your Answer

- [ ] Two-layer S3 layout: raw (immutable) → clean (derived)
- [ ] Why you never modify raw data (audit trail, ability to re-clean)
- [ ] Cleaning transformations: trim, null, encoding — and how to apply idempotently
- [ ] Scheduling: batch vs. triggered vs. streaming trade-offs
- [ ] Extensibility: plugin/rule pattern so new routines don't require pipeline changes
- [ ] Downstream access: catalog abstraction hides the raw/clean distinction
- [ ] Error handling: quarantine for unrecoverable records, temp paths to prevent partial overwrites
- [ ] Scaling: what changes (executor count) vs. what doesn't (architecture)

---

## Follow-Up Questions

1. "A cleaning bug was deployed and corrupted 7 days of clean data. How do you recover?"
*(Answer: re-run the fixed cleaning job against the raw partitions — raw data was never modified.)*

2. "Different downstream teams need different cleaning rules (e.g., ML team needs HTML stripped, analytics team doesn't). How do you handle this?"
*(Answer: multiple derived paths / multiple pipeline variants, or parameterized cleaning configs.)*

3. "How do you ensure the cleaning job is idempotent?"
*(Answer: write to a temp path, verify row counts, then atomically swap to the final path.)*

4. "How do you test a new cleaning rule before deploying it to production?"
*(Answer: run against a sample partition in a staging environment; compare record counts before/after.)*

5. "What if the cleaning job's output is also immutable (e.g., Delta Lake with ACID)? Does your design change?"
