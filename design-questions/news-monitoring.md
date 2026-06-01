# Design: Real-Time News Monitoring with Search and Re-Ingestion

**Difficulty:** Medium-Hard
**Time:** ~60 minutes
**Topics:** Kafka, pub/sub partitioning, database schema, ETL re-ingestion

---

## Overview

This question is asked in parts. Each part builds on the previous one. Read the problem statement for each part before designing your solution. It is normal to revisit earlier decisions as the requirements evolve.

---

## Part 1: Real-Time News Feed

### Problem Statement

Design a system that ingests a real-time stream of news articles from multiple third-party providers and delivers them to downstream consumers (dashboards, alerting services, ML pipelines).

Assume:
- Providers push articles via webhooks or you pull from their APIs.
- Volume: ~10,000 articles per hour at peak.
- Consumers need near-real-time delivery (seconds, not minutes).

### What to Design

Describe the end-to-end flow from source to consumer. Address:

1. **Ingestion layer:** How do you receive articles from providers? How do you normalize different provider schemas into a common format?
2. **Message broker:** Why use a message queue here rather than direct DB writes? Which broker (Kafka, RabbitMQ, Pulsar) and why?
3. **Consumer groups:** How do multiple downstream services each receive all articles independently?
4. **Delivery guarantees:** How do you ensure at-least-once delivery? What are the tradeoffs vs. exactly-once?
5. **Offset management:** If a consumer restarts after a crash, how does it know where to resume?

### Architecture Sketch

```
[Provider A webhook]  ──┐
[Provider B API poll] ──┼──> [Ingestion Service] ──> [Kafka Topic: news.raw]
[Provider C webhook]  ──┘         (normalize)
                                                         │
                                    ┌────────────────────┼────────────────────┐
                                    │                    │                    │
                              [Dashboard            [Alert             [ML Feature
                               Consumer]            Service            Pipeline
                               Group]               Consumer]          Consumer]
```

### Key Discussion Points

- **Schema normalization:** Providers have different field names, date formats, and encodings. An ingestion service translates each provider's format into a canonical `NewsArticle` schema before publishing to Kafka. This is the right place to handle provider-specific quirks.

- **Why Kafka over direct DB writes:** Kafka decouples producers from consumers. A slow downstream service cannot slow down ingestion. You can replay events. Multiple consumer groups can each consume the full stream independently.

- **Consumer groups:** Each logical downstream service creates its own consumer group. Kafka tracks each group's offset independently, so the dashboard service and the ML pipeline both receive every article, but progress independently.

- **At-least-once delivery:** Consumers commit offsets only after successfully processing a message. If a consumer crashes mid-processing, it will re-read and re-process the last uncommitted message on restart. This means your processing logic must be idempotent (e.g., upsert by article ID rather than blind insert).

- **Offset reset on reconnect:** When a new consumer group first connects, or when a group's stored offset is no longer valid (e.g., log segment has been deleted), you must choose a reset policy: `earliest` (replay all retained messages) or `latest` (skip old messages, start from now). For a new ML pipeline needing historical data, `earliest` makes sense. For a live dashboard, `latest` is appropriate.

### Follow-Up Questions

- How would you handle duplicate articles from different providers covering the same story?
- What retention period would you set for the Kafka topic? What is the storage cost?
- How do you monitor consumer lag? What alerts would you set up?

---

## Part 2: Tag-Based Filtering

### Problem Statement

Each news article is tagged with one or more topic tags (e.g., "technology", "finance", "climate", "earnings:AAPL"). There are approximately 10,000 distinct tags.

Consumers want to subscribe only to articles with specific tags rather than consuming the full stream.

### Options to Consider

Evaluate these three approaches:

**Option A: One Kafka topic per tag**
- Create a topic for each tag (e.g., `news.tag.technology`, `news.tag.finance`).
- Consumers subscribe only to their tags of interest.

**Option B: Client-side filtering**
- Keep a single `news.raw` topic.
- Each consumer reads all messages and filters locally by tag.

**Option C: Kafka partitioning by tag hash**
- Use a single topic with partitions assigned by hashing the primary tag.
- Consumers are assigned specific partitions corresponding to their tags of interest.

### Analysis

| Approach | Pros | Cons |
|---|---|---|
| One topic per tag | Perfect isolation; consumers read only what they need | 10,000 topics is operationally painful; most brokers impose topic limits; multi-tag articles require publishing to multiple topics (fan-out) |
| Client-side filtering | Simple; no broker changes needed | Every consumer reads the full stream; wasted I/O for low-volume tags |
| Partition by tag hash | Single topic; reduced waste vs. client-side; consumers can be assigned specific partitions | Multi-tag articles still need duplication or a separate index; partition count must be >> number of tags to avoid hotspots; consumers cannot subscribe to an arbitrary tag without owning the entire corresponding partition |

### Recommended Approach

For 10,000 tags, a hybrid works well in practice:

1. Keep a single `news.raw` topic for full ingestion.
2. Run a **tag router** consumer group that reads `news.raw` and writes to a smaller set of **category-level topics** (e.g., `news.category.finance`, `news.category.technology`), where categories are coarser groupings (tens, not thousands).
3. Consumers that need fine-grained tag filtering subscribe to the appropriate category topic and apply local filtering for the specific sub-tag.

For truly high-volume tags (e.g., "earnings:AAPL" during earnings season), you could create a dedicated topic dynamically.

**On partition count:** With 10,000 tags, you do not want 10,000 partitions either. A reasonable number is 50–200 partitions for the `news.raw` topic, sized for throughput rather than tag count. Partition by a hash of the article ID to distribute load evenly.

### Follow-Up Questions

- An article has 5 tags. How many times does it get published, and where?
- How do you handle a tag that is misspelled by the provider and later corrected?
- What happens when you need to add a new tag category that did not exist before?

---

## Part 3: Historical Search by Tag

### Problem Statement

Users now want to search historical articles by tag. A query might be: "Give me all articles tagged 'climate' from the last 30 days."

Kafka is not a good query layer. Design a storage and query layer for historical tag search.

### Database Schema

```sql
-- Core article record. article_id is the canonical deduplication key.
CREATE TABLE news_articles (
    article_id      UUID PRIMARY KEY,
    provider_id     VARCHAR(64) NOT NULL,
    provider_ref    VARCHAR(256),           -- Provider's own article ID
    title           TEXT NOT NULL,
    body            TEXT,
    published_at    TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    url             TEXT,
    language        VARCHAR(8)
);

-- Normalized tag vocabulary.
CREATE TABLE tags (
    tag_id      SERIAL PRIMARY KEY,
    name        VARCHAR(128) UNIQUE NOT NULL,   -- e.g. "climate", "earnings:AAPL"
    category    VARCHAR(64),                     -- e.g. "topic", "ticker", "region"
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Many-to-many: which tags apply to which articles.
CREATE TABLE article_tags (
    article_id  UUID NOT NULL REFERENCES news_articles(article_id),
    tag_id      INT  NOT NULL REFERENCES tags(tag_id),
    tagger_version VARCHAR(32),                  -- which version of the tagger assigned this
    tagged_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (article_id, tag_id)
);

-- Indexes for common query patterns
CREATE INDEX idx_article_tags_tag_id_published
    ON article_tags (tag_id, article_id);

CREATE INDEX idx_news_articles_published_at
    ON news_articles (published_at DESC);
```

### Search Service Layer

The search service sits between the API and the database:

```
[API Gateway]
     │
     ▼
[Search Service]
  - Parse query (tags, date range, language, pagination)
  - Translate tag names to tag_ids (can cache this mapping)
  - Build SQL or Elasticsearch query
  - Return paginated results
     │
     ├──> [PostgreSQL] for structured queries, small result sets
     │
     └──> [Elasticsearch / OpenSearch] for full-text search on body/title
```

**Query pattern for tag search:**

```sql
SELECT a.article_id, a.title, a.published_at, a.url
FROM news_articles a
JOIN article_tags at ON a.article_id = at.article_id
JOIN tags t ON at.tag_id = t.tag_id
WHERE t.name = 'climate'
  AND a.published_at >= NOW() - INTERVAL '30 days'
ORDER BY a.published_at DESC
LIMIT 50
OFFSET 0;
```

For multi-tag intersection queries ("climate AND finance"):

```sql
SELECT a.article_id, a.title, a.published_at
FROM news_articles a
WHERE a.article_id IN (
    SELECT at.article_id FROM article_tags at
    JOIN tags t ON at.tag_id = t.tag_id
    WHERE t.name IN ('climate', 'finance')
    GROUP BY at.article_id
    HAVING COUNT(DISTINCT t.name) = 2   -- must have both tags
)
AND a.published_at >= NOW() - INTERVAL '30 days'
ORDER BY a.published_at DESC;
```

### Follow-Up Questions

- How do you handle pagination for a result set of 1 million articles?
- When would you move from PostgreSQL to Elasticsearch for search?
- How do you keep the search index consistent with the database during high-ingest periods?

---

## Part 4: Re-Tagging After Model Update

### Problem Statement

After 1 year of operation, the ML team releases an improved tagging model (v2). The new model has a different taxonomy (some tags renamed, some split, some merged) and produces better recall on niche topics.

You need to re-tag all historical articles (assume ~100M articles) with the new model, while the live pipeline continues ingesting new articles.

### Approach: Offline Batch Re-Tagging with Parallel Live Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LIVE PIPELINE (unchanged)                   │
│  [Kafka: news.raw] ──> [Tagger v1] ──> [DB: article_tags v1]       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                         BATCH RE-TAGGING JOB                        │
│  [DB: news_articles] ──> [Spark/batch workers] ──> [Tagger v2]     │
│        (full scan)           (parallelized)                         │
│                                      │                              │
│                                      ▼                              │
│                     [DB: article_tags_v2 (shadow table)]            │
└─────────────────────────────────────────────────────────────────────┘
```

**Step-by-step migration plan:**

1. **Add a `tagger_version` column** to `article_tags` (already in schema above). New tags written by v2 carry `tagger_version = 'v2'`.

2. **Write to a shadow table.** Run the batch re-tagging job writing to `article_tags_v2`. Do not touch the live `article_tags` table during the batch run. This keeps the live service unaffected.

3. **Parallelize the batch job.** Partition the 100M articles by `article_id` range or `published_at` range. Run 100+ parallel workers. Use Spark or a distributed task queue (Celery, Ray). At 10,000 articles/second throughput, 100M articles takes ~3 hours.

4. **Handle the overlap window.** While the batch job runs (hours), the live pipeline continues writing new articles with v1 tags. Track a `batch_cutoff_time`. After the batch job completes all articles up to `batch_cutoff_time`, run a catch-up pass for articles written between `batch_cutoff_time` and the end of the batch run.

5. **Switch the live tagger.** After the batch job and catch-up are complete, deploy the v2 tagger to the live pipeline. New articles now get v2 tags directly.

6. **Validate and cut over.** Run shadow queries comparing v1 and v2 results on a sample. Check recall/precision metrics. If acceptable, update the search service to query `tagger_version = 'v2'` records (or drop the v1 records and remove the filter).

7. **Clean up.** Delete v1 tag records from `article_tags` (or archive them) after a confidence period.

### Key Design Decisions

- **Immutability during batch run:** Do not update existing rows in `article_tags`. Write v2 tags as new rows (different `tagger_version`). This lets you A/B test, roll back, or serve both versions simultaneously.

- **Idempotency:** If the batch job is interrupted and restarted, re-processing an already-processed article should be a no-op (upsert by `(article_id, tag_id, tagger_version)`).

- **Rate limiting the batch job:** The batch job and live pipeline share the database. Use a rate limiter or schedule the batch job during off-peak hours to avoid starving live traffic.

### Follow-Up Questions

- How do you handle articles that were ingested but not yet tagged by v1 when the batch job starts?
- The new taxonomy merges two tags ("climate" and "environment") into one ("climate-and-environment"). How does your schema handle this?
- How do you decide when to stop supporting v1 queries?

---

## General Follow-Up Questions

- What happens if the ingestion service goes down for 2 hours? How does the system recover?
- A provider sends a corrected version of an article that was published 6 hours ago. How do you handle the update?
- How would you design a dead letter queue for articles that fail to parse or tag?
- What metrics would you put on a monitoring dashboard for this system?
