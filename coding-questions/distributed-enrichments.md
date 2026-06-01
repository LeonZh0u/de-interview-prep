# Coding: Distributed Stream Aggregation (Enrichments)

**Difficulty:** Medium → Hard (with scaling portion)
**Time:** ~45 minutes
**Languages:** Python
**Focus areas:** Stream processing, stateful aggregation, timeouts, sharding/routing, fault tolerance

---

## Context

A news platform processes documents through many NLP enrichers in parallel (sentiment analysis, entity linking, topic classification, etc.). Each enricher works at its own speed and writes results to a shared queue.

Your job: build an **aggregator** that collects all expected enrichments for a document and emits a merged output once all are received (or a deadline is hit).

---

## Problem 1: Basic Aggregator (No Timeouts)

### Input Stream

Messages arrive on a queue, one at a time:
```json
{"document_id": "doc1", "enricher_id": "Sentiment Analysis",   "enrichment": "positive"}
{"document_id": "doc2", "enricher_id": "Entity Linking",        "enrichment": ["AAPL"]}
{"document_id": "doc1", "enricher_id": "Topic Classification",  "enrichment": ["AUTO"]}
{"document_id": "doc1", "enricher_id": "Entity Linking",        "enrichment": ["TSLA"]}
```

Your aggregator is configured with a set of enricher IDs it needs to collect (e.g., `{"Sentiment Analysis", "Entity Linking"}`).

### Output

As soon as all expected enrichments are received for a document, emit:
```json
{
    "document_id": "doc1",
    "enricher_ids": ["Sentiment Analysis", "Entity Linking"],
    "enrichments": {
        "Sentiment Analysis": "positive",
        "Entity Linking": ["TSLA"]
    }
}
```

### Approach

```python
class Aggregator:
    def __init__(self, required_enrichers: set[str]):
        self.required_enrichers = required_enrichers
        # Maps document_id → { enricher_id → enrichment value }
        self.pending: dict[str, dict] = {}

    def process(self, message: dict) -> dict | None:
        doc_id = message["document_id"]
        enricher_id = message["enricher_id"]
        enrichment = message["enrichment"]

        # Ignore enrichers we don't care about
        if enricher_id not in self.required_enrichers:
            return None

        # Store the enrichment
        if doc_id not in self.pending:
            self.pending[doc_id] = {}
        self.pending[doc_id][enricher_id] = enrichment

        # Check if all required enrichments have been received
        if self.pending[doc_id].keys() == self.required_enrichers:
            result = {
                "document_id": doc_id,
                "enricher_ids": list(self.required_enrichers),
                "enrichments": self.pending.pop(doc_id),  # release memory
            }
            return result

        return None
```

### Key Points
- Release memory once a document is fully aggregated (`pop`).
- Ignore enrichments for enrichers not in `required_enrichers`.
- Complexity: O(1) per message (dict lookup + set comparison).

---

## Problem 2: Aggregator with Timeouts

**New requirement:** Downstream consumers need aggregations within `delay` seconds of document publication. If not all enrichments arrive within the deadline, emit a **partial aggregation** with whatever was received.

### Updated Input (includes document publication time)
```json
{"document_id": "doc1", "document_toa": "2025-08-10 15:46:44", "enricher_id": "Sentiment Analysis", "enrichment": "positive"}
{"document_id": "doc1", "document_toa": "2025-08-10 15:46:44", "enricher_id": "Entity Linking",       "enrichment": ["TSLA"]}
```

### Approach: Background Timer per Document

```python
import threading
from datetime import datetime, timedelta

class AggregatorWithTimeout:
    def __init__(self, required_enrichers: set[str], delay_seconds: float):
        self.required_enrichers = required_enrichers
        self.delay_seconds = delay_seconds
        self.pending: dict[str, dict] = {}
        self.timers: dict[str, threading.Timer] = {}
        self.lock = threading.Lock()

    def _emit(self, doc_id: str):
        with self.lock:
            enrichments = self.pending.pop(doc_id, None)
            self.timers.pop(doc_id, None)
        if enrichments:
            print({
                "document_id": doc_id,
                "enricher_ids": list(self.required_enrichers),
                "enrichments": enrichments,
            })

    def process(self, message: dict):
        doc_id = message["document_id"]
        enricher_id = message["enricher_id"]
        enrichment = message["enrichment"]
        doc_toa = datetime.fromisoformat(message["document_toa"])

        if enricher_id not in self.required_enrichers:
            return

        with self.lock:
            # First enrichment for this document → start the timer
            if doc_id not in self.pending:
                self.pending[doc_id] = {}
                remaining = self.delay_seconds - (datetime.now() - doc_toa).total_seconds()
                if remaining <= 0:
                    return  # Already past deadline
                timer = threading.Timer(remaining, self._emit, args=[doc_id])
                self.timers[doc_id] = timer
                timer.start()

            self.pending[doc_id][enricher_id] = enrichment

            # All enrichments received early — cancel timer and emit now
            if self.pending[doc_id].keys() == self.required_enrichers:
                timer = self.timers.pop(doc_id, None)
                if timer:
                    timer.cancel()
                enrichments = self.pending.pop(doc_id)

        if 'enrichments' in dir():  # emitted early
            print({
                "document_id": doc_id,
                "enrichments": enrichments,
            })
```

### Key Design Trade-offs

| Approach | Guarantees deadline? | Memory? | Complexity |
|---|---|---|---|
| Fire on each new enrichment | No (misses documents with no late enrichment) | Low | Low |
| Background thread per document | Yes | High (one thread per in-flight doc) | Medium |
| `asyncio` coroutine per document | Yes | Low (coroutines are cheap) | Medium |
| Single background thread with sorted deadline queue | Yes | Low | Higher |

**Threading concern:** If there can be millions of in-flight documents, one thread per document doesn't scale. Use `asyncio` coroutines or a single background sweeper.

---

## Problem 3: Scaling Up (Architecture / Design)

**New constraints:**
- Millions of documents per day
- Tens of aggregator types running simultaneously
- Aggregator nodes can fail randomly or deterministically

### Key Scaling Questions

#### How do you route enrichments to the right aggregator instance?

**Option A: Hash by document_id (consistent routing)**
- `shard = hash(document_id) % num_aggregators`
- All enrichments for `doc1` always go to the same aggregator instance.
- Pros: No cross-node coordination. Simple state management.
- Cons: If that instance fails, in-progress aggregations for its documents are lost.

**Option B: Random / Round-robin**
- Enrichments for `doc1` could land on different instances.
- Problem: No single instance has all enrichments for a document.
- Requires a **shared external state store** (e.g., Redis, Cassandra) to merge enrichments across nodes.

#### Handling Aggregator Node Failure (with hash-based routing)

If an aggregator node fails:
- Re-route its document partitions to surviving nodes (or a replacement node).
- The in-flight documents on the failed node are lost if state is in memory only.
- **Fix:** Write enrichments to an external store (Redis, Cassandra) as they arrive. On recovery, reload state from the store.

Alternatively: Use Kafka with **commit-on-emit semantics**.
- Consumer commits its offset only after emitting the aggregation.
- On restart, the consumer re-reads uncommitted messages.
- Requires idempotent emit (downstream must handle duplicate aggregations).

#### Architecture Sketch

```
Kafka (enriched events)
    |
    v
[Load Balancer / Kafka Partitioner]
    — partition by document_id → same partition → same consumer always
    |
    v
[Aggregator Service instances (N nodes)]
    — each instance handles a subset of partitions
    — state in memory (fast) + checkpointed to Redis/Cassandra (durable)
    |
    v
[Aggregated Output Kafka Topic]
    — downstream consumers read from here
```

---

## What to Cover in Your Answer

**Problem 1:**
- [ ] Dictionary keyed by `document_id` for pending state
- [ ] Set comparison to detect completion
- [ ] Memory release after aggregation

**Problem 2:**
- [ ] Timer fires at deadline, emitting partial aggregation
- [ ] Cancel timer if all enrichments arrive early
- [ ] Thread safety (lock around shared state)
- [ ] Trade-off: thread-per-doc vs. async vs. sweeper thread

**Problem 3:**
- [ ] Partition by `document_id` for locality
- [ ] Trade-off: routing strategy (hash vs. random)
- [ ] Failure handling: in-memory state is volatile
- [ ] Recovery: Kafka offset commit + external state store
- [ ] Exactly-once challenge: on restart, avoid duplicate emissions

---

## Follow-Up Questions

1. "Your aggregator has 10K documents in-flight with open timers. A deploy happens. How do you ensure no documents are lost?"
2. "Enricher X starts taking 10 seconds instead of 1 second. How does this affect your system?"
3. "How do you monitor that aggregations are completing on time? What metric would you track?"
4. "If the same `(document_id, enricher_id)` pair arrives twice (duplicate), how do you handle it?"
