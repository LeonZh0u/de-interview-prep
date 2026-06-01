# Coding: Event Deduplication (Streaming)

**Difficulty:** Medium (3-part progressive)
**Time:** ~45 minutes
**Languages:** Python
**Focus areas:** Stream processing, strategy pattern, hashing, sliding window, deque, extensibility

---

## Problem Statement

Financial events (trade alerts, earnings announcements, price movements) arrive from many upstream data sources. The same real-world event often arrives multiple times with slight variations — different timestamps by a few seconds, prices rounded differently, or fields in different order. Build a system that ingests a stream of events and filters out duplicates, returning only the first occurrence of each distinct real-world event.

An event is a dictionary of key-value pairs, for example:

```python
{"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1000}
{"ticker": "AAPL", "type": "TRADE", "price": 150.01, "timestamp": 1003}  # near-duplicate
{"ticker": "MSFT", "type": "EARNINGS", "price": 420.00, "timestamp": 2000}
```

---

## Part 1: Exact Deduplication (~10 min)

### Task

Given a set of key fields (e.g., `"ticker"` and `"type"`), filter out any events whose values on those fields have been seen before. Only the first occurrence of each unique key combination should pass through.

### Example

```python
events = [
    {"ticker": "AAPL", "type": "TRADE",    "price": 150.00, "timestamp": 1000},
    {"ticker": "AAPL", "type": "TRADE",    "price": 150.01, "timestamp": 1003},  # duplicate on key fields
    {"ticker": "MSFT", "type": "EARNINGS", "price": 420.00, "timestamp": 2000},
    {"ticker": "AAPL", "type": "EARNINGS", "price": 149.50, "timestamp": 3000},
]

deduplicator = ExactEventDeduplicator(key_fields=["ticker", "type"])
for event in events:
    result = deduplicator.process(event)
    if result:
        print(result)

# Expected output:
# {"ticker": "AAPL", "type": "TRADE",    "price": 150.00, "timestamp": 1000}
# {"ticker": "MSFT", "type": "EARNINGS", "price": 420.00, "timestamp": 2000}
# {"ticker": "AAPL", "type": "EARNINGS", "price": 149.50, "timestamp": 3000}
```

### Solution

```python
class ExactEventDeduplicator:
    def __init__(self, key_fields: list[str]):
        self.key_fields = key_fields
        self.seen: set[tuple] = set()

    def _make_key(self, event: dict) -> tuple:
        return tuple(event.get(f) for f in self.key_fields)

    def process(self, event: dict) -> dict | None:
        key = self._make_key(event)
        if key in self.seen:
            return None
        self.seen.add(key)
        return event
```

### Complexity

| Operation | Time | Space |
|-----------|------|-------|
| `process()` | O(1) amortized | O(n) total for n unique events |
| `_make_key()` | O(k) where k = number of key fields | — |

### Key Points to Mention

- Using a `set` of tuples gives O(1) average-case lookup and insertion.
- Tuple key order must be deterministic — always derive from the same ordered list of fields.
- This approach uses unbounded memory; all unique keys are retained forever. That limitation motivates Part 3.

---

## Part 2: Fuzzy Matching Rules (~20 min)

### Task

Exact field matching is too strict for real financial data. A trade at price 150.00 arriving from one feed and 150.01 from another is the same event. A timestamp of 1000 and 1003 likely refers to the same event. You need configurable, per-field matching rules.

Design a system where each field can have its own matching rule:

| Field | Rule |
|-------|------|
| `ticker` | Exact match |
| `price` | Within 1% relative tolerance |
| `timestamp` | Within 5 seconds |

Two events are considered duplicates if all of their rules match simultaneously.

### Design: Strategy Pattern

Define a `MatchRule` base class. Each concrete rule encapsulates matching logic for one field.

```python
from abc import ABC, abstractmethod
from collections import deque


class MatchRule(ABC):
    def __init__(self, field: str):
        self.field = field

    @abstractmethod
    def matches(self, event_a: dict, event_b: dict) -> bool: ...

    @property
    def is_exact(self) -> bool:
        return False


class ExactMatch(MatchRule):
    def matches(self, event_a: dict, event_b: dict) -> bool:
        return event_a.get(self.field) == event_b.get(self.field)

    @property
    def is_exact(self) -> bool:
        return True


class NumericTolerance(MatchRule):
    def __init__(self, field: str, tolerance: float):
        super().__init__(field)
        self.tolerance = tolerance  # relative tolerance, e.g. 0.01 for 1%

    def matches(self, event_a: dict, event_b: dict) -> bool:
        a, b = event_a.get(self.field), event_b.get(self.field)
        if a is None or b is None:
            return a is b
        if a == 0 or b == 0:
            return abs(a - b) <= self.tolerance
        return abs(a - b) / max(abs(a), abs(b)) <= self.tolerance


class TimestampWindow(MatchRule):
    def __init__(self, field: str, window: float):
        super().__init__(field)
        self.window = window  # e.g. 5 seconds

    def matches(self, event_a: dict, event_b: dict) -> bool:
        a, b = event_a.get(self.field), event_b.get(self.field)
        if a is None or b is None:
            return a is b
        return abs(a - b) <= self.window
```

### Key Design Insight: Bucketing

A naive approach checks every incoming event against every stored event: O(n) per event. With thousands of stored events this becomes expensive.

**Optimization:** partition rules into exact vs. fuzzy. Use exact-match fields to compute a bucket key. Events with the same exact-match fields land in the same bucket. Only scan within the bucket for fuzzy matches.

- Without bucketing: O(n) per event
- With bucketing: O(bucket_size) per event, which is much smaller when exact fields (like `ticker`) have high cardinality

### Full Solution

```python
class FuzzyEventDeduplicator:
    def __init__(self, rules: list[MatchRule]):
        self.rules = rules
        self.exact_rules = [r for r in rules if r.is_exact]
        self.fuzzy_rules = [r for r in rules if not r.is_exact]
        # bucket_key -> list of stored events
        self.buckets: dict[tuple, list[dict]] = {}

    def _bucket_key(self, event: dict) -> tuple:
        return tuple(event.get(r.field) for r in self.exact_rules)

    def _is_duplicate(self, event: dict, candidates: list[dict]) -> bool:
        for candidate in candidates:
            if all(r.matches(event, candidate) for r in self.fuzzy_rules):
                return True
        return False

    def process(self, event: dict) -> dict | None:
        key = self._bucket_key(event)
        candidates = self.buckets.get(key, [])

        if self._is_duplicate(event, candidates):
            return None

        if key not in self.buckets:
            self.buckets[key] = []
        self.buckets[key].append(event)
        return event
```

### Example Usage

```python
rules = [
    ExactMatch("ticker"),
    ExactMatch("type"),
    NumericTolerance("price", tolerance=0.01),
    TimestampWindow("timestamp", window=5),
]

deduplicator = FuzzyEventDeduplicator(rules)

events = [
    {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1000},
    {"ticker": "AAPL", "type": "TRADE", "price": 150.01, "timestamp": 1003},  # fuzzy duplicate
    {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1010},  # not duplicate (>5s later)
    {"ticker": "MSFT", "type": "TRADE", "price": 420.00, "timestamp": 1000},  # different ticker
]

for event in events:
    result = deduplicator.process(event)
    if result:
        print(result)

# Expected output:
# {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1000}
# {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1010}
# {"ticker": "MSFT", "type": "TRADE", "price": 420.00, "timestamp": 1000}
```

### Complexity

| Operation | Time | Space |
|-----------|------|-------|
| Bucketing | O(k) for k exact rules | — |
| Duplicate check | O(bucket_size * f) for f fuzzy rules | — |
| Overall | O(bucket_size) per event | O(n) total stored events |

---

## Part 3: Sliding Time Window (~15 min)

### Task

The deduplicator in Part 2 stores every event forever. For a long-running streaming pipeline this is unsustainable. Events older than some time window should be evicted — they are old enough that any re-delivery of them should either be discarded upstream or treated as a new event.

**Key insight:** the `TimestampWindow` rule already defines the relevant window. If two events are more than `window` seconds apart, `TimestampWindow.matches()` will return `False` regardless — so there is no deduplication benefit in retaining events older than that window. No separate parameter is needed.

### Data Structures

- **`deque`** (double-ended queue): maintain events in arrival order. Evict from the left when the front event falls outside the window of the current event's timestamp.
- **bucket dict**: same as Part 2, but entries are removed as events age out. Must keep bucket and deque in sync.

### Eviction Logic

On each new event:
1. Determine the cutoff: `current_timestamp - window`.
2. Pop from the left of the deque while the front event's timestamp is before the cutoff.
3. For each evicted event, remove it from its bucket.
4. Then check the (now-trimmed) bucket for fuzzy duplicates.
5. If not a duplicate, append to deque and bucket.

### Full Solution

```python
class FuzzyEventDeduplicator:
    def __init__(self, rules: list[MatchRule]):
        self.rules = rules
        self.exact_rules = [r for r in rules if r.is_exact]
        self.fuzzy_rules = [r for r in rules if not r.is_exact]

        # Derive the eviction window from the TimestampWindow rule, if present.
        # If no TimestampWindow rule exists, window is None and no eviction occurs.
        timestamp_rules = [r for r in rules if isinstance(r, TimestampWindow)]
        self.window = timestamp_rules[0].window if timestamp_rules else None
        self.timestamp_field = timestamp_rules[0].field if timestamp_rules else None

        self.buckets: dict[tuple, list[dict]] = {}
        self.queue: deque[dict] = deque()

    def _bucket_key(self, event: dict) -> tuple:
        return tuple(event.get(r.field) for r in self.exact_rules)

    def _evict_expired(self, current_ts: float) -> None:
        if self.window is None:
            return
        cutoff = current_ts - self.window
        while self.queue and self.queue[0].get(self.timestamp_field, 0) < cutoff:
            old_event = self.queue.popleft()
            key = self._bucket_key(old_event)
            bucket = self.buckets.get(key, [])
            if old_event in bucket:
                bucket.remove(old_event)
            if not bucket:
                self.buckets.pop(key, None)

    def _is_duplicate(self, event: dict, candidates: list[dict]) -> bool:
        for candidate in candidates:
            if all(r.matches(event, candidate) for r in self.fuzzy_rules):
                return True
        return False

    def process(self, event: dict) -> dict | None:
        if self.timestamp_field:
            current_ts = event.get(self.timestamp_field, 0)
            self._evict_expired(current_ts)

        key = self._bucket_key(event)
        candidates = self.buckets.get(key, [])

        if self._is_duplicate(event, candidates):
            return None

        if key not in self.buckets:
            self.buckets[key] = []
        self.buckets[key].append(event)
        self.queue.append(event)
        return event
```

### Example Usage

```python
rules = [
    ExactMatch("ticker"),
    ExactMatch("type"),
    NumericTolerance("price", tolerance=0.01),
    TimestampWindow("timestamp", window=5),
]

deduplicator = FuzzyEventDeduplicator(rules)

events = [
    {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1000},
    {"ticker": "AAPL", "type": "TRADE", "price": 150.01, "timestamp": 1003},  # fuzzy dup, within window
    {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1010},  # NOT dup: >5s from t=1000, t=1003 evicted
    {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1012},  # fuzzy dup of t=1010
]

for event in events:
    result = deduplicator.process(event)
    status = "PASS" if result else "DROP"
    print(f"[{status}] {event}")

# Expected output:
# [PASS] {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1000}
# [DROP] {"ticker": "AAPL", "type": "TRADE", "price": 150.01, "timestamp": 1003}
# [PASS] {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1010}
# [DROP] {"ticker": "AAPL", "type": "TRADE", "price": 150.00, "timestamp": 1012}
```

### Complexity

| Operation | Time | Space |
|-----------|------|-------|
| Eviction (amortized) | O(1) per event (each event enqueued and dequeued exactly once) | — |
| Duplicate check | O(bucket_size * f) | — |
| Total memory | O(W) where W = max events in the sliding window | Bounded |

The deque ensures total eviction work across all events is O(n), making amortized cost per event O(1).

---

## What to Cover in Your Answer

**Part 1**
- Use a `set` of tuples for O(1) lookup
- Key must be derived deterministically from a fixed ordered list of fields
- Unbounded memory is the main limitation

**Part 2**
- Strategy pattern: `MatchRule` base class with one concrete class per matching strategy
- `is_exact` property lets the deduplicator self-organize rules into buckets vs. fuzzy scan
- Bucketing reduces the search space from all stored events to a small bucket per exact-key group
- `NumericTolerance` handles the zero-denominator edge case explicitly

**Part 3**
- `deque` for O(1) append/popleft with time-ordered eviction
- Bucket dict must stay in sync with the deque — eviction removes from both
- The window is derived from `TimestampWindow` rule, avoiding a separate constructor parameter
- Amortized O(1) eviction because each event is inserted and removed from the deque exactly once

---

## Follow-Up Questions

**1. "What if there are no exact-match rules?"**

There is no bucket key — all events fall into a single bucket. Duplicate checking degrades to O(n) per event, scanning all events in the window. For high-throughput scenarios this is unacceptable. You would need a different indexing strategy (e.g., spatial index for numeric fields, or a locality-sensitive hash).

**2. "What other rule types might be useful?"**

- `StringSimilarity`: fuzzy string match using edit distance or Jaccard similarity (useful for company names or event descriptions that may be abbreviated differently)
- `CaseInsensitiveExact`: exact match after lowercasing (acts as exact for bucketing purposes)
- `SetMembership`: one value must be in a predefined set of equivalents
- `RegexNormalize`: normalize a string field with a regex before exact comparison

**3. "What if events arrive out of order?"**

The current `deque`-based eviction assumes events arrive in non-decreasing timestamp order. Out-of-order events break this assumption in two ways:

- An early event might evict a still-relevant later event
- A late-arriving event might compare against an already-evicted event, missing a duplicate

**Options:**
- Use a min-heap keyed on timestamp for ordered eviction, and accept a bounded lateness tolerance
- Drop events whose timestamp is older than `current_max_timestamp - window` (late-event drop policy)
- Buffer events for a fixed grace period before processing, absorbing mild out-of-order arrival

**4. "How would you scale this to handle millions of events per second?"**

A single-process deduplicator becomes a bottleneck. Key strategy: **shard by exact-match key**.

- Use a message queue (e.g., Kafka) where partition assignment is determined by the exact-match bucket key (e.g., hash of `ticker + type`)
- Each consumer instance handles one or more partitions, maintaining its own `FuzzyEventDeduplicator`
- Because all events with the same exact-match key land on the same partition, no cross-shard coordination is needed for deduplication
- This scales horizontally: add partitions and consumers as throughput grows

**5. "What if the window is very large (hours)?"**

Memory grows linearly with the number of unique events in the window. For a multi-hour window on a high-volume ticker, this can be gigabytes.

**Options:**
- Cap memory with an LRU eviction policy (risk of missing duplicates for popular keys)
- Use an external state store (e.g., Redis with TTL-based expiry) rather than in-process dicts
- Store a compact fingerprint (e.g., a hash of the fuzzy-normalized event) instead of the full event dict, trading exact recall for a probabilistic guarantee (similar to a Bloom filter approach)
- Accept probabilistic deduplication: a counting Bloom filter with TTL offers sub-linear space with a tunable false-positive rate
