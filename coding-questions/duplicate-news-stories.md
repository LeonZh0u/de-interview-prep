# Coding: Duplicate Detection in News Streams

**Difficulty:** Easy-Medium
**Time:** ~45 minutes
**Languages:** Python
**Focus areas:** Hashing, sliding window, deque, stream processing, amortized complexity

---

## Problem Statement

Given a stream of news stories, determine which stories are duplicates within a 60-second window. Two stories are duplicates if they have the same body AND headline. Stories arrive in chronological order.

**Input format:** Each story on a separate line:

```
<id>, <body>, <headline>, <timestamp>
```

where `timestamp` is epoch seconds.

---

## Part 1: Basic Duplicate Detection

**Approach:**
- Use a dict mapping `(body, headline)` → `StoryInfo` for O(1) lookup
- For each story, check if the key exists and the timestamp difference is <= 60s
- Return the list of duplicate stories

```python
from collections import defaultdict


class StoryInfo:
    def __init__(self, story_id, time_of_arrival):
        self.id = story_id
        self.time_of_arrival = time_of_arrival


def parse_story(line):
    # Expected format: <id>, <body>, <headline>, <timestamp>
    parts = [p.strip() for p in line.split(',')]
    story_id, body, headline, timestamp = parts[0], parts[1], parts[2], int(parts[3])
    return body, headline, StoryInfo(story_id, timestamp)


def duplicate_stories(inp, max_time_seconds=60):
    duplicates = []
    previous_stories = dict()
    lines = filter(bool, map(lambda l: l.strip(), inp.split('\n')))

    for line in lines:
        body, headline, story_info = parse_story(line)
        if (body, headline) in previous_stories:
            prev = previous_stories[(body, headline)]
            if abs(prev.time_of_arrival - story_info.time_of_arrival) <= max_time_seconds:
                duplicates.append((prev.id, body, headline, prev.time_of_arrival))
        previous_stories[(body, headline)] = story_info

    return duplicates
```

### Test Cases

```python
def test_no_duplicates():
    inp = """
    1, body_a, headline_a, 1000
    2, body_b, headline_b, 1010
    3, body_c, headline_c, 1020
    """
    assert duplicate_stories(inp) == []


def test_clear_duplicate():
    inp = """
    1, body_a, headline_a, 1000
    2, body_a, headline_a, 1030
    """
    result = duplicate_stories(inp)
    assert len(result) == 1
    assert result[0][0] == '1'  # original story id


def test_boundary_exactly_60s():
    # At exactly 60s, the pair IS a duplicate (<=)
    inp = """
    1, body_a, headline_a, 1000
    2, body_a, headline_a, 1060
    """
    assert len(duplicate_stories(inp)) == 1


def test_outside_window():
    # At 61s, the pair is NOT a duplicate
    inp = """
    1, body_a, headline_a, 1000
    2, body_a, headline_a, 1061
    """
    assert duplicate_stories(inp) == []


def test_same_body_different_headline():
    inp = """
    1, body_a, headline_a, 1000
    2, body_a, headline_b, 1010
    """
    assert duplicate_stories(inp) == []


def test_same_headline_different_body():
    inp = """
    1, body_a, headline_a, 1000
    2, body_b, headline_a, 1010
    """
    assert duplicate_stories(inp) == []
```

**Complexity (Part 1):**
- Time per event: O(1) average (dict lookup + insert)
- Space: O(N) — the dict grows unboundedly with the total number of unique stories

---

## Part 2: Bounded Memory with a Sliding Window (TimedDictionary)

**The problem with Part 1:** The dict holds every story ever seen. For a high-volume stream running for days, this is impractical.

**Goal:** O(k) space, where k = the expected number of unique stories within the 60-second window.

**Approach:**
- Combine a `deque` and a `dict` into a `TimedDictionary`
- On each insertion, evict all entries from the front of the deque whose timestamps fall outside the current window
- Each story is enqueued once and dequeued once, so eviction is **amortized O(1)** per event

```python
from collections import deque


class TimedDictionary:
    """
    A dict-like structure that automatically evicts entries older than
    `window_seconds` relative to the most recently inserted timestamp.

    Internally maintains:
      - self.store: dict mapping key -> value for O(1) lookup
      - self.queue: deque of (timestamp, key) in insertion order for eviction

    Amortized O(1) per insert because each entry is pushed and popped exactly once.
    """

    def __init__(self, window_seconds=60):
        self.window_seconds = window_seconds
        self.store = dict()
        self.queue = deque()  # elements: (timestamp, key)

    def insert(self, key, value, timestamp):
        self._evict(timestamp)
        self.store[key] = value
        self.queue.append((timestamp, key))

    def get(self, key):
        return self.store.get(key)

    def __contains__(self, key):
        return key in self.store

    def _evict(self, current_timestamp):
        cutoff = current_timestamp - self.window_seconds
        while self.queue and self.queue[0][0] < cutoff:
            _, old_key = self.queue.popleft()
            # Only delete if no newer entry for the same key was inserted
            if old_key in self.store:
                del self.store[old_key]


def duplicate_stories_bounded(inp, max_time_seconds=60):
    duplicates = []
    timed_dict = TimedDictionary(window_seconds=max_time_seconds)
    lines = filter(bool, map(lambda l: l.strip(), inp.split('\n')))

    for line in lines:
        body, headline, story_info = parse_story(line)
        key = (body, headline)

        if key in timed_dict:
            prev = timed_dict.get(key)
            # Entries in the dict are already guaranteed to be within the window,
            # so no additional time check is needed here.
            duplicates.append((prev.id, body, headline, prev.time_of_arrival))

        timed_dict.insert(key, story_info, story_info.time_of_arrival)

    return duplicates
```

**Complexity (Part 2):**
- Time per event: O(1) amortized — each story is enqueued once and dequeued once
- Space: O(k) — only stories within the current window are retained

---

## Complexity Summary

| Approach | Time per event | Space |
|---|---|---|
| Basic (unbounded dict) | O(1) avg | O(N) total stories |
| TimedDictionary (bounded) | O(1) amortized | O(k) stories in window |

---

## Follow-Up Questions

### 1. What if stories arrive out of order?

The current solution assumes stories arrive in strictly chronological order, so `current_timestamp >= all previous timestamps`. If late arrivals are possible:

- Maintain a buffer for the maximum expected lateness (e.g., N minutes)
- Use a **min-heap** keyed on timestamp to process events in order once they are "settled"
- Only emit/evict entries once the watermark (minimum observed future timestamp) has advanced past their window

### 2. How would you scale to 2 billion stories per day?

A single machine cannot handle this volume. The key insight is that duplicate detection only requires comparing stories with the **same headline and body** — no cross-key coordination is needed.

- **Partition by `hash(headline)`** using a message queue (e.g., Kafka)
- All stories with the same headline are guaranteed to land on the same consumer/partition
- Each consumer runs an independent `TimedDictionary` instance
- No distributed locking or cross-node coordination required
- Scale horizontally by adding partitions and consumers

At 2B stories/day that is roughly 23,000 stories/second. With, say, 100 partitions, each consumer handles ~230 stories/second — well within the throughput of a single process.

### 3. How would you support fuzzy matching (e.g., 95% headline match, 80% body match)?

Exact key hashing no longer works because two slightly different strings hash to different values.

- **Bag-of-words:** tokenize headline/body, remove stop words, represent as a set of terms
- **Jaccard similarity:** `|A ∩ B| / |A ∪ B|` — declare a duplicate if similarity exceeds the threshold
- **MinHash / LSH (Locality Sensitive Hashing):** approximate nearest-neighbor search that lets you bucket likely-similar documents without comparing all pairs, suitable for high-throughput streams
- Trade-off: fuzzy matching adds CPU cost and introduces false-positive/negative rates that need tuning

### 4. How would you handle this against a database of 1 year of sorted stories?

- The data is already sorted by timestamp, so you can use a **sliding window scan** rather than hashing the entire dataset into memory
- Maintain a window of stories within the 60-second range using two pointers or a deque
- **Parallelize independent windows:** partition the year of data into non-overlapping time chunks (with small overlap at boundaries to catch cross-boundary duplicates), and process each chunk independently across machines
- Each worker emits its local duplicates; a final merge step reconciles boundary cases

---

## Checklist: What to Cover in an Interview

- [ ] Dict keyed on `(body, headline)` for O(1) lookup
- [ ] Time window check before declaring a duplicate
- [ ] Sliding window eviction with a deque for bounded memory
- [ ] Amortized analysis of eviction (each entry pushed and popped exactly once)
- [ ] Scaling strategy via hash-based partitioning (same key → same consumer)
