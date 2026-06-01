# Coding: Story Index (Inverted Index + Time-Range Query)

**Difficulty:** Easy–Medium
**Time:** ~45 minutes
**Languages:** Python (C++ also used in some versions)
**Focus areas:** Inverted index, binary search, sorted data structures, complexity analysis, distributed scale

---

## Problem Statement

A news platform receives articles (stories) in a streaming fashion. Each story has:
- A unique identifier (`id`)
- A list of tags (e.g., company tickers like `"MSFT"`, topics like `"TECHNOLOGY"`)
- A publication timestamp

Design and implement a module that allows:
1. **Indexing** new stories as they arrive.
2. **Querying** stories by tag and time range — efficiently.

### Example Story
```json
{
    "id": "BBTYYIAT142",
    "ts": 1614438904,
    "tags": ["MSFT", "TECHNOLOGY", "CLOUD"]
}
```

### Interface
```python
class StoryIndex:
    def add_story(self, story: dict) -> None:
        """Add a story to the index."""

    def match(self, tag: str, start_ts: int, end_ts: int) -> set[str]:
        """Return story IDs where the given tag appears in [start_ts, end_ts]."""
```

### Scale Notes
- **Many more queries than insertions.**
- Stories arrive in approximately increasing timestamp order.
- Up to 100K distinct tags; up to 10–20 tags per story.
- Up to several million stories in a single day.

---

## Step 1: Interface Design (5 min)

Think about:
- Should `match` return a `set` (unique IDs) or a list? → `set` (story IDs are unique).
- Is `start_ts` inclusive? `end_ts` inclusive? → Define your bounds clearly.
- Can timestamps be duplicate across stories? → Assume unique for simplicity.

---

## Step 2: Data Structure Choice (5–10 min)

**Naive approach:** Linear scan through all stories for each query.
- `match` is O(N × avg_tags) — N = total stories.
- Unacceptable since queries >> insertions.

**Better approach:** Two-index structure:
1. **Inverted index (hash map):** `tag → sorted list of (timestamp, story_id)`
   - O(1) lookup by tag.
   - Pre-sorted by timestamp enables binary search for range queries.
2. **Sorted structure per tag:** Use `SortedList` (Python `sortedcontainers`) or `bisect`.

**Complexity:**
- `add_story`: O(T × log N) where T = tags per story, N = stories with that tag.
- `match`: O(log N + K) where K = number of matching stories.

This is optimal — we can't do better than returning K results without reading them.

---

## Step 3: Implementation (15–20 min)

```python
from sortedcontainers import SortedList
from collections import defaultdict

class StoryIndex:
    def __init__(self):
        # Maps tag → SortedList of (timestamp, story_id)
        # SortedList maintains sorted order by first element (timestamp)
        self.index: dict[str, SortedList] = defaultdict(SortedList)

    def add_story(self, story: dict) -> None:
        """O(T * log N) — T tags per story, N = stories per tag"""
        ts = story["ts"]
        sid = story["id"]
        for tag in story["tags"]:
            self.index[tag].add((ts, sid))

    def match(self, tag: str, start_ts: int, end_ts: int) -> set[str]:
        """O(log N + K) — binary search + linear scan of K results"""
        if tag not in self.index:
            return set()

        tag_list = self.index[tag]

        # Binary search for the left boundary (first ts >= start_ts)
        left = tag_list.bisect_left((start_ts,))
        # Binary search for the right boundary (first ts > end_ts)
        right = tag_list.bisect_right((end_ts, chr(0x10FFFF)))  # max unicode after any sid

        # Collect story IDs in range
        return {tag_list[i][1] for i in range(left, right)}
```

### Without `sortedcontainers` (using `bisect` + list)

```python
import bisect
from collections import defaultdict

class StoryIndex:
    def __init__(self):
        # tag → list of timestamps (sorted)
        self.ts_index: dict[str, list] = defaultdict(list)
        # tag → list of story_ids (parallel array, same order as timestamps)
        self.id_index: dict[str, list] = defaultdict(list)

    def add_story(self, story: dict) -> None:
        ts, sid = story["ts"], story["id"]
        for tag in story["tags"]:
            pos = bisect.bisect_left(self.ts_index[tag], ts)
            self.ts_index[tag].insert(pos, ts)
            self.id_index[tag].insert(pos, sid)

    def match(self, tag: str, start_ts: int, end_ts: int) -> set[str]:
        if tag not in self.ts_index:
            return set()
        ts_list = self.ts_index[tag]
        id_list = self.id_index[tag]
        left = bisect.bisect_left(ts_list, start_ts)
        right = bisect.bisect_right(ts_list, end_ts)
        return set(id_list[left:right])
```

---

## Step 4: Test Cases (5 min)

```python
idx = StoryIndex()

s1 = {"id": "id1", "ts": 100, "tags": ["MSFT", "CLOUD"]}
s2 = {"id": "id2", "ts": 200, "tags": ["MSFT", "TECH"]}
s3 = {"id": "id3", "ts": 300, "tags": ["MSFT", "GOOG"]}
s4 = {"id": "id4", "ts": 400, "tags": ["GOOG", "AMZN"]}

for s in [s1, s2, s3, s4]:
    idx.add_story(s)

assert idx.match("MSFT", 150, 350) == {"id2", "id3"}
assert idx.match("MSFT", 100, 400) == {"id1", "id2", "id3"}
assert idx.match("GOOG", 300, 500) == {"id3", "id4"}
assert idx.match("UNKNOWN", 0, 1000) == set()
assert idx.match("MSFT", 500, 1000) == set()  # out of range

print("All tests passed!")
```

---

## Step 5: Follow-Up Questions (10–15 min)

### Q: What if timestamps are not in insertion order?
- The `SortedList` / `bisect.insort` approach still works — insertion maintains sorted order regardless.
- If you used a "hint" (amortized O(1) insertion assuming sorted input), out-of-order insertions would break it.

### Q: What if two stories can have the same timestamp?
- Use a `SortedList` of `(ts, sid)` tuples — the `sid` acts as a tiebreaker, ensuring uniqueness.
- Alternatively, use `std::multimap` in C++.

### Q: Multi-tag queries — disjunction (ANY tag)

```python
def match_any(self, tags: list[str], start_ts: int, end_ts: int) -> set[str]:
    """Return stories matching ANY of the given tags in the time range."""
    result = set()
    for tag in tags:
        result |= self.match(tag, start_ts, end_ts)
    return result
```

### Q: Multi-tag queries — conjunction (ALL tags)

```python
def match_all(self, tags: list[str], start_ts: int, end_ts: int) -> set[str]:
    """Return stories matching ALL of the given tags in the time range."""
    if not tags:
        return set()
    result = self.match(tags[0], start_ts, end_ts)
    for tag in tags[1:]:
        result &= self.match(tag, start_ts, end_ts)
    return result
```

Optimization: Start with the tag that has the fewest stories (smallest set) to minimize intersection work.

### Q: How do you scale this to a distributed system?

The single-machine index doesn't fit if there are hundreds of millions of stories or trillions of tag-story associations.

**Distributed approach:**
1. **Shard by tag:** Each node is responsible for a subset of tags. A consistent hash ring assigns each tag to a node.
2. **Query routing:** The query router sends `match(tag, start, end)` to the responsible node.
3. **Multi-tag queries:** Fan out to multiple nodes, gather results, merge in the query layer.
4. **Replication:** Each shard is replicated 2–3x for fault tolerance.
5. **Eventual consistency:** New stories are indexed asynchronously after publication.

**Existing systems that implement this:**
- **Elasticsearch / OpenSearch:** Distributed inverted index, sharded by document.
- **Solr:** Similar to Elasticsearch.
- **Apache Lucene:** The underlying library powering both.

---

## Complexity Summary

| Operation | Time | Space |
|---|---|---|
| `add_story` | O(T × log N) | O(N × T) total |
| `match` (single tag) | O(log N + K) | O(K) for results |
| `match_any` (M tags) | O(M × (log N + K)) | O(K total) |
| `match_all` (M tags) | O(M × (log N + K_min)) | O(K_min) |

Where: T = tags per story, N = stories per tag, K = matching stories, M = number of query tags.

---

## What to Cover in Your Answer

- [ ] Reject brute-force O(N) scan — too slow when queries >> insertions
- [ ] Two-index design: hash map for O(1) tag lookup + sorted list for O(log N) range search
- [ ] Binary search for left and right boundaries of the time range
- [ ] Memory cleanup: in a streaming system, old stories may need eviction
- [ ] Multi-tag extensions: union (OR) and intersection (AND)
- [ ] Distributed scaling: shard by tag, consistent hashing, fan-out for multi-tag queries
