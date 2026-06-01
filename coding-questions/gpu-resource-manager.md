# Coding: Resource Quota Manager (Event Log + Point-in-Time Queries)

**Difficulty:** Medium-Hard
**Time:** ~45 minutes
**Language:** Python
**Topics:** Event sourcing, immutable logs, point-in-time queries, heap, time-travel

---

## Problem Statement

You are building a credit management system for GPU resources. Users are allocated credits that can be spent on compute jobs. Credits have expiration times — if you do not use them before they expire, they are lost.

Implement a class `ResourceQuotaManager` with the following three operations:

### Operations

**`add(user_id, amount, start_time, duration=None)`**

Allocate `amount` credits to `user_id`. The credits are valid starting at `start_time`. If `duration` is provided (in seconds), the credits expire at `start_time + duration`. If `duration` is `None`, the credits never expire.

**`use(user_id, amount, execution_time)`**

Consume `amount` credits from `user_id`'s balance at `execution_time`. Consume from the earliest-expiring allocation first (to minimize waste). If the user does not have enough credits at `execution_time`, raise an `InsufficientQuotaError`.

Credits from an allocation can only be used if `start_time <= execution_time < expiry_time` (or, for non-expiring allocations, if `start_time <= execution_time`).

**`quota(user_id, timestamp)`**

Return the number of credits available to `user_id` at `timestamp`. This must work for past, present, and future timestamps. It should reflect the true state at that point in time — factoring in allocations that were active and usages that occurred at or before `timestamp`.

---

## Clarification Questions

Before coding, think through these. In an interview, ask the ones that affect your design.

1. **Can events arrive out of order?** For example, can `use()` be called with an `execution_time` before a previously recorded `use()`? → Yes; events may be recorded out of order.
2. **Can `use()` be called at exactly the expiration time?** → No; credits expire at `expiry_time` (exclusive upper bound).
3. **What happens if there are insufficient credits?** → Raise an exception. Do not perform a partial deduction.
4. **Can a single `use()` span multiple allocations?** → Yes; if the user has 30 credits in allocation A and 40 in allocation B, a `use()` of 50 should consume all 30 from A and 20 from B.
5. **Can credits go negative?** → No. Reject the `use()` if credits are insufficient.
6. **Is `duration` always a positive number?** → Yes; assume valid inputs unless stated otherwise.
7. **Do we need to support `remove()` or `cancel()` of an allocation?** → Not in this problem.
8. **What is the precision of timestamps?** → Assume they are comparable Python values (integers, floats, or datetimes — your choice).
9. **Can two allocations have the exact same expiry time?** → Yes; handle this gracefully (break ties by allocation order).
10. **Is the system single-threaded?** → Yes, for this problem. Concurrency is a follow-up.

---

## Edge Cases

Think through these before writing code:

1. A `quota()` query for a timestamp before any allocations → return 0.
2. A `use()` at exactly `start_time` (inclusive boundary) → allowed.
3. A `use()` at exactly `expiry_time` (exclusive boundary) → not allowed (credits have expired).
4. A user has two allocations; `use()` consumes from the wrong one first → should always consume earliest-expiring first.
5. A `use()` partially depletes an allocation → remaining credits in that allocation are still usable.
6. A `quota()` query for a future timestamp where allocations have not yet started → do not count those credits.
7. A `quota()` query for a past timestamp, after a `use()` that occurred later than that timestamp → the later `use()` must not affect the result.
8. Credits expire between a `use()` being recorded and the `quota()` query timestamp → expired credits are not available.
9. Two allocations have the same expiry time → consume from both as if they are a single pool; order within the tie does not matter.
10. A `use()` is recorded out of order (execution_time is earlier than a previously recorded use) → `quota()` results for all timestamps must still be correct.

---

## The Key Design Challenge

### Why Naive Mutation Breaks Point-in-Time Queries

The intuitive approach is to maintain a mutable balance per user:

```python
# Naive approach (WRONG for point-in-time queries)
class Naive:
    def __init__(self):
        self.balance = {}  # user_id -> current credits

    def add(self, user_id, amount, start_time, duration=None):
        self.balance[user_id] = self.balance.get(user_id, 0) + amount

    def use(self, user_id, amount, execution_time):
        self.balance[user_id] -= amount   # mutates current state

    def quota(self, user_id, timestamp):
        return self.balance[user_id]      # always returns CURRENT balance
                                          # cannot answer "what was balance at t=5?"
```

This is wrong because:
- `quota(user_id, past_timestamp)` returns the current balance, not the balance at `past_timestamp`.
- Out-of-order `use()` calls invalidate the computed balance.
- There is no way to replay history to reconstruct past states.

### Event Log (Append-Only) Approach

Instead of mutating state, record every `add` and `use` as an immutable event. To compute `quota(user_id, timestamp)`, replay all events up to `timestamp`.

```
Events for user "alice":
  t=0:  ADD  100 credits, expires t=100
  t=10: ADD  50  credits, expires t=200
  t=20: USE  30  credits
  t=50: USE  60  credits

quota("alice", t=30):
  → apply ADD at t=0: +100 (active, not expired)
  → apply ADD at t=10: +50 (active, not expired)
  → apply USE at t=20: -30 (occurred before t=30)
  → skip USE at t=50: (occurs after t=30)
  → balance = 100 + 50 - 30 = 120
```

This is the same pattern used in **feature stores** for point-in-time correct feature retrieval: never mutate historical data, always replay from the log up to the query timestamp.

---

## Approach Comparison

| Approach | `add` | `use` | `quota` | Space |
|---|---|---|---|---|
| Naive mutation (incorrect) | O(1) | O(n log n) | O(1) wrong | O(u * a) |
| Event log + full replay | O(1) | O(n * m) | O(n * m) | O(u * (a + m)) |
| Min-heap per user | O(log n) | O(k log n) | O(n * m) | O(u * (a + m)) |

Where:
- u = number of users
- a = number of allocations per user
- m = number of `use` events per user
- n = number of active allocations per user
- k = number of allocations consumed by one `use` call

The **min-heap approach** optimizes the `use` operation by keeping allocations sorted by expiry time, so we always consume from the nearest-expiring allocation without scanning all of them. However, `quota` still requires replaying the full log for point-in-time correctness.

---

## Solution

### Data Structures

```python
import heapq
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
import math


class InsufficientQuotaError(Exception):
    pass


@dataclass
class Allocation:
    amount: float
    start_time: float
    expiry_time: float  # math.inf if no expiry

    def is_active_at(self, timestamp: float) -> bool:
        return self.start_time <= timestamp < self.expiry_time

    def __lt__(self, other):
        # For heap ordering: sort by expiry_time ascending
        return self.expiry_time < other.expiry_time


@dataclass
class UsageEvent:
    amount: float
    execution_time: float


class ResourceQuotaManager:
    def __init__(self):
        # user_id -> list of Allocation objects (stored immutably)
        self._allocations: dict[str, List[Allocation]] = {}
        # user_id -> list of UsageEvent objects (stored immutably)
        self._usage_log: dict[str, List[UsageEvent]] = {}
```

### `add` Implementation

```python
    def add(self, user_id: str, amount: float,
            start_time: float, duration: Optional[float] = None) -> None:
        """Record a new credit allocation. O(1) — just append to the log."""
        expiry = start_time + duration if duration is not None else math.inf
        allocation = Allocation(amount=amount, start_time=start_time, expiry_time=expiry)

        if user_id not in self._allocations:
            self._allocations[user_id] = []
        self._allocations[user_id].append(allocation)
```

### `quota` Implementation

```python
    def quota(self, user_id: str, timestamp: float) -> float:
        """
        Compute available credits at the given timestamp by replaying all events.

        This is O(n*m) where n = allocations, m = use events.
        It is correct for any timestamp, including past and future.
        """
        allocations = self._allocations.get(user_id, [])
        usage_events = self._usage_log.get(user_id, [])

        # Find allocations active at this timestamp and sum their remaining amounts.
        # We must account for usages that occurred at or before `timestamp`.

        # Step 1: Collect all usages at or before timestamp, sorted by execution_time.
        relevant_usages = sorted(
            [u for u in usage_events if u.execution_time <= timestamp],
            key=lambda u: u.execution_time
        )

        # Step 2: For each usage, determine how many credits were consumed from each
        # allocation (same earliest-expiry-first logic as in `use`).
        # We need to replay the consumption history to know how much remains in each
        # allocation at `timestamp`.

        # Track remaining amount in each allocation (index-aligned with `allocations`)
        remaining = [a.amount for a in allocations]

        for usage in relevant_usages:
            needed = usage.amount
            # Build a min-heap of (expiry_time, allocation_index) for allocations
            # that were active at usage.execution_time
            active = [
                (allocations[i].expiry_time, i)
                for i in range(len(allocations))
                if allocations[i].is_active_at(usage.execution_time) and remaining[i] > 0
            ]
            heapq.heapify(active)

            while needed > 0 and active:
                expiry, idx = heapq.heappop(active)
                consumed = min(remaining[idx], needed)
                remaining[idx] -= consumed
                needed -= consumed

            # Note: if needed > 0 here, this usage was invalid — but we trust the
            # log was validated at use() time.

        # Step 3: Sum remaining credits in allocations active at `timestamp`.
        total = 0.0
        for i, alloc in enumerate(allocations):
            if alloc.is_active_at(timestamp):
                total += remaining[i]

        return total
```

### `use` Implementation

```python
    def use(self, user_id: str, amount: float, execution_time: float) -> None:
        """
        Consume credits, prioritizing earliest-expiring allocations.

        Raises InsufficientQuotaError if insufficient credits are available.
        This operation is O(k log n) where k is the number of allocations consumed.
        """
        # Validate that sufficient credits are available at execution_time
        available = self.quota(user_id, execution_time)
        if available < amount:
            raise InsufficientQuotaError(
                f"User {user_id} has {available} credits at t={execution_time}, "
                f"but {amount} were requested."
            )

        # Record the usage event (append-only, do not mutate allocations)
        if user_id not in self._usage_log:
            self._usage_log[user_id] = []
        self._usage_log[user_id].append(UsageEvent(amount=amount, execution_time=execution_time))
```

---

## Example Walkthrough

```python
mgr = ResourceQuotaManager()

# Alice gets 100 credits at t=0, expiring at t=100
mgr.add("alice", 100, start_time=0, duration=100)

# Alice gets 50 credits at t=10, expiring at t=200
mgr.add("alice", 50, start_time=10, duration=190)

# How many credits does Alice have at t=5?
# Allocation 1: active (0 <= 5 < 100), amount=100
# Allocation 2: not active yet (10 > 5)
# Total: 100
assert mgr.quota("alice", 5) == 100

# How many credits at t=15?
# Allocation 1: active, amount=100
# Allocation 2: active, amount=50
# Total: 150
assert mgr.quota("alice", 15) == 150

# Alice uses 80 credits at t=20
# Earliest expiring is Allocation 1 (expires t=100).
# Consume 80 from Allocation 1. Remaining in Alloc 1: 20.
mgr.use("alice", 80, execution_time=20)

# How many credits at t=25?
# Allocation 1: 100 - 80 = 20 remaining, still active
# Allocation 2: 50 remaining, still active
# Total: 70
assert mgr.quota("alice", 25) == 70

# How many credits at t=5? (before the use() event)
# use() happened at t=20, which is after t=5.
# Replay: no usages at or before t=5.
# Allocation 1: active at t=5, full amount=100
# Total: 100
assert mgr.quota("alice", 5) == 100   # time-travel query: still correct!

# How many credits at t=105? (Allocation 1 has expired)
# Allocation 1: expired (105 >= 100), not counted
# Allocation 2: active (10 <= 105 < 200), amount=50 - 0 usages = 50
# But wait: did the use() at t=20 consume from Allocation 1 or 2?
# At t=20: Alloc 1 expires at t=100, Alloc 2 expires at t=200.
# Earliest expiry = Alloc 1 → consumed 80 from Alloc 1. Alloc 2 untouched.
# At t=105: Alloc 1 expired, Alloc 2 has 50 remaining.
assert mgr.quota("alice", 105) == 50

# Alice tries to use 60 credits at t=110 — only 50 available
try:
    mgr.use("alice", 60, execution_time=110)
    assert False, "Should have raised"
except InsufficientQuotaError:
    pass  # correct

# Alice uses 30 credits at t=110 — OK
mgr.use("alice", 30, execution_time=110)
assert mgr.quota("alice", 110) == 20

print("All assertions passed.")
```

---

## Connection to Feature Stores and Point-in-Time Correctness

This problem is a direct analogy to **point-in-time correct feature retrieval** in ML feature stores.

In feature stores:
- You have a stream of feature updates (e.g., "user's transaction count is now 5 as of t=1000").
- When training a model, you need to look up what the feature value *was* at the time of each training label — not the current value.
- Using the current value introduces **data leakage**: the model learns from information that would not have been available at prediction time.

The solution in both cases is the same: **never mutate historical records, append events to an immutable log, and replay from the log to reconstruct any past state**.

Feature stores like Feast, Tecton, and Hopsworks implement this with a "point-in-time join" operation that, for each training example `(entity_id, label_timestamp)`, looks up the feature value at `label_timestamp` — exactly the same logic as `quota(user_id, timestamp)`.

---

## Follow-Up Questions

### Scaling

- **How would you handle 1 million users with thousands of allocation events each?**
  - Replaying the full log for every `quota()` query is O(n*m). For large logs, maintain checkpoints: periodically snapshot the per-allocation remaining amounts at specific timestamps. For a `quota()` query, load the nearest checkpoint before `timestamp` and replay only events since the checkpoint.
  - This is analogous to write-ahead logs with checkpointing in databases.

- **How would you make this thread-safe?**
  - Use a read-write lock: multiple `quota()` queries can run concurrently (reads), but `add()` and `use()` acquire an exclusive write lock. Or use an actor model (one thread per user) to serialize operations per user without global locking.

- **How would you persist this to a database?**
  - Store `allocations` and `usage_log` as append-only tables. Never update or delete rows. Use `user_id + event_timestamp + event_id` as the composite key.
  - For efficient point-in-time queries, index by `(user_id, timestamp)`. Consider time-series databases (TimescaleDB, InfluxDB) for very high event rates.

### Design Variations

- **What if you want to support `cancel(allocation_id)`?**
  - Append a `CANCEL` event to the log rather than deleting the allocation row. During replay, treat cancelled allocations as if they had zero remaining credits.
  - This preserves the immutability of the log while supporting cancellation.

- **What if the same user has thousands of active allocations?**
  - The O(k log n) complexity of `use()` becomes important. With a min-heap, you only process allocations in expiry order, so `k` (number of allocations consumed) is typically much smaller than `n` (total allocations).

- **What if you need to support partial `use()` — deduct as much as available, return the remainder?**
  - Change the interface: `use()` returns the number of credits actually consumed. Remove the `InsufficientQuotaError`. This is a simpler variant useful for soft quota enforcement.
