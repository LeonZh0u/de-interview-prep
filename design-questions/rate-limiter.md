# Design: Rate Limiter (Redis + Distributed)

**Difficulty:** Medium
**Time:** ~45 minutes
**Topics:** Redis, sliding window vs. fixed window, distributed consistency, race conditions

---

## Overview

Rate limiting is a foundational component for any service gateway or API platform. This question asks you to design a rate limiter from scratch, starting simple and progressively hardening it. There is no single correct answer — the goal is to reason through tradeoffs clearly.

---

## Problem Statement

You are building a rate limiter for a service gateway. The gateway sits in front of multiple backend services.

Requirements:
- Each user can make requests to multiple different backend resources.
- Quotas are tracked **per (user, resource)** pair independently.
- Each request consumes a variable number of **quota units** (not just 1 per request — e.g., a large query might cost 10 units, a small one costs 1).
- Quota resets on a time window basis (configurable: per-minute, per-hour, per-day).
- The system must handle **thousands of distinct (user, resource) pairs** concurrently.
- Enforce the limit: reject requests that would exceed the quota.

---

## Part 1: Fixed Window Algorithm

### Concept

Divide time into fixed windows (e.g., each minute from :00 to :59). Track how many units have been used within the current window. Reset the counter at the start of each new window.

### Data Model (Redis Hash)

Key: `ratelimit:{user_id}:{resource_id}`

Value (stored as a Redis hash):
```
tokens_used: 47
window_start: 1700000000   (Unix timestamp of the start of the current window)
```

### Algorithm

```
On each request (user_id, resource_id, cost):
  1. GET ratelimit:{user_id}:{resource_id}
  2. current_time = now()
  3. If record does not exist OR (current_time - window_start) >= window_size:
       # New window — reset
       SET ratelimit:{user_id}:{resource_id}
           tokens_used = cost
           window_start = floor(current_time / window_size) * window_size
       ALLOW request
  4. Else:
       If tokens_used + cost > quota_limit:
           DENY request
       Else:
           INCR tokens_used by cost
           ALLOW request
```

### Diagram

```
Time:   0        60       120      180
        |--------|--------|--------|
Window:   W1       W2       W3

W1: user makes requests consuming 30, 20 units → total = 50
    At t=60, window resets. tokens_used = 0 again.

W2: user makes requests consuming 40, 15 units → total = 55
    At t=61, user tries to consume 10 more → 55+10=65 > 60 limit → DENY
```

### TTL for Cleanup

Set a TTL on the Redis key equal to `2 * window_size`. If a user makes no requests for two full windows, the key expires automatically and storage is reclaimed.

```
EXPIRE ratelimit:{user_id}:{resource_id}  120   # for a 60-second window
```

### Fixed Window Problem: Burst at Window Boundary

A user can consume the full quota just before the window resets and again just after. In the worst case, a user consumes `2 * quota_limit` units in a short burst spanning the boundary.

```
t=55: user uses 60 units (hits limit for window W1)
t=60: window resets
t=61: user uses 60 units (hits limit for window W2)

→ 120 units consumed in 6 seconds, even though the per-minute quota is 60.
```

---

## Part 2: Sliding Window Algorithm

### Concept

Instead of resetting at fixed boundaries, track a rolling window of the last N seconds. Any request outside that window no longer counts.

### Data Model (Redis List)

Key: `ratelimit:sliding:{user_id}:{resource_id}`

Value: a list of entries, each being a serialized `(cost, timestamp)` pair.

The list is ordered by timestamp (oldest first).

### Algorithm

```
On each request (user_id, resource_id, cost):
  1. current_time = now()
  2. window_start = current_time - window_size

  # Prune expired entries (older than the window)
  3. Remove all list entries where timestamp < window_start

  # Sum remaining entries to get current usage
  4. total_used = sum of cost for all remaining entries

  5. If total_used + cost > quota_limit:
       DENY request
  6. Else:
       Append (cost, current_time) to the list
       ALLOW request
```

### Redis Implementation with Sorted Set

A Redis sorted set is cleaner than a list for this pattern. Use the timestamp as the score and a unique request ID as the member. Store the cost in the member value.

```
Key:    ratelimit:sliding:{user_id}:{resource_id}

ZADD    key  <timestamp>  "<cost>:<request_uuid>"
ZREMRANGEBYSCORE  key  0  <window_start>   # prune old entries
ZRANGEBYSCORE     key  <window_start>  +inf  # get current window entries
  → sum cost fields → compare to limit
```

You can wrap steps 2–6 in a Lua script to execute them atomically on the Redis server, eliminating race conditions between prune and read.

### Cleanup: Background Job vs. On-Request

**On-request pruning:** Each request prunes expired entries before checking. Simple, no background job needed. Downside: a key with no recent requests still retains old entries until the next request arrives.

**Background job:** A periodic job scans for keys with all-expired entries and deletes them. Needed if you have many users who make occasional requests — otherwise memory grows without bound. Use Redis `SCAN` to iterate keys without blocking.

Recommendation: do both. Prune on each request (fast path), and run a background job every few minutes to clean up fully-stale keys.

---

## Part 3: Race Conditions

### The Problem

Between the time you read `tokens_used` (step 1) and the time you write the updated value (step 4), another request for the same user/resource might have already updated the counter. This is a classic check-then-act race condition.

```
Thread A reads: tokens_used = 55, limit = 60, cost = 4 → 55+4=59 ≤ 60 → will allow
Thread B reads: tokens_used = 55, limit = 60, cost = 4 → 55+4=59 ≤ 60 → will allow

Thread A writes: tokens_used = 59
Thread B writes: tokens_used = 59   ← should be 63, limit exceeded!
```

### Solutions

**Option 1: Lua Script (Atomic on Redis)**

Execute the entire read-check-write sequence as a single Lua script. Redis executes Lua scripts atomically — no other command runs while the script is executing.

```lua
local key = KEYS[1]
local cost = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local window_size = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens_used', 'window_start')
local tokens_used = tonumber(data[1]) or 0
local window_start = tonumber(data[2]) or 0

if (now - window_start) >= window_size then
  tokens_used = 0
  window_start = now - (now % window_size)
end

if tokens_used + cost > limit then
  return 0  -- denied
end

redis.call('HMSET', key, 'tokens_used', tokens_used + cost, 'window_start', window_start)
redis.call('EXPIRE', key, window_size * 2)
return 1  -- allowed
```

**Option 2: Optimistic Locking with WATCH/MULTI/EXEC**

Use Redis `WATCH` to watch the key. If the key changes between `WATCH` and `EXEC`, the transaction is aborted and the client retries.

```
WATCH ratelimit:{user_id}:{resource_id}
GET   ratelimit:{user_id}:{resource_id}
# compute new value locally
MULTI
SET ratelimit:{user_id}:{resource_id} <new_value>
EXEC   # returns nil if WATCH key was modified → retry
```

Lua scripts are simpler and preferred for this use case.

**Option 3: Pre-Deduct with Adjustment**

Immediately deduct the cost (increment by cost) without checking first. If the new total exceeds the limit, deny the request and refund the cost (decrement). This approach has lower latency in the happy path but requires a compensating write on denial.

This can overshoot the limit briefly if many concurrent denials are happening simultaneously. Acceptable for soft limits, not acceptable for hard billing limits.

---

## Part 4: Caching with LRU In-Memory Cache

For very active users, even a Redis round-trip (0.5–2ms) on every request adds up. Add an in-memory LRU cache in front of Redis within each gateway instance.

### Architecture

```
[Incoming Request]
       │
       ▼
[Gateway Instance N]
  LRU Cache (in-process)
  ├── Cache hit: check local counter → fast path (microseconds)
  └── Cache miss or local limit: call Redis → update local cache
       │
       ▼
   [Redis]
```

### Design Considerations

- **Cache entry TTL:** Set to a small fraction of the rate limit window (e.g., if window is 60s, cache for 5s). Stale cache means a user might briefly exceed their quota before Redis is consulted.
- **LRU eviction:** Keep only the top N most recently active (user, resource) pairs in memory. For a gateway serving 10,000 concurrent users, a cache of 50,000 entries is reasonable.
- **Multi-instance consistency:** Each gateway instance has its own local cache. Instance A might see 50 units used while Instance B sees 48 units used for the same user. Redis is the authoritative source. The local cache is an optimization, not the source of truth.
- **When to bypass the cache:** On a cache hit indicating the user is near the limit (e.g., > 80% used), fall through to Redis immediately to get an accurate count. Only use the local cache when the user is well within their quota.

---

## Part 5: Follow-Up Discussion

### Hard vs. Soft vs. Elastic Throttling

| Mode | Behavior | Use Case |
|---|---|---|
| Hard limit | Requests beyond quota are rejected immediately (HTTP 429) | Billing-based quotas, security rate limits |
| Soft limit | Requests beyond quota are queued or delayed, not rejected | Internal batch jobs, background tasks |
| Elastic limit | Quota can temporarily exceed the configured limit; overage is charged or tracked | Paid API tiers with burst allowances |

### Configuration Management

Rate limit configurations (quota per user per resource, window size) should not be hardcoded. Store them in a configuration service (e.g., a database table or a config management system) and propagate updates to gateway instances via a pub/sub channel or periodic polling.

When a quota changes, you may need to handle in-progress windows carefully (e.g., a user who is mid-window when their quota is increased should immediately benefit from the higher limit).

### Testing Distributed Rate Limiters

- **Unit tests:** Test the algorithm logic (fixed window, sliding window) with deterministic timestamps and mock Redis calls.
- **Race condition tests:** Use multiple goroutines/threads hammering the same key simultaneously. Assert that the total accepted cost never exceeds the limit.
- **Chaos tests:** Kill a Redis replica during a load test. Verify the gateway either fails open (allows requests) or fails closed (denies all), per your stated policy.
- **Clock skew tests:** Gateway instances may have slightly different system clocks. Verify that a 1-second clock skew does not cause double-counting or missed counts at window boundaries.
- **Load tests:** Measure P99 latency of rate limit checks under peak load. Ensure the rate limiter itself is not the bottleneck.

---

## Summary of Tradeoffs

| Algorithm | Accuracy | Memory | Complexity | Burst at boundary |
|---|---|---|---|---|
| Fixed window | Medium | O(1) per key | Low | Yes (2x burst possible) |
| Sliding window (sorted set) | High | O(requests in window) per key | Medium | No |
| Token bucket | High | O(1) per key | Medium | Configurable |

For most API gateway use cases, a **sliding window with Redis sorted sets and a Lua script for atomicity** is the right balance of accuracy and operational simplicity.
