# Data Sharding (Data Partitioning)

Sharding is the process of splitting data across multiple nodes/machines to improve scalability, performance, and availability. It is a foundational concept for any large-scale data engineering role.

---

## Why Shard?

A single database server has hard limits: CPU, RAM, disk I/O, and network bandwidth. Once you hit those limits, you must either scale **vertically** (bigger machine — limited, expensive) or scale **horizontally** (more machines — sharding).

**Rule of thumb:** If your dataset is too large for a single node, or your write throughput exceeds a single node's capacity, you need sharding.

---

## Sharding Strategies

### 1. Horizontal Partitioning (Range-Based Sharding)

Rows are distributed across shards based on a range of a key value.

```sql
-- Shard 1: store_id < 1000
-- Shard 2: 1000 <= store_id < 2000
-- Shard 3: store_id >= 2000
PARTITION BY RANGE (store_id) (
    PARTITION p0 VALUES LESS THAN (1000),
    PARTITION p1 VALUES LESS THAN (2000),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
```

**Pros:** Simple; great for range queries (e.g., "get all events between date A and date B").

**Cons:** Hotspots — if most traffic hits the latest time range, one shard gets overwhelmed (e.g., time-series data where the newest shard is always hit).

---

### 2. Hash-Based Partitioning

Apply a hash function to a key to determine the shard.

```python
shard_id = hash(user_id) % num_shards
```

**Pros:** Distributes data evenly; avoids hotspots for writes/reads.

**Cons:**
- Range queries are inefficient (data is spread randomly).
- Adding/removing shards requires rehashing and moving data.
- **Fix:** Use **consistent hashing** (see below).

---

### 3. Vertical Partitioning

Split by feature/column — different groups of columns live on different nodes.

**Example:** User profile table → metadata on one DB, photos on another, activity logs on a third.

**Pros:** Simple; low app-level impact initially.

**Cons:** Further growth may require horizontal sharding on top of vertical partitioning. Cross-column queries require joins across nodes.

---

### 4. Directory-Based Partitioning

A lookup service (directory) maps each key → shard location. The application queries the directory before querying data.

**Pros:** Flexible; can change sharding scheme without touching app logic.

**Cons:** Directory becomes a single point of failure and a bottleneck. Must be highly available and cached.

---

### 5. List Partitioning

Assign specific discrete values to specific partitions.

```sql
PARTITION BY LIST(region) (
    PARTITION p_us VALUES IN ('US-EAST', 'US-WEST'),
    PARTITION p_eu VALUES IN ('EU-WEST', 'EU-CENTRAL'),
    PARTITION p_asia VALUES IN ('APAC', 'JP')
);
```

**Use case:** Geographic sharding, tenant-based multi-tenancy.

---

### 6. Composite (Compound) Partitioning

Combine strategies: e.g., first partition by range (year), then by hash within each year.

Consistent hashing is a specific composite of hash + list partitioning:
```
Key → hash → reduced key space → list → shard
```

---

## Consistent Hashing

Standard hash-based sharding breaks when you add/remove nodes — you have to rehash nearly all keys.

**Consistent hashing** fixes this: nodes and keys are placed on an abstract ring. A key is assigned to the first node clockwise from its position on the ring.

**Benefits:**
- Adding a node only moves keys from one neighbor — O(K/N) keys move, not O(K).
- Removing a node only reassigns its keys to the next node.
- Used by: Amazon DynamoDB, Apache Cassandra, Redis Cluster.

**Virtual nodes (vnodes):** Each physical node is assigned multiple positions on the ring. This improves load balance when nodes have different capacities.

---

## Common Sharding Problems

### Hotspots / Data Skew

**Symptom:** One shard handles disproportionate traffic while others are idle.

**Causes:**
- Poor shard key choice (e.g., sharding on a column with low cardinality — only a few distinct values).
- Temporal skew — range sharding on timestamps means "current" shard always gets hit.

**Solutions:**
- Add a random prefix/suffix to the shard key to spread writes.
- Use a higher-cardinality composite key.
- Pre-split hot ranges.

**Interview angle:** "Cardinality matters in distributed processing. Too-low cardinality → bucket overflow (hotspot); too-high cardinality → too many tiny buckets (overhead). Find the Goldilocks zone."

---

### Cross-Shard Joins

Joins across shards require fetching data from multiple nodes over the network — expensive.

**Solutions:**
- **Denormalize:** Store redundant data so queries hit a single shard.
- **Broadcast small tables:** In Spark, a small dimension table can be broadcast to all workers (broadcast join), avoiding shuffle.
- **Colocate related data:** Ensure rows that are frequently joined share the same shard key.

---

### Referential Integrity

Foreign key constraints cannot be enforced across shards by the database engine.

**Solution:** Enforce integrity at the application layer. Run periodic jobs to detect dangling references.

---

### Rebalancing

When shard load becomes uneven (e.g., one shard grows much larger), you must rebalance — this involves moving data, which causes downtime or requires careful online migration strategies.

**Consistent hashing minimizes rebalancing cost** — only adjacent keys on the ring need to move.

---

## Sharding in Practice: Kafka

Kafka topics are sharded into **partitions**:
- The **partition key** determines which partition a message goes to (`hash(key) % num_partitions`).
- All messages with the same key go to the same partition → guaranteed ordering per key.
- Each partition is consumed by exactly one consumer in a consumer group.

**Interview angle:** "If you need to aggregate all enrichments for a given `document_id`, you should partition by `document_id`. This ensures all enrichments for a doc land on the same consumer — no cross-node coordination needed."

---

## Sharding in Practice: Spark

Spark's equivalent of sharding is **partitioning** of RDDs/DataFrames:
- `repartition(n)` — reshuffles data into `n` partitions (full shuffle).
- `partitionBy(key)` — hash-partitions by a key (useful before groupBy/join to avoid re-shuffle).
- A `join` between two datasets with the same partitioning key avoids a shuffle entirely.

---

## Key Interview Questions to Practice

1. **Shard key selection:** "You're storing stock quote events (timestamp, ticker, user_id). What's your shard key if you need to produce a daily top-100 ticker report?"
2. **Hotspot handling:** "Your time-series data has all writes going to the latest shard. How do you fix this?"
3. **Consistent hashing:** "You're adding 3 new nodes to a 10-node Cassandra cluster. With consistent hashing, roughly how much data needs to move?"
4. **Joins:** "Your left table has 50B rows sharded by user_id. Your right table is a 10K-row dimension table. How do you join efficiently in Spark?"
5. **Cardinality:** "Why is cardinality an important consideration when choosing a Kafka partition key or a MapReduce key?"
