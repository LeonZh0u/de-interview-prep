# Distributed Systems Fundamentals

Core concepts every data engineer must know cold. These appear in both system design discussions and as warm-up questions.

---

## CAP Theorem

A distributed system can guarantee at most **two** of the following three properties simultaneously:

| Property | Definition |
|---|---|
| **Consistency (C)** | Every read receives the most recent write or an error. All nodes see the same data at the same time. |
| **Availability (A)** | Every request to a non-failing node returns a response (no timeouts), but the data may be stale. |
| **Partition Tolerance (P)** | The system continues operating even when network partitions (message loss or delays) occur. |

**Key insight:** In the real world, network partitions are unavoidable, so the real trade-off is **CP vs AP**:

- **CP (Consistency + Partition Tolerance):** HBase, Zookeeper, MongoDB (default). Returns an error if a partition means the node can't guarantee up-to-date data.
- **AP (Availability + Partition Tolerance):** Cassandra, CouchDB, DynamoDB. Always responds, but may return stale data.
- **CA** is only possible without partitions — not realistic in distributed systems.

**Interview angle:** Be prepared to say which side your system should favor and why. Example: a financial trading system favors CP (stale prices are dangerous); a social media feed favors AP (showing a slightly stale feed is fine).

---

## Replication

Replication keeps copies of data on multiple nodes for **fault tolerance** and **read throughput**.

### Primary-Replica (Leader-Follower)
- All writes go to the **primary/leader**.
- Replicas receive changes asynchronously (or synchronously for strong consistency).
- On leader failure, a follower is promoted via leader election.

### Quorum Reads/Writes
Given `N` replicas, `W` write quorum, `R` read quorum:

```
R + W > N  →  strong consistency guaranteed
```

Common configurations for `N=3`:
- `W=2, R=2` — balanced strong consistency
- `W=1, R=3` — fast writes, slower reads
- `W=3, R=1` — slower writes (durable), fast reads

**Example question:** "In a 5-node Cassandra cluster, what quorum settings would you pick for a feature store that needs fast reads but can tolerate slight write lag?"

---

## Leader Election

When the current leader fails, nodes must agree on a new leader without human intervention.

### Bully Algorithm (simple, sync)
- Each node has a unique numeric ID.
- On leader failure, the node with the highest ID claims leadership.
- Downside: frequent re-elections if the highest-ID node is unstable.

### Raft / Paxos (production-grade)
- Nodes have states: `Follower`, `Candidate`, `Leader`.
- Leader sends periodic heartbeats; if followers time out, they start an election.
- A candidate wins by receiving majority votes.
- Used by: etcd, Zookeeper, CockroachDB.

---

## Redundancy vs. Replication

| Concept | Purpose |
|---|---|
| **Redundancy** | Having backup components (e.g., standby nodes) to survive failures — eliminates single points of failure. |
| **Replication** | Continuously syncing data across nodes — enables both fault tolerance and horizontal read scaling. |

---

## Message Queues

Message queues decouple producers from consumers, enabling asynchronous, buffered data pipelines.

- **Producer** writes messages to a topic/queue.
- **Consumer** reads from the queue at its own pace.
- Messages are retained until consumed (or TTL expires).

**Kafka specifics** (most common in DE interviews):
- Topics are split into **partitions** — the unit of parallelism.
- Each partition is an ordered, immutable log.
- Consumers track their position using **offsets**.
- Consumer **groups** allow multiple consumers to share a topic's partitions.
- Kafka retains messages for a configurable period (TTL), enabling replay.

---

## Key Interview Questions to Practice

1. **CAP:** "You're building a real-time leaderboard. Which CAP trade-off do you make and why?"
2. **Quorum:** "Your Cassandra cluster has 5 nodes. A network partition isolates 2 nodes. With W=3, R=2, what happens to reads and writes?"
3. **Replication lag:** "How do you prevent a model from being trained on stale features due to replication lag in your offline store?"
4. **Leader election:** "Your ETL coordinator crashes mid-job. How do you ensure exactly one node resumes the job without re-processing?"
