# Coding: Dependency Resolution (Graph Traversal & Topological Sort)

**Difficulty:** Easy → Hard (progressive)
**Time:** ~45 minutes
**Language:** Python
**Focus areas:** Graph algorithms, BFS/DFS, topological sort, DAGs, cycle detection

---

## Context

In data engineering, pipelines have dependencies: ETL jobs depend on upstream jobs completing first; services must start in dependency order; library builds require transitive dependencies. This problem abstracts that into a graph problem.

Given a dependency graph represented as an adjacency map (`node → list of dependencies`), implement the following parts progressively.

**Example graph:**

```python
graph = {
    "A": ["B", "C"],
    "B": ["D"],
    "C": ["D", "E"],
    "D": [],
    "E": [],
}
```

In this graph, `A` depends on `B` and `C`; `B` depends on `D`; etc.

---

## Part 1: Find All Dependencies (Easy)

Given an entry node, find **all** nodes it depends on, directly or transitively.

**Requirements:**
- Use BFS or DFS with a visited set
- Handle cycles gracefully (do not revisit nodes)
- Edge case: if the entry node is not in the graph, return an empty set
- Time complexity: O(E) where E = number of edges

**Example:**
```python
find_all_dependencies(graph, "A")
# Expected: {"B", "C", "D", "E"}

find_all_dependencies(graph, "B")
# Expected: {"D"}

find_all_dependencies(graph, "missing_node")
# Expected: set()
```

**Solution:**
```python
def find_all_dependencies(graph: dict, entry: str) -> set:
    visited = set()
    working_set = list(graph.get(entry, []))
    while working_set:
        node = working_set.pop()
        if node not in visited:
            visited.add(node)
            working_set.extend(graph.get(node, []))
    return visited
```

**Key points:**
- `graph.get(entry, [])` handles the missing node edge case cleanly
- The visited set prevents infinite loops on cyclic graphs
- Note: the entry node itself is **not** included in the returned set

---

## Part 2: Explain Why a Dependency is Needed (Easy)

Find **all paths** from the entry node to a specific target dependency node.

**Requirements:**
- Similar to Part 1, but track full paths rather than just visited nodes
- Returns a list of paths, where each path is a list of node names
- Time complexity: O(V!) worst case (exponential — all possible paths through the graph)

**Example:**
```python
find_paths(graph, "A", "D")
# Expected: [["A", "B", "D"], ["A", "C", "D"]]
```

**Solution:**
```python
def find_paths(graph: dict, entry: str, target: str) -> list:
    results = []
    stack = [[entry]]  # stack of in-progress paths

    while stack:
        path = stack.pop()
        current = path[-1]

        if current == target:
            results.append(path)
            continue

        for neighbor in graph.get(current, []):
            if neighbor not in path:  # avoid revisiting nodes on current path
                stack.append(path + [neighbor])

    return results
```

**Key points:**
- The `neighbor not in path` check prevents cycles within a single path traversal
- Unlike Part 1, the same node can appear across different paths
- This is useful for explaining why a slow or failing upstream job blocks a downstream job

---

## Part 3: Find the Longest Path (Medium)

Assuming parallel execution where independent nodes run simultaneously and each step takes one unit of time, what is the total wall-clock execution time? This equals the length of the longest path from the entry node to any leaf.

**Requirements:**
- Track the longest known path length to each node
- Only extend a path if the new route is longer than what was previously recorded
- Works correctly for DAGs; will loop infinitely on cyclic graphs (cycle detection is Part 4)
- Time complexity: O(V + E) for DAGs

**Example:**
```python
# In the example graph, A -> C -> E is length 3, A -> B -> D is also length 3
longest_path(graph, "A")
# Expected: 3
```

**Solution:**
```python
def longest_path(graph: dict, entry: str) -> int:
    # longest_to[node] = longest path length from entry to that node
    longest_to = {entry: 1}
    stack = [entry]

    while stack:
        node = stack.pop()
        for neighbor in graph.get(node, []):
            new_length = longest_to[node] + 1
            if new_length > longest_to.get(neighbor, 0):
                longest_to[neighbor] = new_length
                stack.append(neighbor)

    return max(longest_to.values())
```

**Key points:**
- The condition `new_length > longest_to.get(neighbor, 0)` ensures we only re-explore a node when we find a strictly longer path to it
- The maximum value across all nodes gives the critical path length (the bottleneck for parallel execution)
- On a cyclic graph, this will not terminate — cycle detection should be applied first

---

## Part 4: Compute Execution Ordering (Hard)

Produce a valid serial execution order where all dependencies of a node appear before the node itself. This is a **topological sort**.

**Requirements:**
- Use Kahn's algorithm (iterative BFS-based approach)
- Only include nodes reachable from the entry node (combine with Part 1)
- Detect and raise an error if a cycle exists among reachable nodes
- The result is not necessarily unique — any valid topological order is acceptable
- Time complexity: O(V + E)

**Example:**
```python
topological_sort(graph, "A")
# One valid result: ["D", "E", "B", "C", "A"]
# Another valid result: ["D", "B", "E", "C", "A"]
```

**Kahn's Algorithm Overview:**

1. Find all nodes reachable from the entry (using Part 1), plus the entry itself
2. For the reachable subgraph, compute the in-degree of each node (number of nodes that depend on it)
3. Start a queue with all nodes that have in-degree 0 (no dependents within the subgraph — these are the leaves)
4. Repeatedly dequeue a node, add it to the ordering, and decrement the in-degree of each node that depends on it
5. When a node's in-degree reaches 0, enqueue it
6. If the final ordering does not contain all reachable nodes, a cycle was detected

**Solution:**
```python
from collections import deque

def topological_sort(graph: dict, entry: str) -> list:
    # Step 1: find all reachable nodes including the entry
    reachable = find_all_dependencies(graph, entry) | {entry}

    # Step 2: build in-degree map for the reachable subgraph
    in_degree = {node: 0 for node in reachable}
    for node in reachable:
        for dep in graph.get(node, []):
            if dep in reachable:
                in_degree[dep] = in_degree.get(dep, 0) + 1

    # Step 3: seed the queue with leaf nodes (in-degree 0)
    queue = deque([n for n, d in in_degree.items() if d == 0])
    order = []

    # Step 4: process the queue
    while queue:
        node = queue.popleft()
        order.append(node)
        for dep in graph.get(node, []):
            if dep in reachable:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)

    # Step 5: cycle detection
    if len(order) != len(reachable):
        raise ValueError("Cycle detected in dependency graph")

    # Reverse so dependencies come before dependents
    return list(reversed(order))
```

**Key points:**
- Scoping to the reachable subgraph avoids sorting irrelevant nodes
- The cycle check (`len(order) != len(reachable)`) is a property of Kahn's algorithm: if a cycle exists, the nodes in the cycle will never reach in-degree 0 and will never be enqueued
- The final `reversed()` call flips the order from "dependents first" to "dependencies first"

---

## Complexity Summary

| Part | Problem | Time Complexity |
|------|---------|-----------------|
| 1 | Find all dependencies | O(E) |
| 2 | Find all paths to a node | O(V!) worst case |
| 3 | Longest path (critical path) | O(V + E) for DAGs |
| 4 | Topological sort | O(V + E) |

---

## Checklist

- [ ] Explain graph representation (adjacency map: node maps to list of its dependencies)
- [ ] BFS/DFS traversal with cycle handling via visited set
- [ ] Path tracking vs. node tracking (Part 1 vs. Part 2)
- [ ] Critical path reasoning for parallel execution (Part 3)
- [ ] Topological sort using Kahn's algorithm (Part 4)
- [ ] Cycle detection via in-degree invariant
- [ ] Time complexity analysis for each part

---

## Follow-Up Questions

**"How would you detect and report cycles, not just raise an error?"**

Use DFS with two states per node: "currently on the recursion stack" and "fully visited." If you encounter a node that is currently on the stack, you've found a cycle. Collect the path at that point to report exactly which nodes form the cycle.

```python
def find_cycle(graph: dict) -> list:
    """Returns a list of nodes forming a cycle, or empty list if none."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node: WHITE for node in graph}
    path = []

    def dfs(node):
        color[node] = GRAY
        path.append(node)
        for neighbor in graph.get(node, []):
            if color[neighbor] == GRAY:
                cycle_start = path.index(neighbor)
                return path[cycle_start:]
            if color[neighbor] == WHITE:
                result = dfs(neighbor)
                if result:
                    return result
        path.pop()
        color[node] = BLACK
        return []

    for node in graph:
        if color[node] == WHITE:
            cycle = dfs(node)
            if cycle:
                return cycle
    return []
```

**"How would you parallelize the execution?"**

Execute all nodes with in-degree 0 simultaneously as a batch. After each batch completes, decrement in-degrees and collect the next batch. This is essentially Kahn's algorithm where each iteration of the outer loop represents one parallel "wave" of execution.

```python
def parallel_execution_waves(graph: dict, entry: str) -> list:
    """Returns a list of batches; nodes within each batch can run in parallel."""
    reachable = find_all_dependencies(graph, entry) | {entry}
    in_degree = {node: 0 for node in reachable}
    for node in reachable:
        for dep in graph.get(node, []):
            if dep in reachable:
                in_degree[dep] += 1

    waves = []
    current_wave = [n for n, d in in_degree.items() if d == 0]
    while current_wave:
        waves.append(current_wave)
        next_wave = []
        for node in current_wave:
            for dep in graph.get(node, []):
                if dep in reachable:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        next_wave.append(dep)
        current_wave = next_wave

    return list(reversed(waves))  # dependencies-first order
```

**"How does this relate to workflow orchestration frameworks?"**

Workflow orchestrators such as Airflow and Argo Workflows represent pipelines as DAGs where tasks are nodes and dependencies are directed edges. The scheduler performs topological sort to determine which tasks are eligible to run. Each task only starts when all its upstream dependencies have completed successfully. The concepts map directly: this problem is the algorithmic core of what an orchestrator does.

**"What if a dependency fails mid-execution?"**

Mark the failed node and propagate a "blocked" status to all downstream nodes (those that depend on it, transitively). Retry policies can be applied at the node level before propagating failure. This is a reachability problem on the reverse graph: find all nodes reachable from the failed node in the graph where edges are reversed.
