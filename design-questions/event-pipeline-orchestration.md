# Design: Event-Driven Pipeline Orchestration

**Difficulty:** Medium
**Time:** ~30–45 minutes
**Topics:** Kafka, Airflow/Argo, event-driven ETL, workflow orchestration, multi-tenancy

---

## Overview

This question explores how to connect discrete ML pipeline steps together using events, without requiring each step to know about the next. The system you design should be generic enough to work for any sequence of data processing jobs, not just ML training workflows.

---

## Problem Statement

You work on a platform team that supports many ML teams. Each ML team has a multi-step pipeline:

1. A **data annotation job** runs and writes labeled data to S3.
2. A **Spark job** reads from S3, cleans the data, and deduplicates it.
3. A **model training job** reads the cleaned data and trains a new model version.
4. A **model deployment job** takes the trained model and deploys it to a serving endpoint.

Currently, these steps are chained together with shell scripts and cron jobs. Teams copy-paste each other's pipeline code. When one step fails, there is no automatic retry or alerting. When a team wants to add a new step (e.g., data validation between annotation and cleaning), they must modify the existing glue code.

**Design an automation platform that:**
- Connects pipeline steps via events (each step emits an event when it completes, triggering the next step).
- Requires **minimal code changes** to existing jobs (they should only need to emit a completion event).
- Is **tech-agnostic**: works with Spark, Python scripts, Kubernetes jobs, or any other execution environment.
- Is **extensible**: new steps can be inserted without modifying existing steps.
- Supports **multiple teams** (multi-tenant).

---

## Requirements Clarification

Before designing, consider asking (or thinking through):

- **Delivery guarantee:** If the platform crashes between steps 2 and 3, should step 3 run again from scratch, or resume? → At-least-once delivery is usually acceptable for batch pipeline steps; idempotent jobs make this safe.
- **Latency requirements:** Is this a near-real-time pipeline (seconds) or a batch pipeline (minutes to hours)? → Likely batch; tolerate seconds to minutes of latency between steps.
- **Failure handling:** If step 3 fails, should the platform retry automatically? How many times? → Yes; configurable retry with backoff.
- **Visibility:** Do teams need to see the status of their pipeline runs? → Yes; a dashboard or at minimum logs.
- **Isolation:** Should one team's pipeline failures affect another team? → No; tenant isolation is a hard requirement.

---

## Building Blocks

### 1. Event Queue (Kafka)

Each pipeline step, on completion, publishes a **completion event** to a Kafka topic.

```
Topic: pipeline.events

Message schema:
{
  "event_id":    "uuid",
  "event_type":  "job.completed",        // or job.failed, job.started
  "tenant_id":   "team-alpha",
  "pipeline_id": "annotation-to-deploy",
  "step_id":     "data-annotation",
  "run_id":      "uuid",                 // unique per pipeline execution
  "timestamp":   "2024-01-15T10:23:00Z",
  "payload": {
    "output_s3_path": "s3://bucket/team-alpha/run-uuid/annotations/",
    "record_count":   150000
  }
}
```

Why Kafka:
- Durable: messages are retained even if the consumer is temporarily down.
- Replayable: if the orchestrator crashes, it can re-read events from the last committed offset.
- Decoupled: job code only needs a Kafka client; it does not need to call the orchestrator directly.

### 2. Event Listeners

A listener is a long-running process that subscribes to the Kafka topic and routes events to the appropriate workflow trigger.

```
[Kafka: pipeline.events]
        │
        ▼
[Event Router / Listener Service]
  - Reads events
  - Looks up: "which workflows are triggered by this event_type + step_id?"
  - Calls the workflow engine to start the next step
  - Commits Kafka offset after successful dispatch
```

The routing configuration (which events trigger which downstream steps) is stored in a database or configuration files, not hardcoded in the listener.

### 3. Connectors

A connector is an adapter that translates a workflow trigger into an actual job execution in a specific execution environment.

| Connector | What it does |
|---|---|
| `SparkConnector` | Submits a Spark job to a cluster (via `spark-submit` or Spark REST API) |
| `KubernetesJobConnector` | Creates a Kubernetes Job manifest and applies it |
| `AirflowConnector` | Triggers an Airflow DAG run via the Airflow REST API |
| `LambdaConnector` | Invokes an AWS Lambda function |
| `ScriptConnector` | SSH-executes a shell script on a VM (legacy support) |

New execution environments require only a new connector class. Existing steps are unchanged.

### 4. Workflow Orchestration (Airflow or Argo)

For more complex workflows with branching, fan-out, or conditional logic, use a dedicated orchestration engine.

**Option A: Apache Airflow**
- Define each pipeline as a DAG (Directed Acyclic Graph).
- Steps are Airflow operators.
- Good for: human-readable DAG definitions, rich UI, mature ecosystem.
- Limitation: DAG definitions are Python code; changing the pipeline requires code deployment.

**Option B: Argo Workflows (Kubernetes-native)**
- Define workflows as YAML manifests.
- Each step is a container image.
- Good for: Kubernetes shops, GitOps workflows, declarative configuration.
- Limitation: more complex to set up; YAML verbosity.

**Recommendation for this platform:** Use a lightweight event-to-workflow trigger layer (the listener + connector pattern above) for simple linear pipelines, and integrate with Airflow or Argo for complex workflows. The event queue is always the entry point; the orchestration engine is just one type of connector.

### 5. Container Images and CI/CD

Each pipeline step should be packaged as a container image:
- Reproducible: the same image runs in dev, staging, and production.
- Versioned: image tags correspond to code versions.
- Isolated: each step's dependencies do not conflict with others.

CI/CD pipeline for a step:
```
[Code push to git]
     │
     ▼
[CI: run unit tests]
     │
     ▼
[Build container image, tag with git SHA]
     │
     ▼
[Push image to registry]
     │
     ▼
[Update pipeline config to reference new image tag]
```

---

## Event Schema Design

### Principles

1. **Include the run_id.** Every event in a single pipeline execution shares the same `run_id`. This makes it possible to trace the full execution history of one pipeline run across all steps.

2. **Include the tenant_id.** Required for routing, isolation, and auditing.

3. **Include a schema version.** Consumers need to handle schema evolution gracefully.

4. **Keep the payload small.** The event should contain metadata and pointers (S3 paths, IDs), not the actual data. Large payloads slow down Kafka and complicate consumers.

5. **Distinguish event types clearly.** `job.started`, `job.completed`, `job.failed` are different events and should be handled differently by downstream listeners.

### Schema Evolution

As pipeline steps add new output fields, the event schema will evolve. Use a schema registry (e.g., Apache Avro with Confluent Schema Registry, or JSON Schema) to:
- Enforce backward compatibility (new fields have defaults).
- Prevent breaking changes (required fields cannot be removed).
- Allow consumers to deserialize older events.

---

## Delivery Guarantees

| Guarantee | Mechanism | Tradeoff |
|---|---|---|
| At-most-once | Commit offset before processing | Fast; messages lost on consumer crash |
| At-least-once | Commit offset after processing | Safe; downstream must be idempotent |
| Exactly-once | Transactional producer + consumer | Complex; highest latency |

For this platform, **at-least-once** is the right choice. Pipeline steps must be designed to be idempotent:
- Use `run_id` as an idempotency key.
- If a step is triggered twice with the same `run_id`, it should detect the duplicate (e.g., check if output already exists in S3) and skip re-processing.

---

## Latency Considerations

In a batch pipeline, latency between steps (the time from "step N completes" to "step N+1 starts") is dominated by:
1. Kafka producer → broker latency: ~5–50ms
2. Listener poll interval: configurable, typically 1–10 seconds
3. Orchestrator scheduling delay: 1–30 seconds depending on the engine
4. Container startup time: 10–60 seconds for Kubernetes

Total inter-step latency: typically 30 seconds to 2 minutes. This is acceptable for batch pipelines but not for streaming or near-real-time pipelines.

If lower latency is needed, the listener can be replaced with a streaming processor (e.g., Flink) and container startup can be eliminated by using long-running worker pools.

---

## Monitoring, Alerting, and Logging

### Metrics to Track

- **Pipeline run duration:** Per step and end-to-end. Alert on P95 regressions.
- **Step failure rate:** Alert when failure rate exceeds threshold.
- **Event queue lag:** Monitor Kafka consumer group lag. Alert if lag grows (listener is falling behind).
- **Retry count per step:** High retry counts indicate a flaky step.

### Logging

Every event emitted by a step should include the `run_id` and `step_id`. Logs from all steps in a run are correlated by `run_id` in a centralized log aggregation system (e.g., Elasticsearch, Splunk). This makes debugging a failed pipeline run tractable.

### Alerting

- On `job.failed` events, send an alert to the team's Slack channel or PagerDuty.
- After N retries with no success, escalate to an on-call engineer.
- Alert the platform team if the event router itself has errors.

---

## Testing

### Unit Tests with Mocks

Test each connector independently:
```python
def test_spark_connector_submits_correct_job():
    mock_spark_client = Mock()
    connector = SparkConnector(client=mock_spark_client)
    event = JobCompletedEvent(step_id="data-annotation", run_id="test-run-1", ...)

    connector.handle(event)

    mock_spark_client.submit.assert_called_once_with(
        job_name="data-cleaning",
        args=["--input", event.payload["output_s3_path"], "--run-id", "test-run-1"]
    )
```

Test the event router:
```python
def test_router_triggers_correct_downstream_step():
    router = EventRouter(config=PIPELINE_CONFIG)
    event = JobCompletedEvent(step_id="data-annotation", tenant_id="team-alpha")
    triggered = router.route(event)
    assert triggered == ["data-cleaning"]
```

### End-to-End Tests with Small Datasets

Run the full pipeline on a small synthetic dataset:
1. Upload 100 synthetic annotation records to a test S3 path.
2. Emit a `job.completed` event for the annotation step.
3. Assert that the cleaning step is triggered (e.g., a Kubernetes Job is created).
4. Run the cleaning step against the test data.
5. Assert the output exists and has the expected record count.

Use a dedicated test tenant and test Kafka topic to isolate e2e tests from production.

---

## Multi-Tenancy and Isolation

### Tenant Isolation Options

**Option A: Separate Kafka topics per tenant**
```
pipeline.events.team-alpha
pipeline.events.team-beta
```
Pro: complete isolation; one team's high-volume pipeline cannot starve another.
Con: operational overhead grows with number of tenants.

**Option B: Single shared topic with tenant filtering**
```
pipeline.events  (all tenants, filtered by tenant_id field)
```
Pro: simple; one topic to manage.
Con: a misconfigured consumer could accidentally process another tenant's events; a noisy tenant affects all consumers.

**Option C: Partition by tenant_id hash**
Single topic, partitioned so each tenant's events land on specific partitions. The event router is assigned all partitions but enforces tenant isolation in code.

**Recommendation:** For a small number of tenants (< 50), separate topics per tenant is operationally manageable and provides the strongest isolation. For hundreds of tenants, use a shared topic with strict consumer-side filtering and quotas enforced at the Kafka producer level.

### Resource Quotas

Use Kafka producer quotas to prevent a single tenant from saturating the event bus:
```
# Kafka broker config
quota.producer.default=10485760    # 10 MB/s default
quota.producer.override.team-alpha=52428800  # 50 MB/s for high-volume team
```

At the orchestrator level, limit concurrent pipeline runs per tenant to prevent one team from exhausting shared compute resources.

### GitOps for Configuration

Store pipeline configurations (which events trigger which steps, connector parameters, retry policies) in a Git repository. Changes to pipeline configurations go through code review and CI validation before being applied to the platform. This provides:
- Audit trail: who changed which pipeline and when.
- Rollback: revert a bad configuration change with a git revert.
- Validation: CI checks that new configurations are syntactically valid and reference real step IDs.

### Authentication (JWT)

When a step emits a completion event, the platform needs to verify that the event is legitimate (i.e., it actually came from that team's job, not spoofed).

Use short-lived JWT tokens:
1. Before a step starts, the orchestrator issues a JWT token scoped to that specific `(tenant_id, run_id, step_id)`.
2. The step includes the JWT in its completion event.
3. The event router validates the JWT before acting on the event.
4. The JWT expires after the step's maximum expected duration.

---

## Architecture Diagram

```
                        ┌──────────────────────────────────────────┐
                        │           PIPELINE PLATFORM              │
                        │                                          │
[Team Alpha Job: Step 1]│    ┌─────────────────────────────────┐   │
  (data annotation)     │    │   Event Queue (Kafka)           │   │
  → emit job.completed ─┼──> │   Topic: pipeline.events.alpha  │   │
                        │    └──────────────┬──────────────────┘   │
                        │                   │                       │
                        │    ┌──────────────▼──────────────────┐   │
                        │    │   Event Router / Listener       │   │
                        │    │   - Validates JWT               │   │
                        │    │   - Looks up routing config     │   │
                        │    │   - Selects connector           │   │
                        │    └──────────────┬──────────────────┘   │
                        │                   │                       │
                        │    ┌──────────────▼──────────────────┐   │
                        │    │   Connectors                    │   │
                        │    │   ├── SparkConnector            │   │
                        │    │   ├── KubernetesJobConnector    │   │
                        │    │   ├── AirflowConnector          │   │
                        │    │   └── LambdaConnector           │   │
                        │    └──────────────┬──────────────────┘   │
                        │                   │                       │
                        └───────────────────┼──────────────────────┘
                                            │
                        ┌───────────────────▼──────────────────────┐
                        │  Execution Environments                   │
                        │  (Spark cluster, Kubernetes, Airflow...)  │
                        └──────────────────────────────────────────┘
```

---

## Follow-Up Questions

- **Versioning:** Two versions of the same pipeline run simultaneously (one with v1 cleaning logic, one with v2). How does the event router handle this?
- **DAG vs. linear:** What changes if the pipeline is not linear but has a fan-out (step 2 triggers steps 3a and 3b in parallel, and step 4 waits for both)?
- **Cost attribution:** How do you track compute costs per tenant per pipeline run?
- **Self-service:** How does a new team register their pipeline with the platform without involving the platform team?
- **Circuit breaker:** If a downstream execution environment (e.g., the Spark cluster) is unavailable, should the event router retry indefinitely or give up and alert?
