# Multi-thread Execution with Python ThreadPool

## Problem statements

Multi-thread Execution and Facebook Ads SDK Initialization Strategy
Background

Facebook Ads ETL pipeline supports parallel execution across multiple extract streams:

Campaign insights

Ad insights

Ad metadata

Ad creatives

Parallelism is required to:

Reduce end-to-end pipeline latency

Isolate failures between independent extract units

Scale horizontally on Cloud Run / Airflow / Kubernetes

However, naive multi-thread execution can introduce data inconsistency and race conditions if SDK clients are improperly shared.

Problem Statement
Shared SDK client in multi-threaded execution

The Facebook Ads Python SDK maintains internal mutable state, including but not limited to:

HTTP session

Retry counters

Pagination cursors

Request context

Error handling and backoff state

When a single global SDK client is initialized and reused across multiple threads:

Threads may overwrite request state of each other

Retry logic from one extract stream can interfere with another

Pagination cursors may be corrupted

Failures become non-deterministic and hard to reproduce

Data may be:

Missing

Duplicated

Incorrectly attributed to the wrong entity (ad_id, creative_id, etc.)

This issue does not always surface immediately, making it especially dangerous in production environments.

Why Sequential Execution Is Not Sufficient

Running extract steps sequentially guarantees consistency but introduces critical drawbacks:

Increased total execution time

Inefficient use of Cloud Run / compute resources

Reduced fault isolation (one failure blocks the entire pipeline)

Therefore, parallel execution is required, but it must be implemented safely.

Design Principle

Each extract unit must own its own Facebook Ads SDK client instance.

This principle applies regardless of whether parallelism is implemented via:

Python threads

Multiprocessing

Cloud Run multiple containers

Airflow tasks

Kubernetes jobs

Architecture Decision
SDK Initialization Responsibility

main.py does NOT initialize Facebook Ads SDK

Each extract function:

Initializes its own SDK client

Uses the same access token and account_id

Maintains full isolation from other extract units

This shifts SDK lifecycle ownership downstream to the extract layer.

## Technical Solutions

Execution Model
Incorrect (Shared Client)
main.py
 └─ FacebookAdsApi.init(...)
 └─ ThreadPoolExecutor
     ├─ extract_ad_creatives()
     ├─ extract_ad_metadata()


❌ Shared mutable SDK state
❌ High risk of race conditions
❌ Non-deterministic failures

Correct (Isolated Clients)
main.py
 └─ ThreadPoolExecutor
     ├─ extract_ad_creatives()
     │    └─ FacebookAdsApi.init(...)
     ├─ extract_ad_metadata()
          └─ FacebookAdsApi.init(...)


✅ Thread-safe execution
✅ Independent retry and pagination logic
✅ Deterministic and reproducible behavior

Why This Works

Facebook Ads access tokens are stateless

Multiple SDK clients can safely authenticate using the same token

SDK initialization cost is negligible compared to:

API calls

Retry backoff

Pagination loops

Each client maintains:

Its own HTTP session

Its own retry lifecycle

Its own error context

Benefits
Data Consistency

No shared state across extract streams

Guaranteed request isolation

Correct attribution of metrics and metadata

Fault Isolation

Failure in ad creative extraction does not affect ad insights

Retries are scoped to the failing extract unit only

Scalability

Seamless migration to:

Cloud Run parallel containers

Airflow task-level parallelism

Distributed workers

Observability

Cleaner logs

Easier debugging

Clear ownership of failures per extract stream

Final Recommendation

Always initialize Facebook Ads SDK inside extract functions

Never share SDK clients across threads

Treat each extract unit as an isolated execution context

This design ensures correctness first, while preserving performance and scalability of the ETL pipeline.

Got it. Below is a clean, decision-oriented technical doc in English, written as something you can drop straight into your repo / architecture docs.

Concurrency Strategy on Google Cloud Run

ThreadPool vs Multiple Tasks vs Multiple Services

1. Context

This service runs a Facebook Ads data pipeline that is:

I/O-bound (waiting on external APIs: Facebook, Google Cloud)

Light on CPU

Minimal in-memory processing

BigQuery cost is negligible compared to Cloud Run runtime cost

The main design question is:

How should we structure concurrency to minimize Cloud Run cost while keeping execution time reasonable?

2. Key Constraints of Cloud Run
Cloud Run execution model

Each Cloud Run service instance:

Has a fixed number of vCPUs

Can handle multiple concurrent requests (configurable)

Is billed by:

Instance time

Allocated CPU & memory

External API wait time still counts as billable time

Important implication

For I/O-bound workloads, CPU is mostly idle while waiting.
This makes in-process concurrency (threads) very cost-effective.

3. Three Architecture Options
Option A — 1 Cloud Run service, 1 request, ThreadPool (2 tasks)

Architecture

Cloud Run (1 instance, 1 vCPU)
└── main.py
    ├── ThreadPoolExecutor
    │   ├── Task A (campaign insights)
    │   └── Task B (ad insights)


Characteristics

Single Cloud Run request

Internal parallelism via threads

Both tasks share:

Memory

Network

CPU

Performance

Excellent for I/O-bound workloads

Threads release the GIL during network I/O

Near-optimal utilization of idle CPU time

Cost

Lowest Cloud Run cost

Only one instance running

No duplicate cold starts

Operational complexity

Slightly more complex code

Requires log buffering / output control

Best when

Tasks are independent

CPU usage is low

Failure of one task should fail the whole run

Option B — 1 Cloud Run service, 2 concurrent requests (no ThreadPool)

Architecture

Cloud Run (1 service)
├── Request 1 → Task A
└── Request 2 → Task B


Characteristics

Concurrency handled by Cloud Run

Each request runs in isolation

No in-process threading

Performance

Similar wall-clock time to Option A

Slight overhead from request lifecycle

Cost

Still 1 service

Possibly 1–2 instances, depending on concurrency & timing

Slightly higher cost than Option A

Operational complexity

Cleaner code

Easier logging & error isolation

Best when

Tasks may be triggered independently

You want isolation without managing threads

You accept slightly higher Cloud Run cost

Option C — 2 separate Cloud Run services

Architecture

Cloud Run Service A → Task A
Cloud Run Service B → Task B


Characteristics

Full isolation

Independent deploy & scaling

Independent failures

Performance

Parallel execution

Extra cold start overhead

Cost

Highest Cloud Run cost

Two services, two instances

Duplicate base runtime cost

Operational complexity

Most infrastructure overhead

CI/CD, monitoring, IAM duplicated

Best when

Tasks are owned by different teams

Very different schedules or SLAs

Strong isolation is required

4. Cost & Efficiency Comparison
Option	Cloud Run Instances	CPU Utilization	Cost	Complexity
A. 1 service + ThreadPool	1	High (for I/O)	⭐ Lowest	Medium
B. 1 service + concurrency	1–2	Medium	⭐⭐	Low
C. 2 services	2	Low	⭐⭐⭐ Highest	High
5. Why ThreadPool Is Ideal Here
Workload nature

External API calls dominate runtime

Local processing is minimal

CPU mostly waits on network I/O

Python behavior

Threads release the GIL during I/O

No meaningful CPU contention

ThreadPool efficiently overlaps waiting time

Cloud economics

Cloud Run bills wall-clock time

Reducing duplicated idle time saves money

One instance doing two waits is cheaper than two instances doing one wait each

Conclusion:
For I/O-bound pipelines, one Cloud Run service with internal concurrency is the most cost-efficient design.

6. Final Recommendation

Use:

1 Cloud Run service

1 request

ThreadPoolExecutor with 2 workers

1 vCPU

Rationale

Lowest Cloud Run cost

Minimal infrastructure

Best utilization of idle CPU time

Clean operational model for batch pipelines

7. When to Revisit This Decision

Reconsider if:

CPU-heavy transformations are added

Tasks need independent retries

One task becomes significantly slower than the other

Per-task SLAs diverge

Until then, ThreadPool inside a single Cloud Run instance is the optimal choice.