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