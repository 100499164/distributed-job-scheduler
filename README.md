# Distributed Job Scheduler

## Author

**Daniel Robles Ruiz**

Built as a personal project to explore the challenges behind distributed scheduling, fault recovery and worker coordination using Python and PostgreSQL.

## Overview

A distributed job scheduler built in Python, using PostgreSQL as the coordination and persistence layer.

Jobs are split into smaller tasks and executed by multiple workers over HTTP. The scheduler handles worker failures, retries, leases, heartbeats and recovery while PostgreSQL keeps track of task ownership and job progress.

The project currently includes three built-in workloads:

| Workload         | Partitioning                            | Final result                            |
| ---------------- | --------------------------------------- | --------------------------------------- |
| `PRIME_COUNT`    | Integer ranges                          | `totalPrimeCount`                       |
| `RANGE_SUM`      | Integer ranges                          | `totalSum`                              |
| `MONTE_CARLO_PI` | Sample batches with deterministic seeds | `samples`, `insideCircle`, `piEstimate` |

```text
Client ──HTTP──> API ──────────────────┐
                                      v
Workers ──HTTP──> Scheduler ───────> PostgreSQL
                      │
                      └── Recovery
```

The API and scheduler run as separate control-plane roles over the same database. Workers only execute workloads registered by the application; jobs cannot submit arbitrary code.

The scheduler uses at-least-once execution, so a physical computation may run more than once after failures. PostgreSQL ensures that only the current attempt can publish a new canonical result.

## Design decisions

A few choices shape how the scheduler behaves:

- **PostgreSQL is the coordination layer.** Task ownership, worker capacity, attempts, retries and job progress live in the same transactional system instead of being split across PostgreSQL and a separate message broker.

- **Execution is at-least-once.** A computation may run more than once after failures, but only the current attempt can publish the canonical result.

- **Workers execute a fixed workload catalog.** They do not accept arbitrary user code, which keeps scheduling, validation and recovery predictable.

- **Worker capabilities are explicit.** Workers announce which operations they support, and the scheduler only assigns compatible tasks.

- **Fairness is approximate.** The scheduler tries to rotate work between eligible jobs, but concurrent transactions may observe the same ordering and produce short bursts.

- **CPU-bound scaling uses processes.** The built-in workloads are CPU-heavy, so horizontal worker processes are preferred over increasing Python thread count.


## Quick start

Requirements:

* Docker with Linux containers
* Docker Compose v2

Start the full system:

```sh
docker compose up --build
```

This starts PostgreSQL, the API, the scheduler and three workers.

The API is available at:

```text
http://127.0.0.1:8080
```

PostgreSQL and the scheduler remain inside the Compose network.

On the first startup, the deployment creates a random database password in the `db-auth` volume. PostgreSQL data is stored in `pgdata`.

A normal:

```sh
docker compose down
```

keeps both volumes.

To run a basic demonstration:

```sh
python deploy/demo.py
```

For a larger mixed-workload example:

```sh
python deploy/showcase.py
```

The basic demo creates a job, repeats the request to check idempotency, follows its progress and compares the final prime count against an independent sequential sieve.

On Windows, `deploy/verify.ps1` can be used to start the stack and run the demo.


## Creating a job

Jobs are submitted to:

```text
POST /v1/jobs
```

with an `Idempotency-Key` header.

Example:

```json
{
  "name": "prime-example",
  "taskCount": 32,
  "payload": {
    "operation": "PRIME_COUNT",
    "fromInclusive": 2,
    "toExclusive": 1000000
  },
  "maxRetries": 3
}
```

Other supported workloads:

```json
{
  "name": "sum-example",
  "taskCount": 16,
  "payload": {
    "operation": "RANGE_SUM",
    "fromInclusive": 1,
    "toExclusive": 1000001
  }
}
```

```json
{
  "name": "pi-estimation",
  "taskCount": 32,
  "payload": {
    "operation": "MONTE_CARLO_PI",
    "samples": 1000000,
    "seed": 123456
  }
}
```

For the examples above, `PRIME_COUNT` produces:

```json
{"totalPrimeCount": 78498}
```

and `RANGE_SUM` produces:

```json
{"totalSum": 500000500000}
```

Monte Carlo produces a deterministic estimate for the same seed and partition count.

Integer ranges use the half-open interval:

```text
[fromInclusive, toExclusive)
```

Work is distributed as evenly as possible between tasks, without empty partitions, gaps or overlaps.

## Inspecting jobs and tasks

Useful endpoints include:

```text
GET /v1/jobs
GET /v1/jobs/{id}
GET /v1/jobs/{id}/tasks
GET /v1/tasks/{id}/attempts
GET /v1/workers
GET /v1/workers/{id}
```

For example:

```text
GET /v1/jobs?status=RUNNING&limit=50
```

List endpoints use cursor-based pagination through `items` and `nextCursor`.

Attempt history can be inspected through:

```text
GET /v1/tasks/{id}/attempts
```

which is useful when looking at retries, worker failures or recovered tasks.


## Worker capabilities

Workers announce the operations they support when registering with the scheduler.

For example:

```sh
WORKER_OPERATIONS=PRIME_COUNT,RANGE_SUM python -m scheduler.worker.runtime
```

or:

```sh
WORKER_OPERATIONS=MONTE_CARLO_PI python -m scheduler.worker.runtime
```

When `WORKER_OPERATIONS` is not set, the worker supports all three operations.

The scheduler only assigns a task when the worker:

* has a valid session;
* has free capacity;
* supports the task's operation.

Capabilities remain fixed for the lifetime of that worker session.

This allows deployments with specialized worker pools while keeping general-purpose workers possible.

## Scheduling

Scheduling is capability-aware and attempts to distribute assignments fairly between jobs.

The scheduler prefers compatible jobs that have never received an assignment, then jobs that have waited the longest since their previous assignment. Within a selected job, it claims the oldest eligible task using PostgreSQL row locks and `SKIP LOCKED`.

Fairness is intentionally approximate rather than strict. Concurrent scheduler transactions can observe the same ordering, so short bursts are possible.

What remains strict is:

* worker capacity;
* operation compatibility;
* task ownership;
* one active attempt per task;
* one canonical successful result per task.

If no compatible worker is available, the job remains queued. Empty claims do not consume retry budget.


## Worker failures and recovery

Workers periodically send heartbeats while tasks are running.

The scheduler uses leases and persisted attempt state to recover from situations such as:

* a worker disappearing;
* an assignment never starting;
* a running attempt exceeding its lease;
* the scheduler restarting;
* a response or completion acknowledgement being lost.

When an attempt can be retried, the task is placed back into the scheduling pool after backoff.

A stale worker cannot overwrite the result of a newer attempt. If a completion was already accepted but its HTTP acknowledgement was lost, the worker can safely retry the request and receive the historical acknowledgement.

Execution is **at-least-once**, not exactly-once. The same physical computation may therefore run more than once during failures.

## Graceful worker shutdown

`SIGTERM` and `SIGINT` put a worker into draining mode.

While draining, the worker:

* stops requesting new work;
* continues heartbeats for active attempts;
* waits for running computations;
* sends pending completions.

The default drain timeout is 30 seconds:

```text
WORKER_DRAIN_TIMEOUT_SECONDS=30
```

Docker Compose gives workers a 45-second grace period by default.

If work cannot finish before the deadline, the process exits and the persisted lease/recovery mechanism handles the unfinished attempt.

## Scaling workers

Workers are stateless apart from their current process-local execution slots, so more instances can be started with Compose:

```sh
docker compose up -d --scale worker=8
```

The default worker capacity is one task:

```text
WORKER_CAPACITY=1
```

and can be configured up to 64.

The included workloads are CPU-bound, so multiple worker processes with capacity 1 are generally more useful than increasing the number of Python threads inside one process.

## Observability

The control plane exposes:

```text
GET /health/live
GET /health/ready
GET /metrics
```

Liveness reports whether the process is responding.

Readiness also checks PostgreSQL access, schema migration checksums and required database permissions.

The scheduler exports metrics for areas such as:

* worker and task state;
* available worker slots;
* queue age;
* HTTP latency;
* assignments and completions;
* retries and expirations;
* database errors;
* recovery duration.

Workers write their local execution metrics to a Prometheus textfile when `WORKER_METRICS_FILE` is configured.

To inspect scheduler metrics in Compose:

```sh
docker compose exec scheduler python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8080/metrics').read().decode())"
```

Worker metrics can be inspected with:

```sh
docker compose exec --index 1 worker cat /tmp/worker-metrics.prom
```

Application logs are structured as JSON and include identifiers needed to follow jobs, tasks and attempts without putting high-cardinality IDs into metric labels.

## Demos

Several scripts are included to exercise different parts of the system.

| Demo                          | Command                                                                                                            |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Basic prime-count demo        | `python deploy/demo.py`                                                                                            |
| Mixed workloads               | `python deploy/showcase.py`                                                                                        |
| Capability-aware worker pools | `python deploy/capability_showcase.py`                                                                             |
| Fair scheduling example       | `python deploy/fairness_showcase.py`                                                                               |
| Worker failure                | `python benchmarks/faults.py --fault worker --output verification/worker-fault.json`                               |
| Short scheduler restart       | `python benchmarks/faults.py --fault scheduler-short --output verification/scheduler-short.json`                   |
| Long scheduler restart        | `python benchmarks/faults.py --fault scheduler-long --outage-seconds 35 --output verification/scheduler-long.json` |

The fault demos intentionally stop real worker or scheduler processes, so they should be run against a local demonstration deployment.

`capability_showcase.py` creates an isolated Compose project with specialized workers and removes only the temporary containers and volumes that it created.

## Running without Docker

The scheduler can also run directly against a PostgreSQL instance.

Install the dependencies and configure:

```text
DATABASE_URL
```

Then run the API:

```sh
export DATABASE_URL='postgresql://user:password@127.0.0.1:5432/scheduler'
export ROLE=api

python -m uvicorn scheduler.control_plane.app:create_app \
  --factory \
  --host 127.0.0.1 \
  --port 8080 \
  --no-access-log
```

Start the scheduler in another terminal:

```sh
export DATABASE_URL='postgresql://user:password@127.0.0.1:5432/scheduler'
export ROLE=scheduler

python -m uvicorn scheduler.control_plane.app:create_app \
  --factory \
  --host 127.0.0.1 \
  --port 8081 \
  --no-access-log
```

And start one or more workers:

```sh
export SCHEDULER_URL='http://127.0.0.1:8081'

python -m scheduler.worker.runtime
```

The API applies schema migrations. The scheduler waits for the expected schema before becoming ready.

## Configuration

| Variable                       | Default / purpose                                  |
| ------------------------------ | -------------------------------------------------- |
| `DATABASE_URL`                 | PostgreSQL connection string for API and scheduler |
| `DATABASE_PASSWORD_FILE`       | Optional password file, used by Compose            |
| `ROLE`                         | `api` or `scheduler`                               |
| `SCHEDULER_URL`                | `http://127.0.0.1:8081` for native workers         |
| `WORKER_CAPACITY`              | `1`                                                |
| `WORKER_OPERATIONS`            | All supported operations when omitted              |
| `WORKER_DRAIN_TIMEOUT_SECONDS` | `30`                                               |
| `POLL_INTERVAL_MS`             | `500`                                              |
| `HEARTBEAT_INTERVAL_MS`        | `5000`                                             |
| `WORKER_TIMEOUT_MS`            | `30000`                                            |
| `ASSIGNMENT_TIMEOUT_MS`        | `15000`                                            |
| `EXECUTION_LEASE_MS`           | `30000`                                            |
| `MAX_EXECUTION_MS`             | `300000`                                           |
| `RECOVERY_INTERVAL_MS`         | `1000`                                             |
| `DEFAULT_MAX_RETRIES`          | `3`                                                |
| `WORKER_METRICS_FILE`          | Optional Prometheus textfile path                  |

`BODY_LIMIT` controls the maximum HTTP request size and defaults to 64 KiB.

`MAX_TASKS` and `MAX_CAPACITY` can be used to lower the built-in protocol limits of 10,000 tasks per job and worker capacity 64.


## Development

The project targets Python 3.12+.

Create a virtual environment and install development dependencies:

```sh
python -m venv .venv
source .venv/bin/activate

python -m pip install -r requirements-dev.lock
python -m pip install --no-deps --no-build-isolation -e .
```

On Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.lock
.\.venv\Scripts\python.exe -m pip install --no-deps --no-build-isolation -e .
```

Run the static checks:

```sh
ruff check .
ruff format --check .
mypy
```

Run the test suite:

```sh
python -m pytest
```

Coverage:

```sh
python -m pytest \
  --cov=scheduler \
  --cov-report=term-missing \
  --cov-report=xml
```

The integration tests use a real PostgreSQL database through Testcontainers by default, so Docker must be available.

You can instead provide an explicit test database:

```sh
export TEST_DATABASE_URL='postgresql://user:password@127.0.0.1:5432/scheduler_test'
python -m pytest
```

The database must be dedicated to testing because the suite clears its working tables.

The test suite includes:

* unit tests;
* PostgreSQL integration tests;
* concurrency tests;
* end-to-end HTTP tests;
* worker and scheduler fault injection.

The E2E tests run real worker/control-plane processes and validate recovery behavior across failures.

## Benchmarks

The repository includes a few benchmarks aimed at understanding scheduler behavior rather than producing a single headline number.

### CPU workload

```sh
python benchmarks/run.py
```

`PRIME_COUNT` is used as the main CPU-bound workload. The benchmark includes warm-up runs and repeated measurements for throughput, speedup and efficiency.

`RANGE_SUM` intentionally uses an O(1) formula and is therefore not useful as a CPU scaling benchmark.

### Scheduler selection

```sh
python benchmarks/selection_scaling.py
```

This measures task-selection latency as the number of jobs, historical attempts and locked candidates increases.

The benchmark also records the number of SQL queries needed for each selection.

### Query plans

```sh
python benchmarks/explain_scheduling.py
```

This creates a temporary PostgreSQL instance through Testcontainers and compares `EXPLAIN ANALYZE` output with and without the per-job eligible-task index.

The script runs against a disposable database and does not modify the normal Compose database.

### Sample results

On a local Docker environment, the scheduler selection benchmark showed:

| Scenario                |      p50 |      p95 | Queries |
| ----------------------- | -------: | -------: | ------: |
| 10 jobs, no contention  |  2.57 ms |  2.75 ms |       2 |
| 100 jobs, no contention |  2.12 ms |  2.34 ms |       2 |
| 100 jobs, 99 locked     | 14.05 ms | 14.94 ms |     101 |

The PostgreSQL query-plan benchmark also showed the impact of the `tasks_eligible_per_job` index when locking an eligible task:

* with the index: ~0.05 ms execution time;
* without the index: ~2.33 ms;
* without the index, PostgreSQL filtered roughly 18,800 rows before finding the task.

These numbers are environment-dependent and are included as a reference rather than a performance guarantee.


## Database migrations

Schema changes are stored in:

```text
scheduler/persistence/migrations/
```

Applied migrations are recorded together with a SHA-256 checksum.

Once a migration has been applied, its file should not be edited. A changed checksum is treated as an error rather than silently modifying migration history.

Current migrations include:

* `001_initial.sql` — initial scheduler schema;
* `002_workloads.sql` — additional workloads;
* `003_worker_capabilities.sql` — worker capability routing and scheduling index.

Migration application is serialized with a PostgreSQL advisory lock so multiple control-plane processes do not attempt to install the schema at the same time.

## Failure model and limitations

Some important design boundaries:

* execution is **at-least-once**, not exactly-once;
* PostgreSQL allows at most one active attempt and one canonical success per task;
* only the current attempt can publish a new result;
* retries are bounded;
* PostgreSQL is the system of record and therefore a single point of failure in this deployment;
* the project does not provide PostgreSQL high availability;
* scheduling fairness is approximate rather than strict;
* there is no job cancellation;
* there is no authentication or authorization layer;
* workers are assumed to run in a trusted environment;
* mixed-version rolling upgrades are not supported.

The project intentionally keeps PostgreSQL as the only coordination system instead of adding a message broker or cache.

That keeps task reservation, worker capacity, attempt ownership and job progress inside the same transactional boundary, at the cost of placing more coordination load on PostgreSQL.

