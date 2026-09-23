import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from pydantic import ValidationError
from test_recovery import age_worker, eligible

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.recovery import Recovery
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import KNOWN_OPERATIONS, Claim, Completion, CreateJob, Register
from scheduler.worker.runtime import Slot, Worker, configured_operations
from scheduler.worker.workload import execute
from scheduler.workloads.catalog import UnsupportedOperation


def register(scheduler, operations=None, capacity=1):
    request = Register(
        workerId=uuid4(),
        hostname="capabilities-test",
        capacity=capacity,
        version="1",
        **({"supportedOperations": operations} if operations is not None else {}),
    )
    scheduler.register(request)
    return request.worker_id


def job(jobs, operation="PRIME_COUNT", count=4):
    payload = (
        {"operation": operation, "samples": 10000, "seed": 123456}
        if operation == "MONTE_CARLO_PI"
        else {"operation": operation, "fromInclusive": 2, "toExclusive": 10002}
    )
    return jobs.create(CreateJob(name="capabilities-test", taskCount=count, payload=payload), str(uuid4()))[0]


def claim(scheduler, worker):
    return scheduler.claim(Claim(workerId=worker, claimRequestId=uuid4()))


def finish(scheduler, worker, assignment):
    scheduler.start(assignment["attemptId"], worker)
    return scheduler.complete(
        assignment["attemptId"],
        Completion(workerId=worker, outcome="SUCCEEDED", result=execute(assignment["payload"])),
    )


@pytest.mark.parametrize(
    "operations", [[], ["UNKNOWN"], ["PRIME_COUNT", "PRIME_COUNT"], ["ALL"], [1], [True], "PRIME_COUNT", None]
)
def test_reject_invalid_capabilities(operations):
    with pytest.raises(ValidationError):
        Register(workerId=uuid4(), hostname="test", capacity=1, version="1", supportedOperations=operations)


@pytest.mark.parametrize("value", ["", " ", "PRIME_COUNT,", "PRIME_COUNT,UNKNOWN", "RANGE_SUM,RANGE_SUM"])
def test_invalid_environment(value):
    with pytest.raises(ValueError, match="WORKER_OPERATIONS"):
        configured_operations(value)


def test_environment_defaults_normalization_and_fail_fast():
    assert configured_operations(None) == list(KNOWN_OPERATIONS)
    assert configured_operations(" RANGE_SUM, PRIME_COUNT ") == ["PRIME_COUNT", "RANGE_SUM"]
    result = subprocess.run(
        [sys.executable, "-m", "scheduler.worker.runtime"],
        capture_output=True,
        text=True,
        env={**os.environ, "WORKER_OPERATIONS": "UNKNOWN"},
        timeout=5,
    )
    assert result.returncode != 0 and "WORKER_OPERATIONS" in result.stderr
    worker = Worker("unused", operations=["PRIME_COUNT"])
    try:
        with pytest.raises(UnsupportedOperation):
            worker._calculate(Slot({"payload": {"operation": "MONTE_CARLO_PI", "samples": 10, "seed": 1}}))
    finally:
        worker.executor.shutdown(wait=True)


@pytest.mark.integration
def test_registration_is_set_like_idempotent_and_immutable(db):
    scheduler = Scheduler(db, Settings())
    request = Register(
        workerId=uuid4(),
        hostname="test",
        capacity=2,
        version="1",
        supportedOperations=["RANGE_SUM", "PRIME_COUNT"],
    )
    first = scheduler.register(request)
    assert first["supportedOperations"] == ["PRIME_COUNT", "RANGE_SUM"]
    assert (
        scheduler.register(
            Register(
                **{**request.model_dump(by_alias=True), "supportedOperations": ["PRIME_COUNT", "RANGE_SUM"]}
            )
        )
        == first
    )
    assert (
        Jobs(db, Settings()).worker(request.worker_id)["supportedOperations"] == first["supportedOperations"]
    )
    with pytest.raises(Conflict) as error:
        scheduler.register(
            Register(**{**request.model_dump(by_alias=True), "supportedOperations": ["PRIME_COUNT"]})
        )
    assert error.value.code == "REGISTRATION_CONFLICT"


@pytest.mark.integration
@pytest.mark.parametrize("operations", [[], ["UNKNOWN"], ["PRIME_COUNT", "PRIME_COUNT"], [None]])
def test_database_capabilities_constraints(db, operations):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.run(
            lambda c: c.execute(
                """INSERT INTO workers(id,hostname,version,status,capacity,supported_operations)
            VALUES (%s,'test','1','ONLINE',1,%s)""",
                (uuid4(), operations),
            )
        )


@pytest.mark.integration
@pytest.mark.parametrize("operation", KNOWN_OPERATIONS)
def test_general_worker_can_execute_each_operation(db, operation):
    scheduler, jobs = Scheduler(db, Settings()), Jobs(db, Settings())
    created = job(jobs, operation, 1)
    worker = register(scheduler)
    assignment = claim(scheduler, worker)
    assert assignment["jobId"] == created["id"]
    assert assignment["payload"]["operation"] == operation
    finish(scheduler, worker, assignment)
    assert jobs.job(created["id"])["status"] == "COMPLETED"


@pytest.mark.integration
def test_no_compatible_workers_wait_then_compatible_slots_make_progress(db):
    scheduler, jobs = Scheduler(db, Settings()), Jobs(db, Settings())
    created = job(jobs, "MONTE_CARLO_PI", 2)
    incompatible = register(scheduler, ["PRIME_COUNT", "RANGE_SUM"], capacity=4)
    for _ in range(3):
        assert claim(scheduler, incompatible) is None
    tasks = jobs.list_tasks(created["id"], 200)["items"]
    assert all(t["status"] == "QUEUED" and t["attemptCount"] == 0 for t in tasks)
    assert jobs.job(created["id"])["status"] == "QUEUED"
    compatible = register(scheduler, ["MONTE_CARLO_PI"])
    first = claim(scheduler, compatible)
    with pytest.raises(Conflict) as error:
        claim(scheduler, compatible)
    assert error.value.code == "CAPACITY_EXHAUSTED"
    assert claim(scheduler, incompatible) is None
    finish(scheduler, compatible, first)
    second = claim(scheduler, compatible)
    finish(scheduler, compatible, second)
    assert jobs.job(created["id"])["status"] == "COMPLETED"


@pytest.mark.integration
def test_recovery_waits_for_compatible_replacement_preserving_payload(db):
    scheduler, jobs = Scheduler(db, Settings()), Jobs(db, Settings())
    created = job(jobs, "MONTE_CARLO_PI", 1)
    worker = register(scheduler, ["MONTE_CARLO_PI"])
    incompatible = register(scheduler, ["PRIME_COUNT"])
    old = claim(scheduler, worker)
    scheduler.start(old["attemptId"], worker)
    age_worker(db, worker)
    Recovery(scheduler).sweep()
    # Backoff cannot be bypassed even by a compatible worker.
    replacement = register(scheduler)
    assert claim(scheduler, replacement) is None
    eligible(db)
    assert claim(scheduler, incompatible) is None
    assert jobs.task(old["taskId"])["status"] == "RETRY_WAIT"
    new = claim(scheduler, replacement)
    assert new["payload"] == old["payload"]
    assert new["attemptNumber"] == 2
    finish(scheduler, replacement, new)
    with pytest.raises(Conflict):
        finish(scheduler, worker, old)
    assert jobs.job(created["id"])["status"] == "COMPLETED"


@pytest.mark.integration
def test_concurrent_scheduler_instances_preserve_routing_capacity_and_unique_tasks(db):
    jobs = Jobs(db, Settings())
    for operation in KNOWN_OPERATIONS:
        job(jobs, operation, 20)
    schedulers = [Scheduler(db, Settings()) for _ in range(12)]
    workers = [register(schedulers[0], [op], capacity=2) for op in KNOWN_OPERATIONS]
    workers.append(register(schedulers[0], capacity=2))
    barrier = Barrier(12)

    def attempt(index):
        barrier.wait(timeout=10)
        try:
            return schedulers[index].claim(Claim(workerId=workers[index % 4], claimRequestId=uuid4()))
        except Conflict as exc:
            assert exc.code == "CAPACITY_EXHAUSTED"
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        assignments = [a for a in pool.map(attempt, range(12)) if a]
    assert len(assignments) == 8
    assert len({a["taskId"] for a in assignments}) == 8
    from invariants import assert_invariants

    assert_invariants(db)
    rows = db.run(
        lambda c: c.execute("SELECT worker_id,count(*) AS n FROM task_attempts GROUP BY worker_id").fetchall()
    )
    assert all(r["n"] == 2 for r in rows)
