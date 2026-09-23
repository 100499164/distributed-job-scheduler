from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_claims import register
from test_recovery import age_worker, eligible

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.recovery import Recovery
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim, Completion, CreateJob
from scheduler.worker.workload import EXECUTORS, Cancelled, execute
from scheduler.workloads.catalog import WORKLOADS, UnsupportedOperation, partition_seed, partitions, workload

PAYLOADS = [
    {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": 100},
    {"operation": "RANGE_SUM", "fromInclusive": -11, "toExclusive": 100},
    {"operation": "MONTE_CARLO_PI", "samples": 10001, "seed": 123456},
]


def create_request(payload, count=3):
    return CreateJob(name="workloads", taskCount=count, payload=payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"operation": "UNKNOWN", "samples": 10, "seed": 0},
        {"operation": "RANGE_SUM", "fromInclusive": True, "toExclusive": 10},
        {"operation": "RANGE_SUM", "fromInclusive": "1", "toExclusive": 10},
        {"operation": "RANGE_SUM", "fromInclusive": 10, "toExclusive": 10},
        {"operation": "RANGE_SUM", "fromInclusive": -100000001, "toExclusive": 10},
        {"operation": "RANGE_SUM", "fromInclusive": 0, "toExclusive": 100000001},
        {"operation": "MONTE_CARLO_PI", "samples": 0, "seed": 0},
        {"operation": "MONTE_CARLO_PI", "samples": 100000001, "seed": 0},
        {"operation": "MONTE_CARLO_PI", "samples": 100, "seed": -1},
        {"operation": "MONTE_CARLO_PI", "samples": 100, "seed": 2**53},
        {"operation": "MONTE_CARLO_PI", "samples": 100.0, "seed": 0},
        {"operation": "MONTE_CARLO_PI", "samples": 100, "seed": True},
        {"operation": "MONTE_CARLO_PI", "samples": 100, "seed": 0, "extra": 1},
        {"operation": "MONTE_CARLO_PI", "samples": float("inf"), "seed": 0},
    ],
)
def test_strict_new_payloads(payload):
    with pytest.raises(ValidationError):
        create_request(payload)


@pytest.mark.parametrize("payload", PAYLOADS)
def test_task_count_and_partition_accounting(payload):
    request = create_request(payload)
    with pytest.raises(ValidationError):
        create_request(payload, request.payload.work_units + 1)
    parts = list(partitions(request.payload, 3))
    assert [i for i, _ in parts] == [0, 1, 2]
    assert (
        sum(workload(p["operation"]).payload_type.model_validate(p).work_units for _, p in parts)
        == request.payload.work_units
    )
    if "samples" in payload:
        assert len({p["seed"] for _, p in parts}) == 3
        assert parts == list(partitions(request.payload, 3))
        assert [p["seed"] for _, p in parts] == [partition_seed(payload["seed"], i) for i in range(3)]
    else:
        assert parts[0][1]["fromInclusive"] == payload["fromInclusive"]
        assert parts[-1][1]["toExclusive"] == payload["toExclusive"]
        assert all(
            left[1]["toExclusive"] == right[1]["fromInclusive"] for left, right in zip(parts, parts[1:])
        )
    with pytest.raises(ValueError):
        list(partitions(request.payload, 0))


def test_legacy_default_and_registry_coverage():
    implicit = create_request({"fromInclusive": 2, "toExclusive": 100})
    assert implicit == create_request(PAYLOADS[0])
    assert set(WORKLOADS) == set(EXECUTORS)
    with pytest.raises(UnsupportedOperation):
        execute({"operation": "PYTHON", "code": "anything"})


@pytest.mark.parametrize("payload", PAYLOADS)
def test_execution_repeatability_reduction_and_cancellation(payload):
    request = create_request(payload)
    results = [execute(p) for _, p in partitions(request.payload, 3)]
    assert results == [execute(p) for _, p in partitions(request.payload, 3)]
    result = workload(payload["operation"]).reduce(results)
    if payload["operation"] == "PRIME_COUNT":
        assert result == {"totalPrimeCount": 25}
    elif payload["operation"] == "RANGE_SUM":
        assert result == {"totalSum": sum(range(-11, 100))}
    else:
        assert result["samples"] == 10001
        assert result["insideCircle"] == sum(r["insideCircle"] for r in results)
        assert abs(result["piEstimate"] - 3.14159) < 0.1
    with pytest.raises(Cancelled):
        execute(payload, lambda: True)


def test_cancellation_during_monte_carlo():
    checks = 0

    def cancelled():
        nonlocal checks
        checks += 1
        return checks == 3

    with pytest.raises(Cancelled):
        execute({"operation": "MONTE_CARLO_PI", "samples": 100000, "seed": 7}, cancelled)
    assert checks == 3


@pytest.mark.parametrize(
    "start,end", [(-100000000, 100000000), (0, 100000000), (-100000000, -99999999), (1, 1000001)]
)
def test_sum_bounds_and_exact_integers(start, end):
    result = execute({"operation": "RANGE_SUM", "fromInclusive": start, "toExclusive": end})
    assert result["rangeSum"] == (end * (end - 1) - start * (start - 1)) // 2
    assert abs(result["rangeSum"]) < 2**53


@pytest.mark.integration
@pytest.mark.parametrize("payload", PAYLOADS)
def test_idempotency_recovery_same_payload_canonical_result_and_ack(db, payload):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    request = create_request(payload, 1)
    job, _ = jobs.create(request, "same")
    original = jobs.list_tasks(job["id"], 200)["items"]
    assert jobs.create(request, "same")[0] == job
    assert jobs.list_tasks(job["id"], 200)["items"] == original
    with pytest.raises(Conflict, match="different content"):
        jobs.create(request.model_copy(update={"name": "changed"}), "same")
    worker = register(scheduler)
    old = scheduler.claim(Claim(workerId=worker, claimRequestId=uuid4()))
    scheduler.start(old["attemptId"], worker)
    age_worker(db, worker)
    Recovery(scheduler).sweep()
    eligible(db)
    replacement = register(scheduler)
    new = scheduler.claim(Claim(workerId=replacement, claimRequestId=uuid4()))
    assert new["payload"] == old["payload"] == original[0]["payload"]
    assert new["attemptNumber"] == 2
    result = execute(new["payload"])
    assert result == execute(old["payload"])
    scheduler.start(new["attemptId"], replacement)
    with pytest.raises(Conflict):
        scheduler.complete(old["attemptId"], Completion(workerId=worker, outcome="SUCCEEDED", result=result))
    completion = Completion(workerId=replacement, outcome="SUCCEEDED", result=result)
    ack = scheduler.complete(new["attemptId"], completion)
    age_worker(db, replacement)
    Recovery(scheduler).sweep()
    assert scheduler.complete(new["attemptId"], completion) == ack
    expected = workload(payload["operation"]).reduce([result])
    assert jobs.job(job["id"])["result"] == expected
    assert jobs.list_jobs(200)["items"][0]["result"] == expected
    assert [a["status"] for a in jobs.attempts(new["taskId"])["items"]] == ["EXPIRED", "SUCCEEDED"]


@pytest.mark.integration
@pytest.mark.parametrize("payload", PAYLOADS)
def test_wrong_result_schema_and_bounds_do_not_mutate_attempt(db, payload):
    jobs, scheduler = Jobs(db, Settings()), Scheduler(db, Settings())
    job, _ = jobs.create(create_request(payload, 1), "wrong-result")
    worker = register(scheduler)
    task = scheduler.claim(Claim(workerId=worker, claimRequestId=uuid4()))
    scheduler.start(task["attemptId"], worker)
    wrong = {"rangeSum": 0} if payload["operation"] == "PRIME_COUNT" else {"primeCount": 0}
    invalid_bounds = {
        "PRIME_COUNT": {"primeCount": 1000},
        "RANGE_SUM": {"rangeSum": 9999999},
        "MONTE_CARLO_PI": {"samples": 1, "insideCircle": 1},
    }[payload["operation"]]
    for result in (wrong, invalid_bounds):
        with pytest.raises(Conflict) as error:
            scheduler.complete(
                task["attemptId"], Completion(workerId=worker, outcome="SUCCEEDED", result=result)
            )
        assert error.value.code == "INVALID_RESULT"
        assert jobs.task(task["taskId"])["status"] == "RUNNING"
        assert jobs.job(job["id"])["result"] is None
    scheduler.complete(
        task["attemptId"],
        Completion(
            workerId=worker,
            outcome="FAILED",
            error={"code": "INVALID_PAYLOAD", "message": "controlled failure"},
        ),
    )
    assert jobs.job(job["id"])["status"] == "FAILED"
    assert jobs.job(job["id"])["result"] is None


@pytest.mark.parametrize(
    "result",
    [
        {"rangeSum": float("nan")},
        {"primeCount": 1, "rangeSum": 1},
        {"samples": 10, "insideCircle": 11},
        {"rangeSum": 2**53},
    ],
)
def test_strict_result_contracts(result):
    with pytest.raises(ValidationError):
        Completion(workerId=uuid4(), outcome="SUCCEEDED", result=result)
