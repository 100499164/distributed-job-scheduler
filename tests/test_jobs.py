from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from scheduler.config import Settings
from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.jobs import Jobs, partitions
from scheduler.protocol.models import CreateJob, PrimePayload


def request(count=7, **changes):
    return CreateJob(
        **{
            "name": "prime",
            "taskCount": count,
            "payload": {"operation": "PRIME_COUNT", "fromInclusive": 2, "toExclusive": 100},
            **changes,
        }
    )


@pytest.mark.parametrize("length,count", [(1, 1), (98, 7), (100, 100), (9999, 1000)])
def test_partition_formula(length, count):
    parts = list(partitions(PrimePayload(fromInclusive=2, toExclusive=length + 2), count))
    assert parts[0][1]["fromInclusive"] == 2
    assert parts[-1][1]["toExclusive"] == length + 2
    assert [i for i, _ in parts] == list(range(count))
    q, r = divmod(length, count)
    for i, (_, p) in enumerate(parts):
        assert p["toExclusive"] - p["fromInclusive"] == q + (i < r)
        if i:
            assert parts[i - 1][1]["toExclusive"] == p["fromInclusive"]


@pytest.mark.integration
def test_create_query_defaults_conflict_and_pagination(db):
    jobs = Jobs(db, Settings())
    first, created = jobs.create(request(), "same")
    assert created
    repeated, created = jobs.create(request(maxRetries=3), "same")
    assert repeated == first and not created
    with pytest.raises(Conflict) as error:
        jobs.create(request(name="changed"), "same")
    assert error.value.code == "IDEMPOTENCY_CONFLICT"
    page = jobs.list_tasks(first["id"], 3)
    assert [t["partitionIndex"] for t in page["items"]] == [0, 1, 2]
    next_page = jobs.list_tasks(first["id"], 3, page["nextCursor"])
    assert [t["partitionIndex"] for t in next_page["items"]] == [3, 4, 5]
    assert jobs.job(first["id"])["result"] is None
    assert jobs.task(page["items"][0]["id"])["retryCount"] == 0
    assert jobs.list_jobs(1)["items"][0]["id"] == first["id"]
    with pytest.raises(Conflict):
        jobs.list_jobs(1, page["nextCursor"])


@pytest.mark.integration
def test_concurrent_creation_one_job_and_exact_tasks(db):
    jobs, barrier = Jobs(db, Settings()), Barrier(8)

    def create(_):
        barrier.wait(timeout=10)
        return jobs.create(request(), "concurrent")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(create, range(8)))
    assert len({r[0]["id"] for r in results}) == 1
    assert sum(r[1] for r in results) == 1
    assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM tasks").fetchone())["n"] == 7


@pytest.mark.integration
def test_creation_rollback_is_atomic(db):
    def interrupt(name, c):
        raise RuntimeError("controlled interruption before commit")

    with pytest.raises(RuntimeError):
        Jobs(db, Settings(), hook=interrupt).create(request(), str(uuid4()))
    assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM jobs").fetchone())["n"] == 0
    assert db.run(lambda c: c.execute("SELECT count(*) AS n FROM tasks").fetchone())["n"] == 0
