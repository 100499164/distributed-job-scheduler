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
            "payload": {
                "operation": "PRIME_COUNT",
                "fromInclusive": 2,
                "toExclusive": 100,
            },
            **changes,
        }
    )


@pytest.mark.parametrize(
    "length,count",
    [
        (1, 1),
        (98, 7),
        (100, 100),
        (9999, 1000),
    ],
)
def test_partition_formula(length, count):
    parts = list(
        partitions(
            PrimePayload(
                fromInclusive=2,
                toExclusive=length + 2,
            ),
            count,
        )
    )

    assert parts[0][1]["fromInclusive"] == 2
    assert parts[-1][1]["toExclusive"] == length + 2
    assert [index for index, _ in parts] == list(range(count))

    q, r = divmod(length, count)

    for index, (_, part) in enumerate(parts):
        # Partition sizes differ by at most one element.
        assert part["toExclusive"] - part["fromInclusive"] == q + (index < r)

        # Partitions must be contiguous with no gaps or overlaps.
        if index:
            assert parts[index - 1][1]["toExclusive"] == part["fromInclusive"]


@pytest.mark.integration
def test_create_query_defaults_conflict_and_pagination(db):
    jobs = Jobs(
        db,
        Settings(),
    )

    first, created = jobs.create(
        request(),
        "same",
    )

    assert created

    # Explicit defaults must be equivalent to omitted defaults.
    repeated, created = jobs.create(
        request(maxRetries=3),
        "same",
    )

    assert repeated == first
    assert not created

    # Reusing the same idempotency key with different content is invalid.
    with pytest.raises(Conflict) as error:
        jobs.create(
            request(name="changed"),
            "same",
        )

    assert error.value.code == "IDEMPOTENCY_CONFLICT"

    page = jobs.list_tasks(
        first["id"],
        3,
    )

    assert [task["partitionIndex"] for task in page["items"]] == [
        0,
        1,
        2,
    ]

    next_page = jobs.list_tasks(
        first["id"],
        3,
        page["nextCursor"],
    )

    assert [task["partitionIndex"] for task in next_page["items"]] == [
        3,
        4,
        5,
    ]

    assert jobs.job(first["id"])["result"] is None

    assert jobs.task(page["items"][0]["id"])["retryCount"] == 0

    assert jobs.list_jobs(1)["items"][0]["id"] == first["id"]

    # A task cursor cannot be reused for the jobs endpoint.
    with pytest.raises(Conflict):
        jobs.list_jobs(
            1,
            page["nextCursor"],
        )


@pytest.mark.integration
def test_concurrent_creation_one_job_and_exact_tasks(db):
    jobs = Jobs(
        db,
        Settings(),
    )

    barrier = Barrier(8)

    def create(_):
        # Make all requests race using the same idempotency key.
        barrier.wait(timeout=10)

        return jobs.create(
            request(),
            "concurrent",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                create,
                range(8),
            )
        )

    # Concurrent idempotent requests must converge on one job.
    assert len({result[0]["id"] for result in results}) == 1

    # Exactly one caller creates the job.
    assert sum(result[1] for result in results) == 1

    # Tasks are inserted exactly once with the winning job creation.
    assert (
        db.run(
            lambda c: c.execute(
                """
            SELECT count(*) AS n
            FROM tasks
            """
            ).fetchone()
        )["n"]
        == 7
    )


@pytest.mark.integration
def test_creation_rollback_is_atomic(db):
    def interrupt(name, c):
        raise RuntimeError("controlled interruption before commit")

    # A failure before commit must roll back both the job and its tasks.
    with pytest.raises(RuntimeError):
        Jobs(
            db,
            Settings(),
            hook=interrupt,
        ).create(
            request(),
            str(uuid4()),
        )

    assert (
        db.run(
            lambda c: c.execute(
                """
            SELECT count(*) AS n
            FROM jobs
            """
            ).fetchone()
        )["n"]
        == 0
    )

    assert (
        db.run(
            lambda c: c.execute(
                """
            SELECT count(*) AS n
            FROM tasks
            """
            ).fetchone()
        )["n"]
        == 0
    )
