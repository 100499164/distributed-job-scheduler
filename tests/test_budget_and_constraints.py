from uuid import uuid4

import psycopg
import pytest
from test_claims import register
from test_jobs import request
from test_recovery import eligible

from scheduler.config import Settings
from scheduler.control_plane.jobs import Jobs
from scheduler.control_plane.scheduling import Scheduler
from scheduler.protocol.models import Claim, Completion

pytestmark = pytest.mark.integration


def test_retry_budget_exhausted_after_four_reservations(db):
    jobs = Jobs(db, Settings())
    scheduler = Scheduler(db, Settings())

    job, _ = jobs.create(
        request(count=1),
        "budget",
    )
    worker = register(scheduler)

    # The default retry budget allows four total attempts:
    # one initial attempt plus three retries.
    for attempt_number in range(1, 5):
        assigned = scheduler.claim(
            Claim(
                workerId=worker,
                claimRequestId=uuid4(),
            )
        )

        assert assigned["attemptNumber"] == attempt_number

        scheduler.start(
            assigned["attemptId"],
            worker,
        )

        scheduler.complete(
            assigned["attemptId"],
            Completion(
                workerId=worker,
                outcome="FAILED",
                error={
                    "code": "TRANSIENT_ERROR",
                    "message": "test",
                },
            ),
        )

        # Advance retry timing so the next attempt becomes eligible.
        eligible(db)

    assert jobs.job(job["id"])["status"] == "FAILED"
    assert jobs.job(job["id"])["failedTasks"] == 1

    # No further assignment is possible once the retry budget is exhausted.
    assert (
        scheduler.claim(
            Claim(
                workerId=worker,
                claimRequestId=uuid4(),
            )
        )
        is None
    )


def test_unique_active_attempt_and_foreign_keys_are_enforced(db):
    jobs = Jobs(db, Settings())
    scheduler = Scheduler(db, Settings())

    job, _ = jobs.create(
        request(count=1),
        "constraints",
    )
    worker = register(scheduler)

    assignment = scheduler.claim(
        Claim(
            workerId=worker,
            claimRequestId=uuid4(),
        )
    )

    # A task may have only one active attempt at a time.
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.run(
            lambda c: c.execute(
                """
                INSERT INTO task_attempts(
                    id,
                    task_id,
                    worker_id,
                    attempt_number,
                    claim_request_id,
                    status,
                    lease_expires_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    2,
                    %s,
                    'ASSIGNED',
                    clock_timestamp() + interval '15 seconds'
                )
                """,
                (
                    uuid4(),
                    assignment["taskId"],
                    worker,
                    uuid4(),
                ),
            )
        )

    # Jobs cannot be deleted while their tasks still reference them.
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db.run(
            lambda c: c.execute(
                "DELETE FROM jobs WHERE id = %s",
                (job["id"],),
            )
        )

    # Job counters cannot exceed the configured number of tasks.
    with pytest.raises(psycopg.errors.CheckViolation):
        db.run(
            lambda c: c.execute(
                "UPDATE jobs SET completed_tasks = 2 WHERE id = %s",
                (job["id"],),
            )
        )
