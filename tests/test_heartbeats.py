from uuid import uuid4

import pytest
from test_completion import completion, setup_job

from scheduler.control_plane.domain import Conflict
from scheduler.protocol.models import Heartbeat

pytestmark = pytest.mark.integration


def expire_lease(db, aid):
    db.run(
        lambda c: c.execute(
            """
            UPDATE task_attempts
            SET
                assigned_at = clock_timestamp() - interval '60 seconds',
                started_at = clock_timestamp() - interval '50 seconds',
                lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE id = %s
            """,
            (aid,),
        )
    )


def test_only_listed_running_attempts_renew_and_deadline_caps(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)
    attempt_id = assignment["attemptId"]

    heartbeat = Heartbeat(
        activeAttemptIds=[attempt_id]
    )

    # Assigned attempts cannot renew their lease until they are running.
    assert (
        scheduler.heartbeat(
            worker,
            heartbeat,
        )["rejected"][0]["reason"]
        == "NOT_RUNNING"
    )

    assert (
        jobs.attempts(
            assignment["taskId"]
        )["items"][0]["leaseExpiresAt"]
        == assignment["leaseExpiresAt"]
    )

    initial = scheduler.start(
        attempt_id,
        worker,
    )

    # Running attempts omitted from the heartbeat must not be renewed.
    scheduler.heartbeat(
        worker,
        Heartbeat(activeAttemptIds=[]),
    )

    assert (
        jobs.attempts(
            assignment["taskId"]
        )["items"][0]["leaseExpiresAt"]
        == initial["leaseExpiresAt"]
    )

    response = scheduler.heartbeat(
        worker,
        heartbeat,
    )

    assert (
        response["renewed"][0]["leaseExpiresAt"]
        > initial["leaseExpiresAt"]
    )

    deadline = db.run(
        lambda c: c.execute(
            """
            UPDATE task_attempts
            SET
                execution_deadline_at = clock_timestamp() + interval '5 seconds',
                lease_expires_at = clock_timestamp() + interval '3 seconds'
            WHERE id = %s
            RETURNING execution_deadline_at
            """,
            (attempt_id,),
        ).fetchone()
    )["execution_deadline_at"]

    # Lease renewal can never extend beyond the absolute execution deadline.
    assert (
        scheduler.heartbeat(
            worker,
            heartbeat,
        )["renewed"][0]["leaseExpiresAt"]
        == deadline
    )


def test_expired_unrecovered_lease_rejects_start_completion_and_renewal(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)
    attempt_id = assignment["attemptId"]

    scheduler.start(
        attempt_id,
        worker,
    )

    expire_lease(
        db,
        attempt_id,
    )

    # Expiration is enforced immediately, even before recovery has processed it.
    assert (
        scheduler.heartbeat(
            worker,
            Heartbeat(
                activeAttemptIds=[attempt_id]
            ),
        )["rejected"][0]["reason"]
        == "EXPIRED"
    )

    with pytest.raises(Conflict):
        scheduler.start(
            attempt_id,
            worker,
        )

    with pytest.raises(Conflict):
        scheduler.complete(
            attempt_id,
            completion(
                worker,
                assignment,
            ),
        )

    assert jobs.job(
        job["id"]
    )["completedTasks"] == 0


def test_foreign_and_unknown_attempts_are_not_renewed(db):
    scheduler, jobs, job, pairs = setup_job(
        db,
        2,
    )

    worker, assignment = pairs[0]
    other_assignment = pairs[1][1]

    # A worker cannot renew an attempt owned by another worker.
    assert (
        scheduler.heartbeat(
            worker,
            Heartbeat(
                activeAttemptIds=[
                    other_assignment["attemptId"]
                ]
            ),
        )["rejected"][0]["reason"]
        == "WRONG_OWNER"
    )

    # Unknown attempt IDs are rejected instead of silently accepted.
    assert (
        scheduler.heartbeat(
            worker,
            Heartbeat(
                activeAttemptIds=[uuid4()]
            ),
        )["rejected"][0]["reason"]
        == "UNKNOWN_ATTEMPT"
    )

    db.run(
        lambda c: c.execute(
            """
            UPDATE workers
            SET
                status = 'OFFLINE',
                offline_at = clock_timestamp()
            WHERE id = %s
            """,
            (worker,),
        )
    )

    # Heartbeats from an expired/offline worker session are rejected.
    with pytest.raises(Conflict) as error:
        scheduler.heartbeat(
            worker,
            Heartbeat(activeAttemptIds=[]),
        )

    assert error.value.code == "SESSION_EXPIRED"