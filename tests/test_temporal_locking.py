import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from test_completion import completion, setup_job

from scheduler.control_plane.domain import Conflict
from scheduler.control_plane.recovery import Recovery
from scheduler.protocol.models import Heartbeat

pytestmark = [
    pytest.mark.integration,
    pytest.mark.fault,
]


def test_completion_rechecks_time_after_waiting_for_job_lock(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)

    scheduler.start(
        assignment["attemptId"],
        worker,
    )

    db.run(
        lambda c: c.execute(
            """
            UPDATE task_attempts
            SET lease_expires_at =
                clock_timestamp() + interval '0.3 seconds'
            """
        )
    )

    waiting = Event()

    def hook(name, c):
        if name == "worker_locked":
            waiting.set()

    scheduler.hook = hook

    with ThreadPoolExecutor(max_workers=1) as pool:
        with db.transaction() as blocker:
            # Hold the job lock long enough for the attempt lease to expire.
            blocker.execute(
                """
                SELECT id
                FROM jobs
                WHERE id = %s
                FOR UPDATE
                """,
                (job["id"],),
            )

            future = pool.submit(
                scheduler.complete,
                assignment["attemptId"],
                completion(
                    worker,
                    assignment,
                ),
            )

            assert waiting.wait(timeout=5)

            deadline = time.monotonic() + 3

            while not db.run(
                lambda c: c.execute(
                    """
                    SELECT
                        clock_timestamp() >= lease_expires_at AS expired
                    FROM task_attempts
                    """
                ).fetchone()
            )["expired"]:
                assert time.monotonic() < deadline
                time.sleep(0.005)

        # Completion must re-check validity after waiting for the lock.
        with pytest.raises(Conflict):
            future.result(timeout=5)

    assert jobs.job(
        job["id"]
    )["completedTasks"] == 0


def test_absolute_deadline_expires_despite_recent_process_heartbeat(db):
    scheduler, jobs, job, [(worker, assignment)] = setup_job(db)

    scheduler.start(
        assignment["attemptId"],
        worker,
    )

    db.run(
        lambda c: c.execute(
            """
            UPDATE task_attempts
            SET
                assigned_at =
                    clock_timestamp() - interval '30 seconds',
                started_at =
                    clock_timestamp() - interval '20 seconds',
                execution_deadline_at =
                    clock_timestamp() - interval '1 second',
                lease_expires_at =
                    clock_timestamp() - interval '2 seconds'
            """
        )
    )

    # A healthy worker heartbeat cannot revive an attempt past its deadline.
    assert (
        scheduler.heartbeat(
            worker,
            Heartbeat(
                activeAttemptIds=[
                    assignment["attemptId"]
                ]
            ),
        )["rejected"][0]["reason"]
        == "EXPIRED"
    )

    Recovery(
        scheduler
    ).sweep()

    # The worker itself is still healthy; only the attempt timed out.
    assert jobs.worker(
        worker
    )["status"] == "ONLINE"

    assert jobs.attempts(
        assignment["taskId"]
    )["items"][0]["errorCode"] == "EXECUTION_TIMEOUT"